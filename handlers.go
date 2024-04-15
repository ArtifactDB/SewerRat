package main

import (
    "log"
    "fmt"

    "os"
    "path/filepath"
    "io/fs"
    "syscall"

    "net/http"
    "net/url"
    "encoding/json"
    "errors"
    "strings"
    "strconv"
    "database/sql"
)

func dumpJsonResponse(w http.ResponseWriter, status int, v interface{}) {
    contents, err := json.Marshal(v)
    if err != nil {
        log.Printf("failed to convert response to JSON; %v", err)
        contents = []byte("unknown")
    }

    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "*")
    w.WriteHeader(status)
    _, err = w.Write(contents)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        log.Printf("failed to write JSON response; %v", err)
        return
    }
}

func validatePath(path string) (string, error) { 
    if path == "" {
        return "", errors.New("'path' should be present as a non-empty string")
    }
    if !filepath.IsAbs(path) {
        return "", errors.New("'path' should be an absolute path")
    }

    // We limit ourselves to cleaning the path and no further normalization.
    // In particular, we don't try to evaluate the symbolic links as the "real"
    // path may not be consistent across the shared filesystem (e.g., due to
    // mounts onto different compute centers). So the symbolic links in the path
    // to the registered directory may be important (though once we're inside
    // a registered directory, any symbolic links are forbidden).
    return filepath.Clean(path), nil
}

func dumpHttpErrorResponse(w http.ResponseWriter, err error) {
    status_code := http.StatusInternalServerError
    var http_err *httpError
    if errors.As(err, &http_err) {
        status_code = http_err.Status
    }
    dumpJsonResponse(w, status_code, map[string]interface{}{ "status": "ERROR", "reason": err.Error() })
}

func dumpVerifierProvisionError(err error, w http.ResponseWriter) {
    if errors.Is(err, os.ErrPermission) {
        err = newHttpError(http.StatusBadRequest, err)
    }
    dumpHttpErrorResponse(w, err)
}

func checkVerificationCode(path string, verifier *verificationRegistry) (fs.FileInfo, error) {
    expected_code, ok := verifier.Pop(path)
    if !ok {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("no verification code available for %q", path))
    }

    // Make sure to use Lstat here, not Stat; otherwise, someone could
    // create a symlink to a file owned by someone else and pretend to be
    // that person when registering a directory.
    expected_path := filepath.Join(path, expected_code)
    code_info, err := os.Lstat(expected_path)
    if errors.Is(err, os.ErrNotExist) {
        return nil, newHttpError(http.StatusUnauthorized, fmt.Errorf("verification failed for %q; %v", path, err))
    } else if err != nil {
        return nil, fmt.Errorf("failed to inspect verification code for %q; %v", path, err)
    }

    // Similarly, prohibit hard links to avoid spoofing identities. Admittedly,
    // if a user has write access to create a hard link in the directory, then
    // they would be able to (de)register the directory themselves anyway.
    s, ok := code_info.Sys().(*syscall.Stat_t)
    if !ok {
        return nil, fmt.Errorf("failed to convert into a syscall.Stat_t; %v", err)
    } else if int(s.Nlink) > 1 {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("verification path has multiple hard links; %v", err))
    }

    return code_info, nil
}

/**********************************************************************/

func newRegisterStartHandler(verifier *verificationRegistry) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a POST request" })
            return
        }

        if r.Body == nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "expected a non-empty request body" })
            return
        }
        dec := json.NewDecoder(r.Body)
        output := struct { Path string `json:"path"` }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to decode body; %v", err) })
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        err = checkValidDirectory(regpath)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        candidate, err := verifier.Provision(regpath)
        if err != nil {
            dumpVerifierProvisionError(err, w)
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "code": candidate })
        return
    }
}

func newRegisterFinishHandler(db *sql.DB, verifier *verificationRegistry, tokenizer *unicodeTokenizer) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a POST request" })
            return
        }

        if r.Body == nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "expected a non-empty request body" })
            return
        }
        dec := json.NewDecoder(r.Body)
        output := struct { 
            Path string `json:"path"`
            Base []string `json:"base"`
        }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to decode body; %v", err) })
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }
        err = checkValidDirectory(regpath)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        allowed := map[string]bool{}
        if output.Base != nil {
            for _, a := range output.Base {
                if len(a) == 0 {
                    dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "empty name in 'base'" })
                    return
                }
                if _, ok := allowed[a]; ok {
                    dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "duplicate names in 'base'" })
                    return
                }
                allowed[a] = true
            }
            if len(allowed) == 0 {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "'base' should have at least one name" })
                return
            }
        } else {
            allowed["metadata.json"] = true
        }

        code_info, err := checkVerificationCode(regpath, verifier)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }
        username, err := identifyUser(code_info)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("cannot identify the registering user from %q; %v", regpath, err))
            return
        }

        failures, err := addDirectory(db, regpath, allowed, username, tokenizer)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to index directory; %v", err))
            return
        }

        dumpJsonResponse(w, http.StatusOK, map[string]interface{}{ "status": "SUCCESS", "comments": failures })
        return
    }
}

/**********************************************************************/

func newDeregisterStartHandler(db *sql.DB, verifier *verificationRegistry) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a POST request" })
            return
        }

        if r.Body == nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "expected a non-empty request body" })
            return
        }
        dec := json.NewDecoder(r.Body)
        output := struct { Path string `json:"path"` }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to decode body; %v", err) })
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        // If the directory doesn't exist, then we don't need to attempt to create a verification code.
        if _, err := os.Lstat(regpath); errors.Is(err, os.ErrNotExist) {
            err := deleteDirectory(db, regpath)
            if err != nil {
                dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to deregister %q; %v", regpath, err) })
                return
            } else {
                dumpJsonResponse(w, http.StatusOK, map[string]string{ "status": "SUCCESS" })
                return
            }
        }

        candidate, err := verifier.Provision(regpath)
        if err != nil {
            dumpVerifierProvisionError(err, w)
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "code": candidate })
        return
    }
}

func newDeregisterFinishHandler(db *sql.DB, verifier *verificationRegistry) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a POST request" })
            return
        }

        if r.Body == nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "expected a non-empty request body" })
            return
        }
        dec := json.NewDecoder(r.Body)
        output := struct { Path string `json:"path"` }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to decode body; %v", err) })
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        _, err = checkVerificationCode(regpath, verifier)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        err = deleteDirectory(db, regpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to deregister %q; %v", regpath, err) })
            return
        }

        dumpJsonResponse(w, http.StatusOK, map[string]string{ "status": "SUCCESS" })
        return
    }
}

/**********************************************************************/

func configureCors(w http.ResponseWriter, r *http.Request) bool {
    if r.Method == "OPTIONS" {
        w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Headers", "*")
        w.WriteHeader(http.StatusNoContent)
        return true
    } else {
        return false
    }
}

func newQueryHandler(db *sql.DB, tokenizer *unicodeTokenizer, wild_tokenizer *unicodeTokenizer, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "POST" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a POST request" })
            return
        }

        params := r.URL.Query()
        var scroll *scrollPosition
        limit := 100

        if params.Has("scroll") {
            val := params.Get("scroll")
            i := strings.Index(val, ",")
            if i < 0 {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "'scroll' should be a comma-separated string" })
                return
            }
            time, err := strconv.ParseInt(val[:i], 10, 64)
            if err != nil {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "failed to parse the time from 'scroll'" })
                return
            }
            pid, err := strconv.ParseInt(val[(i+1):], 10, 64)
            if err != nil {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "failed to parse the path ID from 'scroll'" })
                return
            }
            scroll = &scrollPosition{ Time: time, Pid: pid }
        }

        if params.Has("limit") {
            limit0, err := strconv.Atoi(params.Get("limit"))
            if err != nil || limit0 <= 0 {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "invalid 'limit'" })
                return
            }
            if (limit0 < limit) {
                limit = limit0
            }
        }

        translate := false
        if params.Has("translate") {
            translate = strings.ToLower(params.Get("translate")) == "true"
        }

        if r.Body == nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "expected a non-empty request body" })
            return
        }
        query := &searchClause{}
        restricted := http.MaxBytesReader(w, r.Body, 1048576)
        dec := json.NewDecoder(restricted)
        err := dec.Decode(query)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to parse response body; %v", err) })
            return
        }

        if translate {
            query, err = translateQuery(query)
            if err != nil {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to translate text query; %v", err) })
            }
        }

        san, err := sanitizeQuery(query, tokenizer, wild_tokenizer)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to sanitize an invalid query; %v", err) })
            return
        }

        res, err := queryTokens(db, san, scroll, limit)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to query tokens; %v", err) })
            return
        }

        respbody := map[string]interface{} { "results": res }
        if len(res) == limit {
            last := &(res[limit-1])
            next := endpoint + "?scroll=" + strconv.FormatInt(last.Time, 10) + "," + strconv.FormatInt(last.Pid, 10)
            if translate {
                next += "&translate=true"
            }
            respbody["next"] = next
        }

        dumpJsonResponse(w, http.StatusOK, respbody)
        return
    }
}

/**********************************************************************/

func newRetrieveMetadataHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "GET" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a GET request" })
            return
        }

        params := r.URL.Query()
        if !params.Has("path") {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("expected a 'path' query parameter") })
        }
        path, err := url.QueryUnescape(params.Get("path"))
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("path is not properly URL-encoded; %v", err) })
            return
        }

        use_metadata := true
        if params.Has("metadata") {
            use_metadata = (strings.ToLower(params.Get("metadata")) != "false")
        }

        res, err := retrievePath(db, path, use_metadata)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to retrieve path; %v", err) })
            return
        }

        if res == nil {
            dumpJsonResponse(w, http.StatusNotFound, map[string]string{ "status": "ERROR", "reason": "path is not registered" })
            return
        }

        dumpJsonResponse(w, http.StatusOK, res)
        return
    }
}

/**********************************************************************/

func newRetrieveFileHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "GET" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a GET request" })
            return
        }

        params := r.URL.Query()
        if !params.Has("path") {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("expected a 'path' query parameter") })
        }
        path, err := url.QueryUnescape(params.Get("path"))
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("path is not properly URL-encoded; %v", err) })
            return
        }
        path, err = validatePath(path)
        if err != nil {
            dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid path; %w", err)))
            return
        }

        okay, err := isDirectoryRegistered(db, filepath.Dir(path))
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to check path registration; %v", err) })
            return
        }
        if !okay {
            dumpJsonResponse(w, http.StatusForbidden, map[string]string{ "status": "ERROR", "reason": "cannot retrieve file from an unregistered path" })
            return
        }

        info, err := os.Lstat(path)
        if errors.Is(err, os.ErrNotExist) {
            dumpJsonResponse(w, http.StatusNotFound, map[string]string{ "status": "ERROR", "reason": "path does not exist" })
            return
        } else if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("inaccessible path; %v", err) })
            return
        } else if info.IsDir() {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "path should refer to a file, not a directory" })
            return
        } else if info.Mode() & os.ModeSymlink != 0 {
            _, err := checkSymlinkTarget(path, nil)
            if err != nil {
                dumpHttpErrorResponse(w, fmt.Errorf("failed to check symlink target; %w", err))
                return
            }
        }

        http.ServeFile(w, r, path)
    }
}

/**********************************************************************/

func newListFilesHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "GET" {
            dumpJsonResponse(w, http.StatusMethodNotAllowed, map[string]string{ "status": "ERROR", "reason": "expected a GET request" })
            return
        }

        params := r.URL.Query()
        recursive := params.Get("recursive") == "true"

        if !params.Has("path") {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("expected a 'path' query parameter") })
        }
        path, err := url.QueryUnescape(params.Get("path"))
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("path is not properly URL-encoded; %v", err) })
            return
        }

        path, err = validatePath(path)
        if err != nil {
            dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid path; %w", err)))
            return
        }

        err = checkValidDirectory(path)
        if err != nil {
            dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid directory path; %w", err)))
            return
        }

        okay, err := isDirectoryRegistered(db, path)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to check path registration; %v", err) })
            return
        }
        if !okay {
            dumpJsonResponse(w, http.StatusForbidden, map[string]string{ "status": "ERROR", "reason": "cannot retrieve file from an unregistered path" })
            return
        }

        listing, err := listFiles(path, recursive)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to obtain directory listing; %w", err))
            return
        }
        dumpJsonResponse(w, http.StatusOK, listing)
    }
}
