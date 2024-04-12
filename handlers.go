package main

import (
    "log"
    "fmt"

    "os"
    "path/filepath"
    "io/fs"

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

func validatePath(path string) error { 
    if path == "" {
        return errors.New("'path' should be present as a non-empty string")
    }
    if !filepath.IsAbs(path) {
        return errors.New("'path' should be an absolute path")
    }
    return nil
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

        regpath := output.Path
        err = validatePath(regpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        info, err := os.Lstat(regpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to stat; %v", err) })
            return
        } else if info.Mode() & fs.ModeSymlink != 0 {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "cannot register a symbolic link" })
            return
        }

        candidate, err := verifier.Provision(regpath)
        if err != nil {
            status := http.StatusInternalServerError
            if errors.Is(err, os.ErrPermission) {
                status = http.StatusBadRequest
            }
            dumpJsonResponse(w, status, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to generate a suitable verification code for %q; %v", regpath, err) })
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

        regpath := output.Path
        err = validatePath(regpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
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

        expected_code, ok := verifier.Pop(regpath)
        if !ok {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("no verification code available for %q", regpath) })
            return
        }

        // Make sure to use Lstat here, not Stat; otherwise, someone could
        // create a symlink to a file owned by someone else and pretend to be
        // that person when registering a directory.
        expected_path := filepath.Join(regpath, expected_code)
        code_info, err := os.Lstat(expected_path)
        if errors.Is(err, os.ErrNotExist) {
            dumpJsonResponse(w, http.StatusUnauthorized, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("verification failed for %q; %v", regpath, err) })
            return
        } else if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to inspect verification code for %q; %v", regpath, err) })
            return
        }

        username, err := identifyUser(code_info)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("cannot identify the registering user from %q; %v", regpath, err) })
            return
        }

        failures, err := addDirectory(db, regpath, allowed, username, tokenizer)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to index directory; %v", err) })
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

        regpath := output.Path
        err = validatePath(regpath)
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
            status := http.StatusInternalServerError
            if errors.Is(err, os.ErrPermission) {
                status = http.StatusBadRequest
            }
            dumpJsonResponse(w, status, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to generate a suitable verification code for %q; %v", regpath, err) })
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

        regpath := output.Path
        err = validatePath(regpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        expected_code, ok := verifier.Pop(regpath)
        if !ok {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("no verification code available for %q", regpath) })
            return
        }

        expected_path := filepath.Join(regpath, expected_code)
        _, err = os.Lstat(expected_path)
        if errors.Is(err, os.ErrNotExist) {
            dumpJsonResponse(w, http.StatusUnauthorized, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("verification failed for %q; %v", regpath, err) })
            return
        } else if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to inspect verification code for %q; %v", regpath, err) })
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

        okay, err := isSubpathRegistered(db, filepath.Dir(path))
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to check path registration; %v", err) })
            return
        }
        if !okay {
            dumpJsonResponse(w, http.StatusForbidden, map[string]string{ "status": "ERROR", "reason": "cannot retrieve file from an unregistered path" })
            return
        }

        // Note that isSubpathRegistered already checks for links at every step of the path.
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
        if !params.Has("path") {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("expected a 'path' query parameter") })
        }
        path, err := url.QueryUnescape(params.Get("path"))
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("path is not properly URL-encoded; %v", err) })
            return
        }
        recursive := params.Get("recursive") == "true"

        okay, err := isSubpathRegistered(db, path)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to check path registration; %v", err) })
            return
        }
        if !okay {
            dumpJsonResponse(w, http.StatusForbidden, map[string]string{ "status": "ERROR", "reason": "cannot retrieve file from an unregistered path" })
            return
        }

        // isSubpathRegistered already checks for symlinks in the path.
        info, err := os.Lstat(path)
        if errors.Is(err, os.ErrNotExist) {
            dumpJsonResponse(w, http.StatusNotFound, map[string]string{ "status": "ERROR", "reason": "path does not exist" })
            return
        } else if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("inaccessible path; %v", err) })
            return
        } else if !info.IsDir() {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "path should refer to a directory" })
            return
        }

        listing, err := listFiles(path, recursive)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to obtain listing; %v", err) })
            return
        }
        dumpJsonResponse(w, http.StatusOK, listing)
    }
}
