package main

import (
    "log"
    "fmt"

    "os"
    "path/filepath"
    "io"
    "io/fs"
    "syscall"

    "net/http"
    "net/url"
    "mime"

    "time"

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
    w.Header().Set("Access-Control-Allow-Origin", "*") // setting this for CORS.
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
    // mounts onto different compute centers). 
    return filepath.Clean(path), nil
}

func dumpErrorResponse(w http.ResponseWriter, status_code int, reason string) {
    dumpJsonResponse(w, status_code, map[string]interface{}{ "status": "ERROR", "reason": reason })
}

func dumpHttpErrorResponse(w http.ResponseWriter, err error) {
    status_code := http.StatusInternalServerError
    var http_err *httpError
    if errors.As(err, &http_err) {
        status_code = http_err.Status
    }
    dumpErrorResponse(w, status_code, err.Error())
}

func getPageLimit(params url.Values, limit_name string, upper_limit int) (int, error) {
    if !params.Has(limit_name) {
        return upper_limit, nil
    }

    limit, err := strconv.Atoi(params.Get(limit_name))
    if err != nil || limit <= 0 {
        return 0, newHttpError(http.StatusBadRequest, errors.New("invalid '" + limit_name + "'"))
    }

    if limit > upper_limit {
        return upper_limit, nil
    }

    return limit, nil
}

func encodeNextParameters(scroll_name string, scroll_value string, params url.Values, other_names []string) string {
    gathered := url.Values{ scroll_name: []string{ scroll_value } }
    for _, p := range other_names {
        if params.Has(p) {
            gathered[p] = params[p]
        }
    }
    return gathered.Encode()
}

func checkVerificationCode(path string, verifier *verificationRegistry, timeout time.Duration) (fs.FileInfo, error) {
    expected_code, ok := verifier.Pop(path)
    if !ok {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("no verification code available for %q", path))
    }

    // Make sure to use Lstat here, not Stat; otherwise, someone could
    // create a symlink to a file owned by someone else and pretend to be
    // that person when registering a directory.
    expected_path := filepath.Join(path, expected_code)
    code_info, err := os.Lstat(expected_path)

    // Exponential back-off up to the time limit.
    until := time.Now().Add(timeout)
    sleep := time.Duration(1 * time.Second) 
    max_sleep := time.Duration(32 * time.Second) 
    for err != nil {
        if !errors.Is(err, os.ErrNotExist) {
            return nil, fmt.Errorf("failed to inspect verification code for %q; %w", path, err)
        }

        remaining := until.Sub(time.Now())
        if remaining <= 0 {
            return nil, newHttpError(http.StatusUnauthorized, fmt.Errorf("verification failed for %q; %w", path, err))
        }

        if sleep > remaining {
            sleep = remaining
        }
        time.Sleep(sleep)
        if sleep < max_sleep {
            sleep *= 2
        }

        code_info, err = os.Lstat(expected_path)
    }

    // Similarly, prohibit hard links to avoid spoofing identities. Admittedly,
    // if a user has write access to create a hard link in the directory, then
    // they would be able to (de)register the directory themselves anyway.
    s, ok := code_info.Sys().(*syscall.Stat_t)
    if !ok {
        return nil, fmt.Errorf("failed to convert into a syscall.Stat_t; %w", err)
    } else if int(s.Nlink) > 1 {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("verification path has multiple hard links; %w", err))
    }

    return code_info, nil
}

func verifySymlinks(path string, whitelist linkWhitelist) error {
    for {
        info, err := os.Lstat(path)
        if errors.Is(err, os.ErrNotExist) {
            return newHttpError(http.StatusNotFound, fmt.Errorf("path %q does not exist", path))
        }
        if err != nil {
            return fmt.Errorf("failed to check %q; %w", path, err)
        }

        if info.Mode() & os.ModeSymlink != 0 {
            if !isLinkWhitelisted(info, whitelist) {
                return newHttpError(http.StatusForbidden, fmt.Errorf("%q is not a whitelisted symlink", path))
            }
        }

        parent := filepath.Dir(path)
        if parent == path {
            break
        }
        path = parent
    }

    return nil
}

func verifyDirectory(dir string, whitelist linkWhitelist) error {
    err := verifySymlinks(dir, whitelist)
    if err != nil {
        return err
    }

    // Now that we're sure that all links are safe, we can use Stat() directly.
    info, err := os.Stat(dir)
    if errors.Is(err, os.ErrNotExist) {
        return newHttpError(http.StatusNotFound, fmt.Errorf("directory %q does not exist", dir))
    }
    if err != nil {
        return fmt.Errorf("failed to check %q; %w", dir, err)
    }

    if !info.IsDir() {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("%q is not a directory", dir))
    }
    return nil
}

/**********************************************************************/

func newRegisterStartHandler(verifier *verificationRegistry, whitelist linkWhitelist) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Body == nil {
            dumpErrorResponse(w, http.StatusBadRequest, "expected a non-empty request body")
            return
        }
        dec := json.NewDecoder(r.Body)
        output := struct { Path string `json:"path"` }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("failed to decode body; %v", err))
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, err.Error())
            return
        }

        err = verifyDirectory(regpath, whitelist)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, err.Error())
            return
        }

        candidate, err := verifier.Provision(regpath)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to provision a verification code; %w", err))
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "code": candidate })
        return
    }
}

func newRegisterFinishHandler(db *sql.DB, verifier *verificationRegistry, tokenizer *unicodeTokenizer, add_options *addDirectoryContentsOptions, timeout time.Duration) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Body == nil {
            dumpErrorResponse(w, http.StatusBadRequest, "expected a non-empty request body")
            return
        }

        dec := json.NewDecoder(r.Body)
        output := struct { 
            Path string `json:"path"`
            Base []string `json:"base"`
            Block *bool `json:"block"`
        }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("failed to decode body; %v", err))
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, err.Error())
            return
        }

        err = verifyDirectory(regpath, add_options.LinkWhitelist)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        var allowed []string
        if output.Base != nil {
            allowed = output.Base
            for _, a := range allowed {
                if len(a) == 0 {
                    dumpErrorResponse(w, http.StatusBadRequest, "empty name in 'base'")
                    return
                }
            }
            if len(allowed) == 0 {
                dumpErrorResponse(w, http.StatusBadRequest, "'base' should have at least one name")
                return
            }
        } else {
            existing, err := fetchRegisteredDirectoryNames(output.Path, db, r.Context())
            if err != nil {
                dumpHttpErrorResponse(w, err)
                return
            }
            if existing == nil {
                allowed = []string{ "metadata.json" }
            } else {
                allowed = existing
            }
        }

        code_info, err := checkVerificationCode(regpath, verifier, timeout)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }
        username, err := identifyUser(code_info)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("cannot identify the registering user from %q; %w", regpath, err))
            return
        }

        if output.Block == nil || *(output.Block) {
            failures, err := addNewDirectory(regpath, allowed, username, tokenizer, db, r.Context(), add_options)
            if err != nil {
                dumpHttpErrorResponse(w, fmt.Errorf("failed to index directory; %w", err))
                return
            } else {
                dumpJsonResponse(w, http.StatusOK, map[string]interface{}{ "status": "SUCCESS", "comments": failures })
                return
            }
        } else {
            go func() {
                _, err := addNewDirectory(regpath, allowed, username, tokenizer, db, r.Context(), add_options)
                if err != nil {
                    log.Printf("failed to add directory %q; %v", regpath, err)
                }
            }()
            dumpJsonResponse(w, http.StatusAccepted, map[string]interface{}{ "status": "PENDING" })
            return
        }
    }
}

/**********************************************************************/

func newDeregisterStartHandler(db *sql.DB, verifier *verificationRegistry) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Body == nil {
            dumpErrorResponse(w, http.StatusBadRequest, "expected a non-empty request body")
            return
        }

        dec := json.NewDecoder(r.Body)
        output := struct { 
            Path string `json:"path"` 
            Block *bool `json:"block"`
        }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, fmt.Errorf("failed to decode body; %w", err)))
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, err.Error())
            return
        }

        // No need to check for a valid directory, as we want to be able to deregister missing/symlinked directories.
        // If the directory doesn't exist, then we don't need to attempt to create a verification code.
        if _, err := os.Lstat(regpath); errors.Is(err, os.ErrNotExist) {
            if output.Block == nil || *(output.Block) {
                err := deleteDirectory(regpath, db, r.Context())
                if err != nil {
                    dumpHttpErrorResponse(w, fmt.Errorf("failed to deregister %q; %w", regpath, err))
                    return
                } else {
                    dumpJsonResponse(w, http.StatusOK, map[string]string{ "status": "SUCCESS" })
                    return
                }
            } else {
                go func() {
                    err := deleteDirectory(regpath, db, r.Context())
                    if err != nil {
                        log.Printf("failed to delete directory %q; %v", regpath, err)
                    }
                }()
                dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING" })
                return
            }
        }

        candidate, err := verifier.Provision(regpath)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to provision a verification code; %w", err))
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "code": candidate })
        return
    }
}

func newDeregisterFinishHandler(db *sql.DB, verifier *verificationRegistry, timeout time.Duration) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Body == nil {
            dumpErrorResponse(w, http.StatusBadRequest, "expected a non-empty request body")
            return
        }

        dec := json.NewDecoder(r.Body)
        output := struct {
            Path string `json:"path"`
            Block *bool `json:"block"`
        }{}
        err := dec.Decode(&output)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("failed to decode body; %v", err))
            return
        }

        regpath, err := validatePath(output.Path)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, err.Error())
            return
        }

        _, err = checkVerificationCode(regpath, verifier, timeout)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        if output.Block == nil || *(output.Block) {
            err := deleteDirectory(regpath, db, r.Context())
            if err != nil {
                dumpHttpErrorResponse(w, fmt.Errorf("failed to deregister %q; %w", regpath, err))
                return
            } else {
                dumpJsonResponse(w, http.StatusOK, map[string]string{ "status": "SUCCESS" })
                return
            }
        } else {
            go func() {
                err := deleteDirectory(regpath, db, r.Context())
                if err != nil {
                    log.Printf("failed to delete directory %q; %v", regpath, err)
                }
            }()
            dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING" })
            return
        }
    }
}

/**********************************************************************/

func newQueryHandler(db *sql.DB, tokenizer *unicodeTokenizer, wild_tokenizer *unicodeTokenizer, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        params := r.URL.Query()
        options := newQueryOptions()

        if params.Has("order") {
            val := params.Get("order")
            if val == "path" {
                options.Order.Type = queryOrderPath
                options.Order.Increasing = true
            } else if val == "-path" {
                options.Order.Type = queryOrderPath
                options.Order.Increasing = false
            } else if val == "time" {
                options.Order.Type = queryOrderTime
                options.Order.Increasing = true
            } else if val != "-time" {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "'order' should be one of 'time' or 'path'" })
                return
            }
        }

        if params.Has("scroll") {
            val := params.Get("scroll")
            if options.Order.Type == queryOrderPath {
                options.Scroll = &queryScroll{ Path: val }
            } else {
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

                options.Scroll = &queryScroll{ Time: time, Pid: pid }
            }
        }

        limit, err := getPageLimit(params, "limit", 100)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }
        options.PageLimit = limit

        if params.Has("metadata") {
            options.IncludeMetadata = (params.Get("metadata") != "false")
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
        err = dec.Decode(query)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to parse request body; %v", err) })
            return
        }

        if translate {
            query, err = translateQuery(query)
            if err != nil {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to translate text query; %v", err) })
                return
            }
        }

        san, err := sanitizeQuery(query, tokenizer, wild_tokenizer)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to sanitize an invalid query; %v", err) })
            return
        }

        res, err := queryTokens(san, db, r.Context(), options)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to query tokens; %v", err) })
            return
        }

        respbody := map[string]interface{} { "results": res }
        if len(res) == options.PageLimit {
            last := &(res[options.PageLimit - 1])
            var scroll string
            if options.Order.Type == queryOrderPath {
                scroll = last.Path
            } else {
                scroll = strconv.FormatInt(last.Time, 10) + "," + strconv.FormatInt(last.Pid, 10)
            }
            respbody["next"] = endpoint + "?" + encodeNextParameters("scroll", scroll, params, []string{ "order", "metadata", "translate" })
        }

        dumpJsonResponse(w, http.StatusOK, respbody)
        return
    }
}

/**********************************************************************/

func sanitizePath(path string) (string, error) {
    path, err := url.QueryUnescape(path)
    if err != nil {
        return "", fmt.Errorf("path is not properly URL-encoded; %w", err)
    }
    return filepath.Clean(path), nil
}

func getRetrievePath(params url.Values) (string, error) {
    if !params.Has("path") {
        return "", errors.New("expected a 'path' query parameter")
    }
    path, err := sanitizePath(params.Get("path"))
    return path, err
}

func newRetrieveMetadataHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        params := r.URL.Query()
        path, err := getRetrievePath(params)
        if err != nil {
            dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, err))
            return
        }

        use_metadata := true
        if params.Has("metadata") {
            use_metadata = (strings.ToLower(params.Get("metadata")) != "false")
        }

        res, err := retrievePath(path, use_metadata, db, r.Context())
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to retrieve path; %w", err))
            return
        }

        if res == nil {
            dumpErrorResponse(w, http.StatusNotFound, "path is not registered")
            return
        }

        dumpJsonResponse(w, http.StatusOK, res)
        return
    }
}

/**********************************************************************/

func newRetrieveFileHandler(db *sql.DB, whitelist linkWhitelist) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        params := r.URL.Query()
        path, err := getRetrievePath(params)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, err.Error())
            return
        }

        path, err = validatePath(path)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid path; %v", err))
            return
        }
        err = verifySymlinks(path, whitelist)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        okay, err := isDirectoryRegistered(filepath.Dir(path), db, r.Context())
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to check directory registration; %w", err))
            return
        }
        if !okay {
            dumpHttpErrorResponse(w, newHttpError(http.StatusForbidden, errors.New("cannot retrieve file from an unregistered path")))
            return
        }

        // We use Stat() to resolve any symlink to a file, given that all the symlinks are whitelisted.
        info, err := os.Stat(path)
        if err != nil {
            if errors.Is(err, os.ErrNotExist) {
                err = newHttpError(http.StatusNotFound, errors.New("path does not exist"))
            } else {
                err = fmt.Errorf("inaccessible path; %w", err)
            }
        } else if info.IsDir() {
            err = newHttpError(http.StatusBadRequest, errors.New("path should refer to a file, not a directory"))
        }
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        // Setting this for CORS.
        w.Header().Set("Access-Control-Allow-Origin", "*")

        if (r.Method == "HEAD") {
            w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
            w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
            w.Header().Set("Accept-Ranges", "bytes")

            ctype := mime.TypeByExtension(filepath.Ext(path))
            if (ctype == "") {
                r, err := os.Open(path)
                if err == nil {
                    // Copied from https://cs.opensource.google/go/go/+/refs/tags/go1.22.2:src/net/http/fs.go;l=239-246.
                    defer r.Close()
                    buf := make([]byte, 512)
                    n, err := io.ReadFull(r, buf[:])
                    if err == nil {
                        ctype = http.DetectContentType(buf[:n])
                    }
                }
            }
            if (ctype != "") {
                w.Header().Set("Content-Type", ctype)
            }

            w.WriteHeader(http.StatusOK);
        } else {
            http.ServeFile(w, r, path)
        }
    }
}

/**********************************************************************/

func newListFilesHandler(db *sql.DB, whitelist linkWhitelist) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        params := r.URL.Query()
        recursive := params.Get("recursive") == "true"
        path, err := getRetrievePath(params)
        if err != nil {
            dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, err))
            return
        }

        okay, err := isDirectoryRegistered(path, db, r.Context())
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to check path registration; %w", err))
            return
        }
        if !okay {
            dumpHttpErrorResponse(w, newHttpError(http.StatusForbidden, errors.New("cannot retrieve file from an unregistered path")))
            return
        }

        err = verifyDirectory(path, whitelist)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }

        listing, err := listFiles(path, recursive, whitelist, r.Context())
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to obtain a directory listing; %w", err))
            return
        }

        dumpJsonResponse(w, http.StatusOK, listing)
    }
}

func newListRegisteredDirectoriesHandler(db *sql.DB, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        options := listRegisteredDirectoriesOptions{}
        params := r.URL.Query()

        if params.Has("user") {
            user := params.Get("user")
            options.User = &user
        }

        if params.Has("contains_path") {
            path, err := sanitizePath(params.Get("contains_path"))
            if err != nil {
                dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, err))
                return
            }
            options.ContainsPath = &path
        }

        if params.Has("within_path") {
            path, err := sanitizePath(params.Get("within_path"))
            if err != nil {
                dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, err))
                return
            }
            options.WithinPath = &path
        }

        if params.Has("path_prefix") { // for back-compatibility.
            path, err := sanitizePath(params.Get("path_prefix"))
            if err != nil {
                dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, err))
                return
            }
            options.PathPrefix = &path
        }

        if params.Has("exists") {
            exists := params.Get("exists")
            options.Exists = &exists
        }

        limit, err := getPageLimit(params, "limit", 100)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }
        options.PageLimit = limit

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

            did, err := strconv.ParseInt(val[(i+1):], 10, 64)
            if err != nil {
                dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "failed to parse the directory ID from 'scroll'" })
                return
            }

            options.Scroll = &listRegisteredDirectoriesScroll{ Time: time, Did: did }
        }

        output, err := listRegisteredDirectories(db, r.Context(), options)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to check registered directories; %w", err))
            return
        }

        respbody := map[string]interface{} { "results": output }
        if len(output) == options.PageLimit {
            last := output[options.PageLimit - 1]
            respbody["next"] = endpoint + "?" + encodeNextParameters(
                "scroll",
                strconv.FormatInt(last.Time, 10) + "," + strconv.FormatInt(last.Did, 10),
                params, 
                []string { "user", "path_prefix", "within_path", "contains_path", "exists" },
            )
        }

        dumpJsonResponse(w, http.StatusOK, respbody)
    }
}

/**********************************************************************/

func newListFieldsHandler(db *sql.DB, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        options := listFieldsOptions{}
        params := r.URL.Query()

        if params.Has("pattern") {
            pattern := params.Get("pattern")
            options.Pattern = &pattern
        }

        if params.Has("count") {
            options.Count = params.Get("count") == "true"
        }

        limit, err := getPageLimit(params, "limit", 1000)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }
        options.PageLimit = limit

        if params.Has("scroll") {
            options.Scroll = &listFieldsScroll{ Field: params.Get("scroll") }
        }

        output, err := listFields(db, r.Context(), options)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to list available fields; %w", err))
            return
        }

        respbody := map[string]interface{} { "results": output }
        if len(output) == options.PageLimit {
            respbody["next"] = endpoint + "?" + encodeNextParameters(
                "scroll",
                output[options.PageLimit - 1].Field,
                params,
                []string { "pattern", "count" },
            )
        }

        dumpJsonResponse(w, http.StatusOK, respbody)
    }
}

func newListTokensHandler(db *sql.DB, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        options := listTokensOptions{}
        params := r.URL.Query()

        if params.Has("pattern") {
            pattern := params.Get("pattern")
            options.Pattern = &pattern
        }

        if params.Has("field") {
            field := params.Get("field")
            options.Field = &field
        }

        if params.Has("count") {
            options.Count = params.Get("count") == "true"
        }

        limit, err := getPageLimit(params, "limit", 1000)
        if err != nil {
            dumpHttpErrorResponse(w, err)
            return
        }
        options.PageLimit = limit

        if params.Has("scroll") {
            options.Scroll = &listTokensScroll{ Token: params.Get("scroll") }
        }

        output, err := listTokens(db, r.Context(), options)
        if err != nil {
            dumpHttpErrorResponse(w, fmt.Errorf("failed to list available tokens; %w", err))
            return
        }

        respbody := map[string]interface{} { "results": output }
        if len(output) == options.PageLimit {
            respbody["next"] = endpoint + "?" + encodeNextParameters(
                "scroll",
                output[options.PageLimit - 1].Token,
                params,
                []string { "pattern", "field", "count" },
            )
        }

        dumpJsonResponse(w, http.StatusOK, respbody)
    }
}

/**********************************************************************/

func newDefaultHandler() func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        dumpJsonResponse(w, http.StatusOK, map[string]string{ "name": "SewerRat API", "url": "https://github.com/ArtifactDB/SewerRat" })
    }
}

func newOptionsHandler() func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Headers", "*")
        w.WriteHeader(http.StatusNoContent)
    }
}
