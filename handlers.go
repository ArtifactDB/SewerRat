package main

import (
    "log"
    "fmt"

    "os"
    "path/filepath"

    "net/http"
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

    w.Header()["Content-Type"] = []string { "application/json" }
    _, err = w.Write(contents)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        log.Printf("failed to write JSON response; %v", err)
        return
    } else {
        w.WriteHeader(status)
    }
}

/**********************************************************************/

func newRegisterStartHandler(scratch string, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, endpoint)
        regpath, err := validateRequestPath(encpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        candidate, err := createVerificationCode(regpath)
        if err != nil {
            status := http.StatusInternalServerError
            if errors.Is(err, os.ErrPermission) {
                status = http.StatusBadRequest
            }
            dumpJsonResponse(w, status, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to generate a suitable verification code for %q; %v", regpath, err) })
            return
        }

        err = depositVerificationCode(scratch, regpath, candidate)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to deposit verification code for %q; %v", regpath, err) })
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "value": candidate })
        return
    }
}

func newRegisterFinishHandler(db *sql.DB, scratch string, tokenizer *unicodeTokenizer, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, endpoint)
        regpath, err := validateRequestPath(encpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        expected_code, err := fetchVerificationCode(scratch, regpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to fetch verification code for %q; %v", regpath, err) })
            return
        }

        expected_path := filepath.Join(regpath, expected_code)
        _, err = os.Stat(expected_path)
        if errors.Is(err, os.ErrNotExist) {
            dumpJsonResponse(w, http.StatusUnauthorized, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("verification failed for %q; %v", regpath, err) })
            return
        } else if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to inspect verification code for %q; %v", regpath, err) })
            return
        }

        failures, err := addDirectory(db, regpath, tokenizer)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to index directory; %v", err) })
            return
        }

        dumpJsonResponse(w, http.StatusOK, map[string]interface{}{ "status": "SUCCESS", "comments": failures })
        return
    }
}

/**********************************************************************/

func newDeregisterStartHandler(db *sql.DB, scratch string, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, endpoint)
        regpath, err := validateRequestPath(encpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        // If the directory doesn't exist, then we don't need to attempt to create a verification code.
        if _, err := os.Stat(regpath); errors.Is(err, os.ErrNotExist) {
            err := deleteDirectory(db, regpath)
            if err != nil {
                dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to deregister %q; %v", regpath, err) })
                return
            } else {
                dumpJsonResponse(w, http.StatusOK, map[string]string{ "status": "SUCCESS" })
                return
            }
        }

        candidate, err := createVerificationCode(regpath)
        if err != nil {
            status := http.StatusInternalServerError
            if errors.Is(err, os.ErrPermission) {
                status = http.StatusBadRequest
            }
            dumpJsonResponse(w, status, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to generate a suitable verification code for %q; %v", regpath, err) })
            return
        }

        err = depositVerificationCode(scratch, regpath, candidate)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to deposit verification code for %q; %v", regpath, err) })
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "value": candidate })
        return
    }
}

func newDeregisterFinishHandler(db *sql.DB, scratch string, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, endpoint)
        regpath, err := validateRequestPath(encpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        expected_code, err := fetchVerificationCode(scratch, regpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to fetch verification code for %q; %v", regpath, err) })
            return
        }

        expected_path := filepath.Join(regpath, expected_code)
        _, err = os.Stat(expected_path)
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

func newQueryHandler(db *sql.DB, tokenizer *unicodeTokenizer, wild_tokenizer *unicodeTokenizer, endpoint string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        qparam_str := strings.TrimPrefix(r.URL.Path, endpoint)
        
        var scroll *scrollPosition
        limit := 100
        if qparam_str != "" {
            if qparam_str[0] == '?' {
                qparams := strings.Split(qparam_str, "&")

                for _, q := range qparams {
                    if strings.HasPrefix(q, "scroll=") {
                        scroll_info := strings.TrimPrefix(q, "scroll=")
                        i := strings.Index(scroll_info, ",")
                        time, err := strconv.ParseInt(scroll_info[:i], 10, 64)
                        if err != nil {
                            continue
                        }
                        pid, err := strconv.ParseInt(scroll_info[(i+1):], 10, 64)
                        if err != nil {
                            continue
                        }
                        scroll = &scrollPosition{ Time: time, Pid: pid }

                    } else if strings.HasPrefix(q, "limit=") {
                        limit_info := strings.TrimPrefix(q, "limit=")
                        limit0, err := strconv.Atoi(limit_info)
                        if err == nil && limit0 > 0 && limit0 < limit {
                            limit = limit0
                        }
                    }
                }
            }
        }

        query := searchClause{}
        restricted := http.MaxBytesReader(w, r.Body, 1048576)
        dec := json.NewDecoder(restricted)
        err := dec.Decode(&query)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to parse response body; %v", err) })
            return 
        }

        san, err := sanitizeQuery(&query, tokenizer, wild_tokenizer)
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
            respbody["scroll"] = endpoint + "?scroll=" + strconv.FormatInt(last.Time, 10) + "," + strconv.FormatInt(last.Pid, 10)
        }

        dumpJsonResponse(w, http.StatusOK, respbody)
        return
    }
}

