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

func newRegisterStartHandler(scratch string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, "/register/start/")
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

func newRegisterFinishHandler(db *sql.DB, scratch string, tokenizer *unicodeTokenizer) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, "/register/finish/")
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

        failures, err := addDirectory(db, r.Context(), regpath, tokenizer)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to index directory; %v", err) })
            return
        }

        dumpJsonResponse(w, http.StatusOK, map[string]interface{}{ "status": "SUCCESS", "comments": failures })
        return
    }
}

/**********************************************************************/

func newDeregisterStartHandler(db *sql.DB, scratch string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, "/deregister/start/")
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

func newDeregisterFinishHandler(db *sql.DB, scratch string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, "/deregister/finish/")
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


