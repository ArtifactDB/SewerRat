package main

import (
    "testing"
    "io"
    "os"
    "os/user"
    "strings"
    "path/filepath"
    "net/url"
    "net/http"
    "net/http/httptest"
    "encoding/json"
)

func TestDumpJsonResponse(t *testing.T) {
    rr := httptest.NewRecorder()
    dumpJsonResponse(rr, http.StatusBadRequest, map[string]string{ "foo": "bar" })
    if rr.Code != http.StatusBadRequest {
        t.Fatal("failed to respect the request")
    }

    var contents map[string]string
    dec := json.NewDecoder(rr.Body)
    dec.Decode(&contents)
    if contents["foo"] != "bar" {
        t.Fatalf("failed to dump JSON correctly")
    }

    headers := rr.Header()
    ct := headers["Content-Type"]
    if len(ct) != 1 || ct[0] != "application/json" {
        t.Fatalf("failed to set the content type correctly")
    }
}

func decodeRegisterResponse(input io.Reader, t *testing.T) map[string]string {
    var output map[string]string
    dec := json.NewDecoder(input)
    err := dec.Decode(&output)
    if err != nil {
        t.Fatal(err)
    }
    return output
}

func TestRegisterHandlers(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    dbpath := filepath.Join(tmp, "db.sqlite3")
    dbconn, err := initializeDatabase(dbpath)
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer dbconn.Close()

    scratch := filepath.Join(tmp, "scratch")
    err = os.Mkdir(scratch, 0700)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    var code string
    t.Run("register start", func(t *testing.T) {
        handler := http.HandlerFunc(newRegisterStartHandler(scratch, "/register/start/"))

        {
            req, err := http.NewRequest("POST", "/register/start/" + "foo", nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusBadRequest {
                t.Fatalf("should have failed on a non-absolute path; %v", rr.Code)
            }

            output := decodeRegisterResponse(rr.Body, t)
            if output["status"] != "ERROR" || !strings.Contains(output["reason"], "absolute") {
                t.Fatalf("unexpected body")
            }
        }

        {
            req, err := http.NewRequest("POST", "/register/start/" + url.QueryEscape(to_add), nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusAccepted {
                t.Fatalf("should have succeeded")
            }

            output := decodeRegisterResponse(rr.Body, t)
            code = output["code"]
            if output["status"] != "PENDING" || !strings.HasPrefix(code, ".sewer_") {
                t.Fatalf("unexpected body")
            }
        }
    })

    t.Run("register finish", func(t *testing.T) {
        handler := http.HandlerFunc(newRegisterFinishHandler(dbconn, scratch, tokr, "/register/finish/"))

        // First attempt fails, because we didn't add the registration code.
        {
            req, err := http.NewRequest("POST", "/register/finish/" + url.QueryEscape(to_add), nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusUnauthorized {
                t.Fatalf("should have failed due to lack of code")
            }
        }

        os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)

        {
            req, err := http.NewRequest("POST", "/register/finish/" + url.QueryEscape(to_add), nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded")
            }

            output := struct {
                Status string
                Comments []string
            }{}
            dec := json.NewDecoder(rr.Body)
            err = dec.Decode(&output)
            if err != nil {
                t.Fatal(err)
            }
            if output.Status != "SUCCESS" || len(output.Comments) > 0 {
                t.Fatalf("unexpected body")
            }

            all_paths, err := listPaths(dbconn, to_add)
            if err != nil {
                t.Fatal(err)
            }
            if !equalStringArrays(all_paths, []string{ "metadata.json", "stuff/metadata.json" }) {
                t.Fatalf("unexpected paths in the database %v", all_paths)
            }
        }

        {
            req, err := http.NewRequest("POST", "/register/finish/" + url.QueryEscape(to_add) + "?base=metadata.json,metadata.json", nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusBadRequest {
                t.Fatalf("should have failed with duplicate names")
            }
        }

        {
            req, err := http.NewRequest("POST", "/register/finish/" + url.QueryEscape(to_add) + "?base=metadata.json,other.json", nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded")
            }

            output := struct {
                Status string
                Comments []string
            }{}
            dec := json.NewDecoder(rr.Body)
            err = dec.Decode(&output)
            if err != nil {
                t.Fatal(err)
            }
            if output.Status != "SUCCESS" || len(output.Comments) > 0 {
                t.Fatalf("unexpected body")
            }

            all_paths, err := listPaths(dbconn, to_add)
            if err != nil {
                t.Fatal(err)
            }
            if !equalStringArrays(all_paths, []string{ "metadata.json", "stuff/metadata.json", "stuff/other.json", "whee/other.json" }) {
                t.Fatalf("unexpected paths in the database %v", all_paths)
            }
        }
    })
}

func TestDeregisterHandlers(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    dbpath := filepath.Join(tmp, "db.sqlite3")
    dbconn, err := initializeDatabase(dbpath)
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer dbconn.Close()

    scratch := filepath.Join(tmp, "scratch")
    err = os.Mkdir(scratch, 0700)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true, "other.json": true }, tokr)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    var code string
    t.Run("deregister start", func(t *testing.T) {
        handler := http.HandlerFunc(newDeregisterStartHandler(dbconn, scratch, "/deregister/start/"))

        {
            req, err := http.NewRequest("POST", "/deregister/start/" + "foo", nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusBadRequest {
                t.Fatalf("should have failed on a non-absolute path; %v", rr.Code)
            }
        }

        {
            req, err := http.NewRequest("POST", "/deregister/start/" + url.QueryEscape(to_add), nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusAccepted {
                t.Fatalf("should have succeeded")
            }

            output := decodeRegisterResponse(rr.Body, t)
            code = output["code"]
            if output["status"] != "PENDING" || !strings.HasPrefix(code, ".sewer_") {
                t.Fatalf("unexpected body")
            }
        }
    })

    t.Run("register finish", func(t *testing.T) {
        handler := http.HandlerFunc(newDeregisterFinishHandler(dbconn, scratch, "/deregister/finish/"))

        // First attempt fails, because we didn't add the registration code.
        {
            req, err := http.NewRequest("POST", "/deregister/finish/" + url.QueryEscape(to_add), nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusUnauthorized {
                t.Fatalf("should have failed due to lack of code")
            }
        }

        os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)

        {
            req, err := http.NewRequest("POST", "/deregister/finish/" + url.QueryEscape(to_add), nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded")
            }

            output := decodeRegisterResponse(rr.Body, t)
            if output["status"] != "SUCCESS" {
                t.Fatalf("unexpected body")
            }

            all_paths, err := listPaths(dbconn, to_add)
            if err != nil {
                t.Fatal(err)
            }
            if len(all_paths) != 0 {
                t.Fatalf("unexpected paths in the database %v", all_paths)
            }
        }
    })

    // Readding the directory, and then removing it from the file system.
    comments, err = addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true, "other.json": true }, tokr)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    err = os.RemoveAll(to_add)
    if err != nil {
        t.Fatal(err)
    }

    t.Run("deregister immediate", func(t *testing.T) {
        handler := http.HandlerFunc(newDeregisterStartHandler(dbconn, scratch, "/deregister/start/"))
        req, err := http.NewRequest("POST", "/deregister/start/" + url.QueryEscape(to_add), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        output := decodeRegisterResponse(rr.Body, t)
        if output["status"] != "SUCCESS" {
            t.Fatalf("unexpected body")
        }

        all_paths, err := listPaths(dbconn, to_add)
        if err != nil {
            t.Fatal(err)
        }
        if len(all_paths) != 0 {
            t.Fatalf("unexpected paths in the database %v", all_paths)
        }
    })
}

func TestQueryHandler(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    dbpath := filepath.Join(tmp, "db.sqlite3")
    dbconn, err := initializeDatabase(dbpath)
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer dbconn.Close()

    scratch := filepath.Join(tmp, "scratch")
    err = os.Mkdir(scratch, 0700)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    wtokr, err := newUnicodeTokenizer(true)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true, "other.json": true }, tokr)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    handler := http.HandlerFunc(newQueryHandler(dbconn, tokr, wtokr, "/query"))

    self, err := user.Current()
    if err != nil {
        t.Fatal(err)
    }
    selfname := self.Username

    validateSearchResults := func(input io.Reader) ([]string, string) {
        output := struct {
            Results []map[string]interface{}
            Next string
        }{}

        dec := json.NewDecoder(input)
        err = dec.Decode(&output)
        if err != nil {
            t.Fatal(err)
        }

        all_paths := []string{}
        for _, r := range output.Results {
            path_i, ok := r["path"]
            if !ok {
                t.Fatalf("expected a path property in %v", r)
            }
            path, ok := path_i.(string)
            if !ok {
                t.Fatalf("expected a path string property in %v", r)
            }
            all_paths = append(all_paths, path)

            user_i, ok := r["user"]
            if !ok {
                t.Fatalf("expected a user property in %v", r)
            }
            user, ok := user_i.(string)
            if !ok {
                t.Fatalf("expected a user string property in %v", r)
            }
            if user != selfname {
                t.Fatalf("unexpected username %v", user)
            }

            time_i, ok := r["time"]
            if !ok {
                t.Fatalf("expected a time property in %v", r)
            }
            time, ok := time_i.(float64)
            if !ok {
                t.Fatalf("expected a time integer property in %v", r)
            }
            if time <= 0 {
                t.Fatalf("time should be positive %v", time)
            }

            meta_i, ok := r["metadata"]
            if !ok {
                t.Fatalf("expected a metadata property in %v", r)
            }
            _, ok = meta_i.(map[string]interface{})
            if !ok {
                t.Fatalf("expected a metadata object property in %v", r)
            }
        }

        return all_paths, output.Next
    }

    t.Run("basic", func (t *testing.T) {
        req, err := http.NewRequest("POST", "/query", strings.NewReader(`{ "type": "text", "text": "aaron" }`))
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        all_paths, scroll := validateSearchResults(rr.Body)
        if scroll != "" {
            t.Fatalf("unexpected scroll %v", scroll)
        }
        if len(all_paths) != 1 || all_paths[0] != filepath.Join(to_add, "metadata.json") {
            t.Fatalf("unexpected paths %v", all_paths)
        }
    })

    t.Run("multiple", func (t *testing.T) {
        // Check that we report multiple hits correctly.
        req, err := http.NewRequest("POST", "/query", strings.NewReader(`{ "type": "text", "text": "yuru" }`))
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        all_paths, scroll := validateSearchResults(rr.Body)
        if scroll != "" {
            t.Fatalf("unexpected scroll %v", scroll)
        }
        if len(all_paths) != 2 || all_paths[0] != filepath.Join(to_add, "whee/other.json") || all_paths[1] != filepath.Join(to_add, "stuff/metadata.json") {
            t.Fatalf("unexpected paths %v", all_paths)
        }
    })

    t.Run("sanitized", func (t *testing.T) {
        // Check that the query is indeed sanitized.
        req, err := http.NewRequest("POST", "/query", strings.NewReader(`{ "type": "text", "text": "lamb chicken" }`))
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        all_paths, scroll := validateSearchResults(rr.Body)
        if scroll != "" {
            t.Fatalf("unexpected scroll %v", scroll)
        }
        if len(all_paths) != 1 || all_paths[0] != filepath.Join(to_add, "stuff/other.json") {
            t.Fatalf("unexpected paths %v", all_paths)
        }
    })

    t.Run("scroll", func (t *testing.T) {
        dummy_query := `{ "type": "text", "text": "   " }`

        req, err := http.NewRequest("POST", "/query?limit=2", strings.NewReader(dummy_query))
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        all_paths, scroll := validateSearchResults(rr.Body)
        if !strings.HasPrefix(scroll, "/query?scroll=") {
            t.Fatalf("unexpected scroll %v", scroll)
        }
        if len(all_paths) != 2 || all_paths[0] != filepath.Join(to_add, "whee/other.json") || all_paths[1] != filepath.Join(to_add, "stuff/other.json") {
            t.Fatalf("unexpected paths %v", all_paths)
        }

        // Next scroll.
        req, err = http.NewRequest("POST", scroll, strings.NewReader(dummy_query))
        if err != nil {
            t.Fatal(err)
        }

        rr = httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        all_paths, scroll = validateSearchResults(rr.Body)
        if scroll != "" { // fully exhausted the scroll now.
            t.Fatalf("unexpected scroll %v", scroll)
        }
        if len(all_paths) != 2 || all_paths[0] != filepath.Join(to_add, "stuff/metadata.json") || all_paths[1] != filepath.Join(to_add, "metadata.json") {
            t.Fatalf("unexpected paths %v", all_paths)
        }
    })
}
