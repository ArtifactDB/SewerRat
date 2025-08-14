package main

import (
    "testing"
    "io"
    "os"
    "os/user"
    "strings"
    "bytes"
    "path/filepath"
    "net/http"
    "net/http/httptest"
    "net/url"
    "encoding/json"
    "sort"
    "time"
    "strconv"
    "context"
)

func createAddOptionsForHandlerTests() (*addDirectoryContentsOptions, error) {
    lw, err := createSelfLinkWhitelist() 
    if err != nil {
        return nil, err
    }
    return &addDirectoryContentsOptions{ Concurrency: 1, LinkWhitelist: lw }, nil
}

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

func TestCleanPath(t *testing.T) {
    _, err := cleanPath("")
    if err == nil || !strings.Contains(err.Error(), "empty string") {
        t.Fatalf("expected an empty string error")
    }

    _, err = cleanPath("foobar")
    if err == nil || !strings.Contains(err.Error(), "absolute path") {
        t.Fatalf("expected an absolute path error")
    }

    path, err := cleanPath("/whee/foobar/")
    if err != nil {
        t.Fatal(err)
    }
    if path != "/whee/foobar" {
        t.Fatalf("expected elimination of trailing slashes, got %q instead", path)
    }

    path, err = cleanPath("/whee/a/../foobar/")
    if err != nil {
        t.Fatal(err)
    }
    if path != "/whee/foobar" {
        t.Fatalf("expected cleaning of the path, got %q instead", path)
    }
}

func TestCheckVerificationCode(t *testing.T) {
    v := newVerificationRegistry(time.Minute)

    t.Run("success", func(t * testing.T) {
        target, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        code, err := v.Provision(target)
        if err != nil {
            t.Fatal(err)
        }

        err = os.WriteFile(filepath.Join(target, code), []byte{}, 0644)
        if err != nil {
            t.Fatal(err)
        }

        info, err := checkVerificationCode(target, v, 1)
        if err != nil {
            t.Fatal(err)
        }

        if info == nil || !strings.HasPrefix(info.Name(), ".sewer_") {
            t.Fatalf("unexpected file %v", info)
        }
    })

    t.Run("no code", func(t * testing.T) {
        target, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        _, err = checkVerificationCode(target, v, 1)
        if err == nil || !strings.Contains(err.Error(), "no verification code") {
            t.Fatal("should have failed")
        }
    })

    t.Run("no file", func(t * testing.T) {
        target, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        _, err = v.Provision(target)
        if err != nil {
            t.Fatal(err)
        }

        _, err = checkVerificationCode(target, v, 1)
        if err == nil || !strings.Contains(err.Error(), "verification failed") {
            t.Fatal("should have failed")
        }
    })

    t.Run("hard links", func(t * testing.T) {
        target, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        code, err := v.Provision(target)
        if err != nil {
            t.Fatal(err)
        }

        err = os.WriteFile(filepath.Join(target, code), []byte{}, 0644)
        if err != nil {
            t.Fatal(err)
        }

        err = os.Link(filepath.Join(target, code), filepath.Join(target, "foo"))
        if err != nil {
            t.Fatal(err)
        }

        _, err = checkVerificationCode(target, v, 1)
        if err == nil || !strings.Contains(err.Error(), "hard link") {
            t.Fatal("should have failed")
        }
    })
}

func TestVerifyDirectory(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    wl, err := createSelfLinkWhitelist()
    if err != nil {
        t.Fatal(err)
    }

    err = verifyDirectory(dir, wl)
    if err != nil {
        t.Error(err)
    }

    // Fails if it doesn't exist.
    err = verifyDirectory(filepath.Join(dir, "BAR"), wl)
    if err == nil || !strings.Contains(err.Error(), "does not exist") {
        t.Error(err)
    }

    // Fails if it's not a directory.
    err = os.WriteFile(filepath.Join(dir, "FOO"), []byte{}, 0644)
    if err != nil {
        t.Fatal(err)
    }
    err = verifyDirectory(filepath.Join(dir, "FOO"), wl)
    if err == nil || !strings.Contains(err.Error(), "not a directory") {
        t.Error(err)
    }

    // Fails if it's a symlink.
    staging, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }
    new_path := filepath.Join(staging, "BAR")
    err = os.Symlink(dir, new_path)
    if err != nil {
        t.Fatal(err)
    }
    err = verifyDirectory(new_path, linkWhitelist{})
    if err == nil || !strings.Contains(err.Error(), "whitelisted") {
        t.Error(err)
    }

    // Unless it was whitelisted.
    err = verifyDirectory(new_path, wl)
    if err != nil {
        t.Error(err)
    }
}

func decodeStringyResponse(input io.Reader, t *testing.T) map[string]string {
    var output map[string]string
    dec := json.NewDecoder(input)
    err := dec.Decode(&output)
    if err != nil {
        t.Fatal(err)
    }
    return output
}

func createJsonRequest(method, endpoint string, body map[string]interface{}, t *testing.T) *http.Request {
    contents, err := json.Marshal(body)
    if err != nil {
        t.Fatal(err)
    }

    req, err := http.NewRequest(method, endpoint, bytes.NewReader(contents))
    if err != nil {
        t.Fatal(err)
    }

    return req
}

func TestRegisterHandlers(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }
    defer os.RemoveAll(tmp)

    dbpath := filepath.Join(tmp, "db.sqlite3")
    dbconn, err := initializeDatabase(dbpath)
    if err != nil {
        t.Fatal(err)
    }
    defer dbconn.Close()

    wl, err := createSelfLinkWhitelist()
    if err != nil {
        t.Fatal(err)
    }

    verifier := newVerificationRegistry(time.Minute)
    start_handler := http.HandlerFunc(newRegisterStartHandler(verifier, wl))

    t.Run("register start failed not absolute", func(t *testing.T) {
        req := createJsonRequest("POST", "/register/start", map[string]interface{}{ "path": "foo" }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusBadRequest {
            t.Fatalf("should have failed on a non-absolute path; %v", rr.Code)
        }

        output := decodeStringyResponse(rr.Body, t)
        if output["status"] != "ERROR" || !strings.Contains(output["reason"], "absolute") {
            t.Fatalf("unexpected body; %v", output)
        }
    })

    quickCreate := func() string {
        to_add, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatal(err)
        }
        return to_add
    }

    t.Run("register start ok", func(t *testing.T) {
        to_add := quickCreate()
        req := createJsonRequest("POST", "/register/start", map[string]interface{}{ "path": to_add }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatalf("should have succeeded; %v", rr.Code)
        }

        output := decodeStringyResponse(rr.Body, t)
        code := output["code"]
        if output["status"] != "PENDING" || !strings.HasPrefix(code, ".sewer_") {
            t.Fatalf("unexpected body; %v", output)
        }
    })

    t.Run("register start symlink", func(t *testing.T) {
        to_add := quickCreate()

        tmp, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        to_add2 := filepath.Join(tmp, "symlink")
        err = os.Symlink(to_add, to_add2)
        if err != nil {
            t.Fatal(err)
        }

        req := createJsonRequest("POST", "/register/start", map[string]interface{}{ "path": to_add2 }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatalf("should have succeeded")
        }
    })

    quickRegisterStart := func(to_add string) string {
        req := createJsonRequest("POST", "/register/start", map[string]interface{}{ "path": to_add }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatalf("should have succeeded; %v", rr.Code)
        }

        output := decodeStringyResponse(rr.Body, t)
        return output["code"]
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatal(err)
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    duration := time.Duration(1)
    finish_handler := http.HandlerFunc(newRegisterFinishHandler(dbconn, verifier, tokr, add_options, duration))

    t.Run("register finish fail no code", func(t *testing.T) {
        to_add := quickCreate()
        quickRegisterStart(to_add)
        req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add }, t)
        rr := httptest.NewRecorder()
        finish_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusUnauthorized {
            t.Fatalf("should have failed due to lack of code")
        }
    })

    t.Run("register finish ok", func(t *testing.T) {
        to_add := quickCreate()
        code := quickRegisterStart(to_add)
        err := os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
        if err != nil {
            t.Fatal(err)
        }

        req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add }, t)
        rr := httptest.NewRecorder()
        finish_handler.ServeHTTP(rr, req)
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
    })

    t.Run("register finish with empty names", func(t *testing.T) {
        to_add := quickCreate()
        quickRegisterStart(to_add)
        req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add, "base": []string{ "", "metadata.json" } }, t)
        rr := httptest.NewRecorder()
        finish_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusBadRequest {
            t.Fatalf("should have failed with empty names")
        }
    })

    t.Run("register finish with proper names", func(t *testing.T) {
        to_add := quickCreate()
        code := quickRegisterStart(to_add)
        err := os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
        if err != nil {
            t.Fatal(err)
        }

        req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add, "base": []string{ "metadata.json", "other.json" } }, t)
        rr := httptest.NewRecorder()
        finish_handler.ServeHTTP(rr, req)
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
    })

    t.Run("register finish reuse names", func(t *testing.T) {
        to_add := quickCreate()

        {
            code := quickRegisterStart(to_add)
            err := os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add, "base": []string{ "alpha.json", "bravo.json" } }, t)
            finish_handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded")
            }
        }

        // Re-registering with the same names.
        {
            code := quickRegisterStart(to_add)
            err := os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add }, t)
            finish_handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded")
            }

            all_paths, err := listDirs(dbconn)
            if err != nil {
                t.Fatal(err)
            }
            my_names, ok := all_paths[to_add]
            if !ok || !equalStringArrays(my_names, []string{ "alpha.json", "bravo.json" }) {
                t.Fatalf("unexpected paths in the database %v", my_names)
            }
        }

        // Re-registering with different names, to check that it indeed gets overridden.
        {
            code := quickRegisterStart(to_add)
            err := os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add, "base": []string{ "metadata.json" } }, t)
            finish_handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded")
            }

            all_paths, err := listDirs(dbconn)
            if err != nil {
                t.Fatal(err)
            }
            my_names, ok := all_paths[to_add]
            if !ok || !equalStringArrays(my_names, []string{ "metadata.json" }) {
                t.Fatalf("unexpected paths in the database %v", my_names)
            }
        }
    })

    t.Run("register finish without blocking", func(t *testing.T) {
        to_add := quickCreate()
        code := quickRegisterStart(to_add)
        err := os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
        if err != nil {
            t.Fatal(err)
        }

        req := createJsonRequest("POST", "/register/finish", map[string]interface{}{ "path": to_add, "base": []string{ "metadata.json" }, "block": false }, t)
        rr := httptest.NewRecorder()
        finish_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatal("should have accepted the non-blocking registration")
        }

        // Check if it's been indexed.
        okay := false
        for i := 0; i < 10; i++ {
            time.Sleep(time.Millisecond * 100)
            present, err := listPaths(dbconn, to_add)
            if err != nil {
                t.Fatal(err)
            }
            if len(present) > 0 {
                okay = true
                break
            }
        }
        if !okay {
            t.Fatal("non-blocking registration failed")
        }
    })
}

func TestDeregisterHandlers(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }
    defer os.RemoveAll(tmp)

    dbpath := filepath.Join(tmp, "db.sqlite3")
    dbconn, err := initializeDatabase(dbpath)
    if err != nil {
        t.Fatal(err)
    }
    defer dbconn.Close()

    verifier := newVerificationRegistry(time.Minute)
    start_handler := http.HandlerFunc(newDeregisterStartHandler(dbconn, verifier))

    t.Run("deregister start failed not absolute", func(t *testing.T) {
        req := createJsonRequest("POST", "/deregister/start", map[string]interface{}{ "path": "foo" }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusBadRequest {
            t.Fatalf("should have failed on a non-absolute path; %v", rr.Code)
        }
    })

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatal(err)
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    quickReadd := func() string {
        to_add, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }
        comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) != 0 {
            t.Fatalf("no comments should be present; %v", comments)
        }
        return to_add
    }

    t.Run("deregister start ok", func(t *testing.T) {
        to_add := quickReadd()
        req := createJsonRequest("POST", "/deregister/start", map[string]interface{}{ "path": to_add }, t)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatalf("should have succeeded; %v", rr.Code)
        }

        output := decodeStringyResponse(rr.Body, t)
        code := output["code"]
        if output["status"] != "PENDING" || !strings.HasPrefix(code, ".sewer_") {
            t.Fatalf("unexpected body; %v", output)
        }
    })

    quickDeregisterStart := func(path string) string {
        req := createJsonRequest("POST", "/deregister/start", map[string]interface{}{ "path": path }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatalf("should have succeeded; %v", rr.Code)
        }
        output := decodeStringyResponse(rr.Body, t)
        return output["code"]
    }

    duration := time.Second
    finish_handler := http.HandlerFunc(newDeregisterFinishHandler(dbconn, verifier, duration))

    t.Run("deregister finish fail no code", func(t *testing.T) {
        to_add := quickReadd()
        quickDeregisterStart(to_add)
        req := createJsonRequest("POST", "/deregister/finish", map[string]interface{}{ "path": to_add }, t)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        finish_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusUnauthorized {
            t.Fatalf("should have failed with a 401 (got %d instead)", rr.Code)
        }
    })

    t.Run("deregister finish ok", func(t *testing.T) {
        to_add := quickReadd()
        code := quickDeregisterStart(to_add)
        err := os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
        if err != nil {
            t.Fatal(err)
        }

        req := createJsonRequest("POST", "/deregister/finish", map[string]interface{}{ "path": to_add }, t)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        finish_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded; %v", rr.Code)
        }

        output := decodeStringyResponse(rr.Body, t)
        if output["status"] != "SUCCESS" {
            t.Fatalf("unexpected body; %v", output)
        }

        all_paths, err := listPaths(dbconn, to_add)
        if err != nil {
            t.Fatal(err)
        }
        if len(all_paths) != 0 {
            t.Fatalf("unexpected paths in the database %v", all_paths)
        }
    })

    t.Run("deregister finish without blocking", func(t *testing.T) {
        to_add := quickReadd()
        code := quickDeregisterStart(to_add)
        err = os.WriteFile(filepath.Join(to_add, code), []byte(""), 0644)
        if err != nil {
            t.Fatal(err)
        }

        handler := http.HandlerFunc(newDeregisterFinishHandler(dbconn, verifier, 1))
        req := createJsonRequest("POST", "/deregister/finish", map[string]interface{}{ "path": to_add, "block": false }, t)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatal("should have accepted the non-blocking deregistration")
        }

        // Check if it's been removed from the index.
        okay := false
        for i := 0; i < 10; i++ {
            time.Sleep(time.Millisecond * 100)
            present, err := listPaths(dbconn, to_add)
            if err != nil {
                t.Fatal(err)
            }
            if len(present) == 0 {
                okay = true
                break
            }
        }
        if !okay {
            t.Fatal("non-blocking deregistration failed")
        }
    })

    // Readding the directory, and then removing it from the file system.
    quickReaddAndRemove := func() string {
        to_add_and_remove := filepath.Join(tmp, "to_add_and_remove")
        err := mockDirectory(to_add_and_remove)
        if err != nil {
            t.Fatal(err)
        }

        comments, err := addNewDirectory(to_add_and_remove, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) != 0 {
            t.Fatal("no comments should be present")
        }

        err = os.RemoveAll(to_add_and_remove)
        if err != nil {
            t.Fatal(err)
        }

        return to_add_and_remove
    }

    t.Run("deregister immediate", func(t *testing.T) {
        removed := quickReaddAndRemove()
        req := createJsonRequest("POST", "/deregister/start", map[string]interface{}{ "path": removed }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        output := decodeStringyResponse(rr.Body, t)
        if output["status"] != "SUCCESS" {
            t.Fatalf("unexpected body")
        }

        all_paths, err := listPaths(dbconn, removed)
        if err != nil {
            t.Fatal(err)
        }
        if len(all_paths) != 0 {
            t.Fatalf("unexpected paths in the database %v", all_paths)
        }
    })

    t.Run("deregister immediate without blocking", func(t *testing.T) {
        removed := quickReaddAndRemove()
        req := createJsonRequest("POST", "/deregister/start", map[string]interface{}{ "path": removed, "block": false }, t)
        rr := httptest.NewRecorder()
        start_handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusAccepted {
            t.Fatalf("should have succeeded")
        }

        // Check if it's been removed from the index.
        okay := false
        for i := 0; i < 10; i++ {
            time.Sleep(time.Millisecond * 100)
            present, err := listPaths(dbconn, removed)
            if err != nil {
                t.Fatal(err)
            }
            if len(present) == 0 {
                okay = true
                break
            }
        }
        if !okay {
            t.Fatal("non-blocking deregistration failed")
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

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
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

        sort.Strings(all_paths)
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
        if !equalPathArrays(all_paths, []string{ "stuff/metadata.json", "whee/other.json" }, to_add) {
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
        if !equalPathArrays(all_paths, []string{ "stuff/other.json" }, to_add) {
            t.Fatalf("unexpected paths %v", all_paths)
        }
    })

    t.Run("translated", func (t *testing.T) {
        req, err := http.NewRequest("POST", "/query?translate=true", strings.NewReader(`{ "type": "text", "text": "lamb OR chicken" }`))
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
        if !equalPathArrays(all_paths, []string{ "metadata.json", "stuff/other.json" }, to_add) {
            t.Fatalf("unexpected paths %v", all_paths)
        }

        // Should fail without a closing brace.
        req, err = http.NewRequest("POST", "/query?translate=true", strings.NewReader(`{ "type": "text", "text": "lamb OR (chicken" }`))
        if err != nil {
            t.Fatal(err)
        }
        rr = httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusBadRequest {
            t.Fatalf("should have failed with a bad request")
        }
    })

    t.Run("wildcards", func (t *testing.T) {
        req, err := http.NewRequest("POST", "/query?translate=true", strings.NewReader(`{ "type": "text", "text": "l?mb OR chick*" }`))
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
        if !equalPathArrays(all_paths, []string{ "metadata.json", "stuff/other.json" }, to_add) {
            t.Fatalf("unexpected paths %v", all_paths)
        }
    })

    t.Run("order", func (t *testing.T) {
        dummy_query := `{ "type": "text", "text": "   " }`

        for _, opt := range []string{ "", "-path", "path", "-time", "time" } {
            endpoint := "/query"
            if opt != "" {
                endpoint += "?order=" + opt
            }

            req, err := http.NewRequest("POST", endpoint, strings.NewReader(dummy_query))
            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatal("should have succeeded")
            }

            output := struct {
                Results []queryResult
                Next string
            }{}

            dec := json.NewDecoder(rr.Body)
            err = dec.Decode(&output)
            if err != nil {
                t.Fatal(err)
            }
            if len(output.Results) != 4 {
                t.Fatalf("unexpected number of results %v", output)
            }

            var last_time int64
            var last_path string
            for i, x := range output.Results {
                if i > 0 {
                    failed := false
                    if opt == "" || opt == "-time" {
                        failed = last_time < x.Time
                    } else if opt == "time" {
                        failed = last_time > x.Time
                    } else if opt == "path" {
                        failed = last_path > x.Path
                    } else if opt == "-path" {
                        failed = last_path < x.Path
                    }
                    if failed {
                        t.Errorf("unexpected ordering with %s", opt)
                    }
                }
                last_path = x.Path
                last_time = x.Time
            }
        }

        t.Run("unknown option", func(t *testing.T) {
            req, err := http.NewRequest("POST", "/query?order=random", strings.NewReader(dummy_query))
            if err != nil {
                t.Fatal(err)
            }
            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusBadRequest {
                t.Fatal("should have failed with a bad request")
            }
        })
    })

    t.Run("scroll", func (t *testing.T) {
        for _, opt := range []string{ "", "path" } { // scrolling by time (default) or by path.
            dummy_query := `{ "type": "text", "text": "   " }`
            endpoint := "/query?limit=3"
            if opt != "" {
                endpoint += "&order=" + opt
            }

            // Scroll until exhaustion.
            all_paths := []string{}
            for i := 0; i < 100; i++ {
                req, err := http.NewRequest("POST", endpoint, strings.NewReader(dummy_query))
                if err != nil {
                    t.Fatal(err)
                }

                rr := httptest.NewRecorder()
                handler.ServeHTTP(rr, req)
                if rr.Code != http.StatusOK {
                    t.Fatalf("should have succeeded")
                }

                page_paths, scroll := validateSearchResults(rr.Body)
                all_paths = append(all_paths, page_paths...)

                if len(page_paths) != 3 {
                    if scroll == "" {
                        break
                    } else {
                        t.Fatalf("expected three paths from a limited page %v", page_paths)
                    }
                }

                endpoint = scroll
            }

            sort.Strings(all_paths)
            if !equalPathArrays(all_paths, []string{ "metadata.json", "stuff/metadata.json", "stuff/other.json", "whee/other.json" }, to_add) {
                t.Fatalf("unexpected paths %v", all_paths)
            }
        }
    })

    t.Run("no metadata", func (t *testing.T) {
        dummy_query := `{ "type": "text", "text": "aaron" }`

        req, err := http.NewRequest("POST", "/query?metadata=false", strings.NewReader(dummy_query))
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        output := struct {
            Results []map[string]interface{}
            Next string
        }{}

        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&output)
        if err != nil {
            t.Fatal(err)
        }
        if len(output.Results) == 0 { 
            t.Fatalf("unexpected lack of results; %v", output)
        }

        for _, x := range output.Results {
            if _, ok := x["metadata"]; ok {
                t.Errorf("expected no metadata property in %v", x)
            }
        }
    })
}

func TestRetrieveMetadataHandler(t *testing.T) {
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

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    handler := http.HandlerFunc(newRetrieveMetadataHandler(dbconn))

    self, err := user.Current()
    if err != nil {
        t.Fatal(err)
    }
    selfname := self.Username

    validateResult := func(input io.Reader, expected_path string, has_metadata bool) {
        r := map[string]interface{}{}
        dec := json.NewDecoder(input)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        path_i, ok := r["path"]
        if !ok {
            t.Fatalf("expected a path property in %v", r)
        }
        path, ok := path_i.(string)
        if !ok {
            t.Fatalf("expected a path string property in %v", r)
        }
        if path != expected_path {
            t.Fatalf("unexpected value for the path %s", path)
        }

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
        if has_metadata {
            if !ok {
                t.Fatalf("expected a metadata property in %v", r)
            }
            _, ok = meta_i.(map[string]interface{})
            if !ok {
                t.Fatalf("expected a metadata object property in %v", r)
            }
        } else {
            if ok {
                t.Fatalf("unexpected metadata property in %v", r)
            }
        }
    }

    t.Run("simple", func (t *testing.T) {
        candidate := filepath.Join(to_add, "metadata.json")
        req, err := http.NewRequest("GET", "/retrieve/metadata?path=" + url.QueryEscape(candidate), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        validateResult(rr.Body, candidate, true)
    })

    t.Run("no metadata", func (t *testing.T) {
        candidate := filepath.Join(to_add, "metadata.json")
        req, err := http.NewRequest("GET", "/retrieve/metadata?path=" + url.QueryEscape(candidate) + "&metadata=false", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        validateResult(rr.Body, candidate, false)
    })

    t.Run("missing", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/retrieve/metadata?path=missing.json&metadata=false", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusNotFound {
            t.Fatalf("should have failed without being found")
        }
    })
}

func TestRetrieveFileHandler(t *testing.T) {
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

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    // Here, nothing is actually indexed! So we can't get confused with the metadata retrievals.
    comments, err := addNewDirectory(to_add, []string{}, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    handler := http.HandlerFunc(newRetrieveFileHandler(dbconn, add_options.LinkWhitelist))

    t.Run("success", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/retrieve/file?path=" + url.QueryEscape(filepath.Join(to_add, "metadata.json")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        r := map[string]interface{}{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        foo, ok := r["foo"]
        if !ok || foo != "Aaron had a little lamb" {
            t.Fatal("unexpected result from file retrieval")
        }
    })

    t.Run("head", func (t *testing.T) {
        req, err := http.NewRequest("HEAD", "/retrieve/file?path=" + url.QueryEscape(filepath.Join(to_add, "metadata.json")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded")
        }

        headers := rr.Header()
        cl, err := strconv.Atoi(headers.Get("content-length"))
        if err != nil || cl == 0 {
            t.Fatal("expected a non-zero content-length header");
        }

        ct := headers.Get("content-type")
        if ct != "application/json" {
            t.Fatal("expected a JSON content type header");
        }

        lm := headers.Get("last-modified")
        _, err = time.Parse(time.RFC1123, lm)
        if err != nil {
            t.Fatalf("failed to parse the last-modified header; %v", err);
        }
    })

    t.Run("not found", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/retrieve/file?path=" + url.QueryEscape(filepath.Join(to_add, "other.json")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusNotFound {
            t.Fatalf("should have failed with a 404")
        }
    })

    t.Run("unregistered", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/retrieve/file?path=" + url.QueryEscape(filepath.Join(tmp, "metadata.json")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusForbidden {
            t.Fatalf("should have failed with a 403; got %v instead", rr.Code)
        }
    })

    t.Run("is directory", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/retrieve/file?path=" + url.QueryEscape(filepath.Join(to_add, "stuff")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusBadRequest {
            t.Fatalf("should have failed with a 400")
        }
    })

    err = os.Symlink(filepath.Join(to_add, "metadata.json"), filepath.Join(to_add, "FOO"))
    if err != nil {
        t.Fatal(err)
    }

    err = os.Symlink(filepath.Join(to_add, "stuff"), filepath.Join(to_add, "BAR"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("symlinks", func(t *testing.T) {
        t.Run("ok", func(t *testing.T) {
            // Testing both symlinks to individual files as well as files inside symlinked directories.
            for _, target := range []string{ "FOO", filepath.Join("BAR", "metadata.json") } {
                req, err := http.NewRequest("GET", "/retrieve/file?path=" + url.QueryEscape(filepath.Join(to_add, target)), nil)
                if err != nil {
                    t.Fatal(err)
                }

                rr := httptest.NewRecorder()
                handler.ServeHTTP(rr, req)
                if rr.Code != http.StatusOK {
                    t.Fatalf("retrieving %q should have succeeded, got %v instead", target, rr.Code)
                }

                r := map[string]interface{}{}
                dec := json.NewDecoder(rr.Body)
                err = dec.Decode(&r)
                if err != nil {
                    t.Fatal(err)
                }

                if target == "FOO" {
                    foo, ok := r["foo"]
                    if !ok || foo != "Aaron had a little lamb" {
                        t.Fatal("unexpected result from file retrieval")
                    }
                } else {
                    anime, ok := r["anime"]
                    if !ok || anime != "Yuri Yuru" {
                        t.Fatal("unexpected result from file retrieval")
                    }
                }
            }
        })

        t.Run("blocked", func(t *testing.T) {
            handler := http.HandlerFunc(newRetrieveFileHandler(dbconn, linkWhitelist{}))

            for _, target := range []string{ "FOO", filepath.Join("BAR", "metadata.json") } {
                req, err := http.NewRequest("GET", "/retrieve/file?path=" + url.QueryEscape(filepath.Join(to_add, target)), nil)
                if err != nil {
                    t.Fatal(err)
                }

                rr := httptest.NewRecorder()
                handler.ServeHTTP(rr, req)
                if rr.Code != http.StatusForbidden {
                    t.Fatalf("should have failed with non-whitelisted symlinks")
                }
            }
        })
    })
}

func TestListFilesHandler(t *testing.T) {
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

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    comments, err := addNewDirectory(to_add, []string{}, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    handler := http.HandlerFunc(newListFilesHandler(dbconn, nil))

    t.Run("non-recursive", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/list?path=" + url.QueryEscape(to_add), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := []string{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        sort.Strings(r)
        if len(r) != 3 || r[0] != "metadata.json" || r[1] != "stuff/" || r[2] != "whee/" {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("recursive", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/list?path=" + url.QueryEscape(to_add) + "&recursive=true", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := []string{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        sort.Strings(r)
        if len(r) != 4 || r[0] != "metadata.json" || r[1] != "stuff/metadata.json" || r[2] != "stuff/other.json" || r[3] != "whee/other.json" {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("nested", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/list?path=" + url.QueryEscape(filepath.Join(to_add, "stuff")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := []string{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        sort.Strings(r)
        if len(r) != 2 || r[0] != "metadata.json" || r[1] != "other.json" {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("not found", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/list?path=" + url.QueryEscape(filepath.Join(to_add, "missing")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusNotFound {
            t.Fatalf("should have failed with a 404 (got %d instead)", rr.Code)
        }
    })

    t.Run("unregistered", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/list?path=" + url.QueryEscape(tmp), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusForbidden {
            t.Fatalf("should have failed with a 403")
        }
    })

    t.Run("is file", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/list?path=" + url.QueryEscape(filepath.Join(to_add, "metadata.json")), nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusBadRequest {
            t.Fatalf("should have failed with a 400 (got %d instead)", rr.Code)
        }
    })

    // Note, symlink testing is handled in listFiles().
}

func TestListRegisteredDirectoriesHandler(t *testing.T) {
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

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    for _, name := range []string{ "akari", "ai", "alice" } {
        to_add := filepath.Join(tmp, "to_add_" + name)
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }

        comments, err := addNewDirectory(to_add, []string{"metadata.json"}, name, tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) != 0 {
            t.Fatal("no comments should be present")
        }
    }

    handler := http.HandlerFunc(newListRegisteredDirectoriesHandler(dbconn, "/registered"))

    type lrdResult struct {
        Path string
        User string
        Time int64
        Names []string
    }

    type lrdResponse struct {
        Results []lrdResult
        Next string
    }

    t.Run("basic", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/registered", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp := lrdResponse{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        r := resp.Results
        if len(r) != 3 || r[0].User != "alice" || r[1].User != "ai" || r[2].User != "akari" {
            t.Fatalf("unexpected listing results for the users %q", r)
        }
        if filepath.Base(r[0].Path) != "to_add_alice" || r[0].Time == 0 || r[0].Names[0] != "metadata.json" {
            t.Fatalf("unexpected listing results for the first entry %q", r)
        }
    })

    t.Run("filtered by user", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/registered?user=alice", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp := lrdResponse{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        r := resp.Results
        if len(r) != 1 || r[0].User != "alice" || filepath.Base(r[0].Path) != "to_add_alice" || r[0].Time == 0 || r[0].Names[0] != "metadata.json" {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("filtered by contains_path", func (t *testing.T) {
        inside := filepath.Join(tmp, "to_add_akari", "stuff")
        encoded := url.QueryEscape(inside)
        req, err := http.NewRequest("GET", "/registered?contains_path=" + encoded, nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp := lrdResponse{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        r := resp.Results
        if len(r) != 1 || r[0].User != "akari" {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("filtered by path_prefix", func (t *testing.T) {
        encoded := url.QueryEscape(filepath.Join(tmp, "to_add_ai"))
        req, err := http.NewRequest("GET", "/registered?path_prefix=" + encoded, nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp := lrdResponse{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        r := resp.Results
        if len(r) != 1 || filepath.Base(r[0].Path) != "to_add_ai" {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("filtered by within_path", func (t *testing.T) {
        encoded := url.QueryEscape(filepath.Join(tmp, "to_add_ai"))
        req, err := http.NewRequest("GET", "/registered?within_path=" + encoded, nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp := lrdResponse{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        r := resp.Results
        if len(r) != 1 || filepath.Base(r[0].Path) != "to_add_ai" {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("filtered by exists", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/registered?exists=false", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp := lrdResponse{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        // All directories exist, so exists=false should not pick up anything.
        r := resp.Results
        if len(r) != 0 {
            t.Fatalf("unexpected listing results %q", r)
        }
    })

    t.Run("scroll", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/registered?limit=2", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp := lrdResponse{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        r := resp.Results
        if len(r) != 2 || r[0].User != "alice" || r[1].User != "ai" {
            t.Fatalf("unexpected listing results for the users %q", r)
        }
        if resp.Next == "" {
            t.Fatal("expected next scroll to be non-empty")
        }

        // Continuing with the scroll.
        req, err = http.NewRequest("GET", resp.Next, nil)
        if err != nil {
            t.Fatal(err)
        }
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        resp = lrdResponse{}
        dec = json.NewDecoder(rr.Body)
        err = dec.Decode(&resp)
        if err != nil {
            t.Fatal(err)
        }

        r = resp.Results
        if len(r) != 1 || r[0].User != "akari" {
            t.Fatalf("unexpected listing results for the users %q", r)
        }
        if resp.Next != "" {
            t.Error("expected next scroll to be empty")
        }
    })
}

func TestListFieldsHandler(t *testing.T) {
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

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    handler := http.HandlerFunc(newListFieldsHandler(dbconn, "/fields"))

    t.Run("basic", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/fields", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := struct {
            Results []listFieldsResult
        }{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        if len(r.Results) == 0 || r.Results[0].Field != "anime" {
            t.Errorf("unexpected results; %v", r.Results)
        }
    })

    t.Run("counts", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/fields?count=true", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := struct {
            Results []listFieldsResult
        }{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        if len(r.Results) == 0 || r.Results[0].Field != "anime" || *(r.Results[0].Count) != 1 {
            t.Errorf("unexpected results; %v", r) 
        }
    })

    t.Run("scroll", func (t *testing.T) {
        endpoint := "/fields?limit=5"
        found := map[string]bool{}

        // Hitting up the scroll until exhaustion.
        for i := 0; i < 100; i++ {
            req, err := http.NewRequest("GET", endpoint, nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded (got %d)", rr.Code)
            }

            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded (got %d)", rr.Code)
            }

            r := struct {
                Results []listFieldsResult
                Next string
            }{}
            dec := json.NewDecoder(rr.Body)
            err = dec.Decode(&r)
            if err != nil {
                t.Fatal(err)
            }

            for _, res := range r.Results {
                if _, ok := found[res.Field]; ok {
                    t.Errorf("detected duplicate entries from scroll; %v", res.Field)
                }
                found[res.Field] = true
            }

            if len(r.Results) != 5 {
                if r.Next != "" {
                    t.Errorf("unexpected results; %v", r) 
                } else {
                    break
                }
            }

            if !strings.Contains(r.Next, "scroll=") {
                t.Errorf("expected a next string; %v", r) 
            }
            endpoint = r.Next
        }

        for _, expected := range []string{ "anime", "recipe", "category.iyashikei" } {
            if _, ok := found[expected]; !ok {
                t.Errorf("couldn't find the '%s' field; %v", expected, found)
            }
        }
    })

    t.Run("pattern", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/fields?pattern=characters.%2A", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := struct {
            Results []listFieldsResult
            Next string
        }{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }
        if len(r.Results) == 0 {
            t.Errorf("unexpected results; %v", r) 
        }
        for _, res := range r.Results {
            if !strings.HasPrefix(res.Field, "characters.") {
                t.Errorf("unexpected field detected; %v", res.Field)
            }
        }
    })
}

func TestListTokensHandler(t *testing.T) {
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

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options, err := createAddOptionsForHandlerTests()
    if err != nil {
        t.Fatal(err)
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) != 0 {
        t.Fatal("no comments should be present")
    }

    handler := http.HandlerFunc(newListTokensHandler(dbconn, "/tokens"))

    t.Run("basic", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/tokens", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := struct {
            Results []listTokensResult
        }{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        if len(r.Results) == 0 || r.Results[0].Token != "1" {
            t.Errorf("unexpected results; %v", r.Results)
        }
    })

    t.Run("counts", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/tokens?count=true", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := struct {
            Results []listTokensResult
        }{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }

        if len(r.Results) == 0 || r.Results[0].Token != "1" || *(r.Results[0].Count) != 1 {
            t.Errorf("unexpected results; %v", r) 
        }
    })

    t.Run("scroll", func (t *testing.T) {
        endpoint := "/tokens?limit=5"
        found := map[string]bool{}

        // Hitting up the scroll until exhaustion.
        for it := 0; it < 100; it++ {
            req, err := http.NewRequest("GET", endpoint, nil)
            if err != nil {
                t.Fatal(err)
            }

            rr := httptest.NewRecorder()
            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded (got %d)", rr.Code)
            }


            handler.ServeHTTP(rr, req)
            if rr.Code != http.StatusOK {
                t.Fatalf("should have succeeded (got %d)", rr.Code)
            }

            r := struct {
                Results []listTokensResult
                Next string
            }{}
            dec := json.NewDecoder(rr.Body)
            err = dec.Decode(&r)
            if err != nil {
                t.Fatal(err)
            }

            for _, res := range r.Results {
                if _, ok := found[res.Token]; ok {
                    t.Errorf("detected duplicate entries from scroll; %v", res.Token)
                }
                found[res.Token] = true
            }

            if len(r.Results) != 5 {
                if r.Next != "" {
                    t.Errorf("unexpected results; %v", r) 
                } else {
                    break
                }
            }

            if !strings.Contains(r.Next, "scroll=") {
                t.Errorf("expected a next string; %v", r) 
            }
            endpoint = r.Next
        }

        for _, expected := range []string{ "yuru", "aaron", "merino" } {
            if _, ok := found[expected]; !ok {
                t.Errorf("couldn't find the '%s' token; %v", expected, found)
            }
        }
    })

    t.Run("pattern", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/tokens?pattern=a%2A", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := struct {
            Results []listTokensResult
            Next string
        }{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }
        if len(r.Results) == 0 {
            t.Errorf("unexpected results; %v", r) 
        }
        for _, res := range r.Results {
            if !strings.HasPrefix(res.Token, "a") {
                t.Errorf("unexpected token detected; %v", res.Token)
            }
        }
    })

    t.Run("field", func (t *testing.T) {
        req, err := http.NewRequest("GET", "/tokens?field=characters.first", nil)
        if err != nil {
            t.Fatal(err)
        }

        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)
        if rr.Code != http.StatusOK {
            t.Fatalf("should have succeeded (got %d)", rr.Code)
        }

        r := struct {
            Results []listTokensResult
            Next string
        }{}
        dec := json.NewDecoder(rr.Body)
        err = dec.Decode(&r)
        if err != nil {
            t.Fatal(err)
        }
        if len(r.Results) == 0 {
            t.Errorf("unexpected results; %v", r) 
        }

        expected := map[string]bool{ "akari": true, "hoshino": true }
        for _, res := range r.Results {
            if _, ok := expected[res.Token]; !ok {
                t.Errorf("unexpected token detected; %v", res.Token)
            }
        }
    })
}
