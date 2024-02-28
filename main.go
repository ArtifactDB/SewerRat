package main

import (
    "flag"
    "log"
    "fmt"

    "os"
    "path/filepath"

    "net/http"
    "net/url"

    "encoding/base64"
    "encoding/json"

    "sync"
    "strings"
    "errors"
    "context"

    "crypto/rand"
    "database/sql"
)

/**********************************************************************/

func createTables(path string) error {
    accessible := false
    if _, err := os.Stat(path); err == nil {
        accessible = true
    }

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("failed to create SQLite file at %q; %w", path, err)
	}
    defer db.Close()

    if (!accessible) {
        _, err = db.Exec(`
CREATE TABLE directories(did INTEGER PRIMARY KEY, directory TEXT NOT NULL, user TEXT NOT NULL, timestamp INTEGER NOT NULL)
CREATE INDEX index_dir_user ON directories(user, timestamp)
CREATE INDEX index_dir_timestamp ON directories(timestamp, user)

CREATE TABLE paths(pid INTEGER PRIMARY KEY, did INTEGER, path TEXT NOT NULL, timestamp INTEGER NOT NULL, metadata BLOB, FOREIGN KEY(did) REFERENCES directories(did) ON DELETE CASCADE)
CREATE INDEX index_paths_user ON paths(user, timestamp)
CREATE INDEX index_paths_timestamp ON paths(timestamp, user)

CREATE TABLE tokens(tid INTEGER PRIMARY KEY, token TEXT NOT NULL UNIQUE)
CREATE INDEX index_tokens ON tokens(token)

CREATE TABLE fields(fid INTEGER PRIMARY KEY, field TEXT NOT NULL UNIQUE)
CREATE INDEX index_fields ON fields(field)

CREATE TABLE links(pid INTEGER, fid INTEGER, tid INTEGER, FOREIGN KEY(pid) REFERENCES paths(pid) ON DELETE CASCADE)
CREATE INDEX index_links ON links(tid, fid)
`)
        if err != nil {
            os.Remove(path)
            return fmt.Errorf("failed to create table in %q; %w", path, err)
        }
    }

    return nil
}

func addDirectory(db sql.DB, ctx context.Content, directory string, user string) error {
    gathered := []string{}
    err := path.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
        if err == nil && !d.IsDir() && filepath.Base(d.Name()) == "_metadata.json" {
            gathered = append(gathered, d.Name())
        }
        return nil
    })
    if err != nil {
        return fmt.Errorf("failed to walk through directory at %q; %w", dir, err)
    }

    // Make a request.
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to prepare a database transaction; %w", err)
    }
    defer tx.Rollback()

    // Removing the current directory to propagate deletions, and then adding it again.
    _, err = tx.Exec("DELETE FROM directories WHERE directory = ?", directory) 
    if err != nil {
        return fmt.Errorf("failed to delete existing entry for %q; %w", dir, err)
    }

    var id int
    err = tx.QueryRow("INSERT INTO directories(directory, user, timestamp) VALUES(?, ?, ?) RETURNING vid", directory, user, time.Now().Unix()).Scan(&id)
    if err != nil {
        return fmt.Errorf("failed to insert new entry for %q; %w", dir, err)
    }

    // Looping through and parsing each document.
    contents := make([]interface{}, len(gathered))
    failures := make([]error, len(gathered))
    var wg sync.WaitGroup
    wg.Add(len(gathered))

    for i, f := range gathered {
        go func(i int, f string) {
            defer wg.Done()
            contents, err := os.ReadFile(f)
            if err != nil {
                failures[i] = fmt.Errorf("failed to read %q; %w", f, err.Error())
                return
            }

            var vals interface{}
            err = json.Unmarshal(contents, &vals)
            if err != nil {
                failures[i] = fmt.Errorf("failed to parse %q; %w", f, err.Error())
                return
            }

            contents[i] = vals
        }(i, f)
    }
}

func addDirectory(tx sql.Tx, contents interface{}) error {



}

/**********************************************************************/

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

func validatePath(encoded string) (string, error) {
    if (encoded == "") {
        return "", errors.New("path parameter should be a non-empty string")
    }

    regpath, err := url.QueryUnescape(encoded)
    if (err != nil) {
        return "", errors.New("path parameter should be a URL-encoded path")
    }

    if (!filepath.IsAbs(regpath)) {
        return "", errors.New("path parameter should be an absolute path")
    }

    return regpath, nil
}

func newRegisterStartHandler(scratch string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, "/register/start/")
        regpath, err := validatePath(encpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() });
            return;
        }

        // Generate a unique string to indicate that the user indeed has write permissions here.
        var candidate string
        buff := make([]byte, 40)
        found := false
        for i := 0; i < 10; i++ {
            _, err := rand.Read(buff)
            if err != nil {
                continue
            }

            candidate = ".deposit_" + base64.RawURLEncoding.EncodeToString(buff)
            _, err = os.Stat(filepath.Join(regpath, candidate))

            if err != nil {
                if errors.Is(err, os.ErrNotExist) {
                    found = true
                    break
                } else if errors.Is(err, os.ErrPermission) {
                    dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "path is not accessible" });
                    return
                } else {
                    dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": "failed to inspect path; " + err.Error() });
                    return
                }
            }
        }

        if !found {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": "failed to generate a suitable verification value" });
        }

        reencpath := url.QueryEscape(regpath) // re-encoding it to guarantee that there isn't any weirdness.
        err = os.WriteFile(filepath.Join(scratch, reencpath), []byte(candidate), 0600)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": err.Error() });
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "value": candidate });
    }
}

/**********************************************************************/

func main() {
    db0 := flag.String("db", "", "Path to the SQLite file for the metadata")
    scratch0 := flag.String("scratch", "", "Path to a scratch directory")
    port0 := flag.String("port", "", "Port to listen to for requests")
    flag.Parse()

    db := *db0
    port := *port0
    scratch := *scratch0
    if db == "" || port == "" || scratch == "" {
        flag.Usage()
        os.Exit(1)
    }

    err := os.MkdirAll(scratch, 700)
    if err != nil {
        log.Fatalf("failed to create the scratch directory at %q; %w", scratch, err)
    }

    err = createTables(db)
    if err != nil {
        log.Fatalf("failed to create the initial SQLite file at %q; %w", db, err)
    }

    http.HandleFunc("PUT /register/start/", newRegisterStartHandler(scratch))

    log.Fatal(http.ListenAndServe(":" + port, nil))
}
