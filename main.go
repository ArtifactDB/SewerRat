package main

import (
    "flag"
    "log"
    "fmt"

    "os"
    "io/fs"
    "path/filepath"
    "syscall"
    "os/user"

    "net/http"
    "net/url"

    "encoding/base64"
    "encoding/json"

    "sync"
    "time"
    "errors"
    "context"

    "crypto/rand"
    "database/sql"
    _ "modernc.org/sqlite"

    "strings"
    "strconv"
    "regexp"
	"unicode"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/runes"
)

/**********************************************************************/

func initializeDatabase(path string) (*sql.DB, error) {
    accessible := false
    if _, err := os.Stat(path); err == nil {
        accessible = true
    }

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLite file at %q; %w", path, err)
	}

    if (!accessible) {
        _, err = db.Exec(`
CREATE TABLE directories(did INTEGER PRIMARY KEY, directory TEXT NOT NULL, user TEXT NOT NULL, timestamp INTEGER NOT NULL)
CREATE INDEX index_dir_user ON directories(user, timestamp)
CREATE INDEX index_dir_timestamp ON directories(timestamp, user)

CREATE TABLE paths(pid INTEGER PRIMARY KEY, did INTEGER, path TEXT NOT NULL, metadata BLOB, FOREIGN KEY(did) REFERENCES directories(did) ON DELETE CASCADE)
CREATE INDEX index_paths_did ON paths(did, path)
CREATE INDEX index_paths_path ON paths(path)

CREATE TABLE tokens(tid INTEGER PRIMARY KEY, token TEXT NOT NULL UNIQUE)
CREATE INDEX index_tokens ON tokens(token)

CREATE TABLE fields(fid INTEGER PRIMARY KEY, field TEXT NOT NULL UNIQUE)
CREATE INDEX index_fields ON fields(field)

CREATE TABLE links(pid INTEGER, fid INTEGER, tid INTEGER, FOREIGN KEY(pid) REFERENCES paths(pid) ON DELETE CASCADE)
CREATE INDEX index_links ON links(tid, fid)
`)
        if err != nil {
            os.Remove(path)
            return nil, fmt.Errorf("failed to create table in %q; %w", path, err)
        }
    }

    return db, nil
}

/**********************************************************************/

type unicodeTokenizer struct {
    Stripper transform.Transformer
    Splitter *regexp.Regexp
}

func newUnicodeTokenizer(allow_wildcards bool) (*unicodeTokenizer, error) {
    pattern := ""
    if allow_wildcards {
        pattern = "%_"
    }

    comp, err := regexp.Compile("[^\\p{L}\\p{N}\\p{Co}-" + pattern + "]+")
    if err != nil {
        return nil, fmt.Errorf("failed to compile regex; %w", err)
    }

    return &unicodeTokenizer {
	    Stripper: transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC),
        Splitter: comp,
    }, nil
}

func (u *unicodeTokenizer) Tokenize(x string) ([]string, error) {
    y, _, err := transform.String(u.Stripper, x)
    if err != nil {
        return nil, fmt.Errorf("failed to strip diacritics from %q; %w", x, err)
    }

    y = strings.ToLower(y)
    output := u.Splitter.Split(y, -1)

    final := []string{}
    for _, t := range output {
        if len(t) > 0 {
            final = append(final, t)
        }
    }
    return final, nil
}

type insertStatements struct {
    PathInsert *sql.Stmt
    TokenInsert *sql.Stmt
    FieldInsert *sql.Stmt
    LinkInsert *sql.Stmt
}

func newInsertStatements(tx *sql.Tx) (*insertStatements, error) {
    p, err := tx.Prepare("INSERT INTO paths(did, path, metadata) VALUES(?, ?, ?) RETURNING pid")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare path insertion statement; %w", err)
    }

    t, err := tx.Prepare("INSERT OR IGNORE INTO tokens(token) VALUES(?)")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare token insertion statement; %w", err)
    }

    f, err := tx.Prepare("INSERT OR IGNORE INTO fields(field) VALUES(?)")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare field insertion statement; %w", err)
    }

    l, err := tx.Prepare("INSERT INTO links(pid, fid, tid) VALUES(?, ?, ?)")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare link insertion statement; %w", err)
    }

    return &insertStatements {
        PathInsert: p,
        TokenInsert: t,
        FieldInsert: f,
        LinkInsert: l,
    }, nil
}

func (i *insertStatements) Close() error {
    err := i.PathInsert.Close()
    if err != nil {
        return fmt.Errorf("failed to close path insertion statement; %w", err)
    }

    err = i.TokenInsert.Close()
    if err != nil {
        return fmt.Errorf("failed to close token insertion statement; %w", err)
    }

    err = i.FieldInsert.Close()
    if err != nil {
        return fmt.Errorf("failed to close field insertion statement; %w", err)
    }

    err = i.LinkInsert.Close()
    if err != nil {
        return fmt.Errorf("failed to close link insertion statement; %w", err)
    }

    return nil
}

func addDirectory(db *sql.DB, ctx context.Context, directory string, user string, tokenizer *unicodeTokenizer) error {
    gathered := []string{}
    filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
        // Just skip any directories that we can't access.
        if err != nil {
            log.Printf("failed to walk %q; %v", path, err)
        } else if !d.IsDir() && filepath.Base(d.Name()) == "_metadata.json" {
            gathered = append(gathered, d.Name())
        }
        return nil
    })

    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to prepare a database transaction; %w", err)
    }
    defer tx.Rollback()

    // Removing the current directory to propagate deletions, and then adding it again.
    _, err = tx.Exec("DELETE FROM directories WHERE directory = ?", directory)
    if err != nil {
        return fmt.Errorf("failed to delete existing entry for %q; %w", directory, err)
    }

    var did int
    err = tx.QueryRow("INSERT INTO directories(directory, user, timestamp) VALUES(?, ?, ?) RETURNING vid", directory, user, time.Now().Unix()).Scan(&did)
    if err != nil {
        return fmt.Errorf("failed to insert new entry for %q; %w", directory, err)
    }

    prepped, err := newInsertStatements(tx)
    if err != nil {
        return fmt.Errorf("failed to create prepared insertion statements for %q; %w", directory, err)
    }
    defer prepped.Close()

    // Looping through and parsing each document using multiple goroutines.
    contents := make([]interface{}, len(gathered))
    payload := make([][]byte, len(gathered))
    failures := make([]error, len(gathered))
    var wg sync.WaitGroup
    wg.Add(len(gathered))

    for i, f := range gathered {
        go func(i int, f string) {
            defer wg.Done()
            raw, err := os.ReadFile(f)
            if err != nil {
                failures[i] = fmt.Errorf("failed to read %q; %w", f, err.Error())
                return
            }

            var vals interface{}
            err = json.Unmarshal(raw, &vals)
            if err != nil {
                failures[i] = fmt.Errorf("failed to parse %q; %w", f, err.Error())
                return
            }

            payload[i] = raw
            contents[i] = vals
        }(i, f)
    }
    wg.Wait()

    // Adding each document to the pile. We do this in serial because I don't think transactions are thread-safe.
    for i, f := range contents {
        if failures[i] != nil {
            log.Printf(failures[i].Error())
            continue
        }

        var pid int
        err := prepped.PathInsert.QueryRow(did, f, payload[i]).Scan(&pid)
        if err != nil {
            log.Printf("failed to insert %q into the database; %v", f, err)
            continue
        }

        tokenizeMetadata(tx, contents[i], gathered[i], pid, "", prepped, tokenizer)
    }

    err = tx.Commit()
    if err != nil {
        return fmt.Errorf("failed to commit the transaction for %q; %w", directory, err)
    }

    return nil
}

func tokenizeMetadata(tx *sql.Tx, contents interface{}, path string, pid int, field string, prepped *insertStatements, tokenizer *unicodeTokenizer) {
    switch v := contents.(type) {
    case []interface{}:
        for _, w := range v {
            tokenizeMetadata(tx, w, path, pid, field, prepped, tokenizer)
        }

    case map[string]interface{}:
        for k, w := range v {
            new_field := k
            if field != "" {
                new_field = field + "." + k
            }
            tokenizeMetadata(tx, w, path, pid, new_field, prepped, tokenizer)
        }

    case string:
        tokens, err := tokenizer.Tokenize(v)
        if err != nil {
            log.Printf("failed to tokenize %q in %q; %v", v, path, err)
            return
        }

        for _, t := range tokens {
            _, err := prepped.TokenInsert.Exec(t)
            if err != nil {
                log.Printf("failed to insert token %q from %q; %v", t, path, err)
                continue
            }

            _, err = prepped.FieldInsert.Exec(field)
            if err != nil {
                log.Printf("failed to insert field %q from %q; %v", field, path, err)
                continue
            }

            _, err = prepped.LinkInsert.Exec(pid, field, t)
            if err != nil {
                log.Printf("failed to insert link for field %q to token %q from %q; %v", field, t, path, err)
                continue
            }
        }
    }
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
    if encoded == "" {
        return "", errors.New("path parameter should be a non-empty string")
    }

    regpath, err := url.QueryUnescape(encoded)
    if err != nil {
        return "", errors.New("path parameter should be a URL-encoded path")
    }

    if !filepath.IsAbs(regpath) {
        return "", errors.New("path parameter should be an absolute path")
    }

    return regpath, nil
}

func identifyUser(info fs.FileInfo) (string, error) {
    stat, ok := info.Sys().(*syscall.Stat_t)
    if !ok {
        return "", errors.New("failed to extract system information");
    }

    uinfo, err := user.LookupId(strconv.Itoa(int(stat.Uid)))
    if !ok {
        return "", fmt.Errorf("failed to find user name for author; %w", err)
    }
    return uinfo.Username, nil
}

/**********************************************************************/

func newRegisterStartHandler(scratch string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, "/register/start/")
        regpath, err := validatePath(encpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
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

            candidate = ".sewer_" + base64.RawURLEncoding.EncodeToString(buff)
            _, err = os.Stat(filepath.Join(regpath, candidate))

            if err != nil {
                if errors.Is(err, os.ErrNotExist) {
                    found = true
                    break
                } else if errors.Is(err, os.ErrPermission) {
                    dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": "path is not accessible" })
                    return
                } else {
                    dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": "failed to inspect path; " + err.Error() })
                    return
                }
            }
        }

        if !found {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": "failed to generate a suitable verification code" })
            return
        }

        reencpath := url.QueryEscape(regpath) // re-encoding it to guarantee that there isn't any weirdness.
        err = os.WriteFile(filepath.Join(scratch, reencpath), []byte(candidate), 0600)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to write verification code; %v", err) })
            return
        }

        dumpJsonResponse(w, http.StatusAccepted, map[string]string{ "status": "PENDING", "value": candidate })
        return
    }
}

func newRegisterFinishHandler(db *sql.DB, scratch string, tokenizer *unicodeTokenizer) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        encpath := strings.TrimPrefix(r.URL.Path, "/register/finish/")
        regpath, err := validatePath(encpath)
        if err != nil {
            dumpJsonResponse(w, http.StatusBadRequest, map[string]string{ "status": "ERROR", "reason": err.Error() })
            return
        }

        reencpath := url.QueryEscape(regpath) // re-encoding it to guarantee that there isn't any weirdness.
        expected_raw, err := os.ReadFile(filepath.Join(scratch, reencpath))
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to retrieve verification code; %v", err) })
            return
        }

        expected_path := filepath.Join(regpath, string(expected_raw))
        expected_info, err := os.Stat(expected_path)
        if errors.Is(err, os.ErrNotExist) {
            dumpJsonResponse(w, http.StatusUnauthorized, map[string]string{ "status": "ERROR", "reason": "failed to detect verification code in requested directory" })
            return
        }

        username, err := identifyUser(expected_info)
        if err != nil {
            dumpJsonResponse(w, http.StatusUnauthorized, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to identify the file author; %v", err) })
            return
        }

        err = addDirectory(db, r.Context(), regpath, username, tokenizer)
        if err != nil {
            dumpJsonResponse(w, http.StatusInternalServerError, map[string]string{ "status": "ERROR", "reason": fmt.Sprintf("failed to index directory; %v", err) })
            return
        }

        dumpJsonResponse(w, http.StatusOK, map[string]string{ "status": "SUCCESS" })
        return
    }
}

/**********************************************************************/

func main() {
    dbpath0 := flag.String("db", "", "Path to the SQLite file for the metadata")
    scratch0 := flag.String("scratch", "", "Path to a scratch directory")
    port0 := flag.String("port", "", "Port to listen to for requests")
    flag.Parse()

    dbpath := *dbpath0
    port := *port0
    scratch := *scratch0
    if dbpath == "" || port == "" || scratch == "" {
        flag.Usage()
        os.Exit(1)
    }

    err := os.MkdirAll(scratch, 700)
    if err != nil {
        log.Fatalf("failed to create the scratch directory at %q; %w", scratch, err)
    }

    db, err := initializeDatabase(dbpath)
    if err != nil {
        log.Fatalf("failed to create the initial SQLite file at %q; %w", db, err)
    }
    defer db.Close()

    http.HandleFunc("POST /register/start/", newRegisterStartHandler(scratch))

    tokenizer, err := newUnicodeTokenizer(false)
    if err != nil {
        log.Fatalf("failed to create the default tokenizer; %w", db, err)
    }
    http.HandleFunc("POST /register/finish/", newRegisterFinishHandler(db, scratch, tokenizer))

    log.Fatal(http.ListenAndServe(":" + port, nil))
}
