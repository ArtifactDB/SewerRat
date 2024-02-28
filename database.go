package main

import (
    "os"
    "fmt"
    "context"
    "time"
    "sync"
    "encoding/json"
    "path/filepath"
    "io/fs"
    "database/sql"
    _ "modernc.org/sqlite"
)

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

// Pre-building the insertion statements for efficiency when iterating over and
// within metadata documents.  Note that we need to do this for each
// transaction so it's not worth doing for the per-directory inserts.
type insertStatements struct {
    Path *sql.Stmt
    Token *sql.Stmt
    Field *sql.Stmt
    Link *sql.Stmt
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
        Path: p,
        Token: t,
        Field: f,
        Link: l,
    }, nil
}

func (i *insertStatements) Close() error {
    err := i.Path.Close()
    if err != nil {
        return fmt.Errorf("failed to close path insertion statement; %w", err)
    }

    err = i.Token.Close()
    if err != nil {
        return fmt.Errorf("failed to close token insertion statement; %w", err)
    }

    err = i.Field.Close()
    if err != nil {
        return fmt.Errorf("failed to close field insertion statement; %w", err)
    }

    err = i.Link.Close()
    if err != nil {
        return fmt.Errorf("failed to close link insertion statement; %w", err)
    }

    return nil
}

/**********************************************************************/

func addDirectory(db *sql.DB, ctx context.Context, directory string, user string, tokenizer *unicodeTokenizer) ([]string, error) {
    all_failures := []string{}

    gathered := []string{}
    filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
        // Just skip any directories that we can't access.
        if err != nil {
            all_failures = append(all_failures, fmt.Sprintf("failed to walk %q; %v", path, err))
        } else if !d.IsDir() && filepath.Base(d.Name()) == "_metadata.json" {
            gathered = append(gathered, d.Name())
        }
        return nil
    })

    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare a database transaction; %w", err)
    }
    defer tx.Rollback()

    // Removing the current directory to propagate deletions, and then adding it again.
    _, err = tx.Exec("DELETE FROM directories WHERE directory = ?", directory)
    if err != nil {
        return nil, fmt.Errorf("failed to delete existing entry for %q; %w", directory, err)
    }

    var did int
    err = tx.QueryRow("INSERT INTO directories(directory, user, timestamp) VALUES(?, ?, ?) RETURNING vid", directory, user, time.Now().Unix()).Scan(&did)
    if err != nil {
        return nil, fmt.Errorf("failed to insert new entry for %q; %w", directory, err)
    }

    prepped, err := newInsertStatements(tx)
    if err != nil {
        return nil, fmt.Errorf("failed to create prepared insertion statements for %q; %w", directory, err)
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
            all_failures = append(all_failures, failures[i].Error())
            continue
        }

        var pid int
        err := prepped.Path.QueryRow(did, f, payload[i]).Scan(&pid)
        if err != nil {
            all_failures = append(all_failures, fmt.Sprintf("failed to insert %q into the database; %v", f, err))
            continue
        }

        tokenizeMetadata(tx, contents[i], gathered[i], pid, "", prepped, tokenizer, all_failures)
    }

    err = tx.Commit()
    if err != nil {
        return nil, fmt.Errorf("failed to commit the transaction for %q; %w", directory, err)
    }

    return all_failures, nil
}

// Recurse through the metadata structure to disassemble the tokens.
func tokenizeMetadata(tx *sql.Tx, contents interface{}, path string, pid int, field string, prepped *insertStatements, tokenizer *unicodeTokenizer, failures []string) {
    switch v := contents.(type) {
    case []interface{}:
        for _, w := range v {
            tokenizeMetadata(tx, w, path, pid, field, prepped, tokenizer, failures)
        }

    case map[string]interface{}:
        for k, w := range v {
            new_field := k
            if field != "" {
                new_field = field + "." + k
            }
            tokenizeMetadata(tx, w, path, pid, new_field, prepped, tokenizer, failures)
        }

    case string:
        tokens, err := tokenizer.Tokenize(v)
        if err != nil {
            failures = append(failures, fmt.Sprintf("failed to tokenize %q in %q; %v", v, path, err))
            return
        }

        for _, t := range tokens {
            _, err := prepped.Token.Exec(t)
            if err != nil {
                failures = append(failures, fmt.Sprintf("failed to insert token %q from %q; %v", t, path, err))
                continue
            }

            _, err = prepped.Field.Exec(field)
            if err != nil {
                failures = append(failures, fmt.Sprintf("failed to insert field %q from %q; %v", field, path, err))
                continue
            }

            _, err = prepped.Link.Exec(pid, field, t)
            if err != nil {
                failures = append(failures, fmt.Sprintf("failed to insert link for field %q to token %q from %q; %v", field, t, path, err))
                continue
            }
        }
    }
}

/**********************************************************************/

func deleteDirectory(db *sql.DB, directory string) error {
    // Removing the current directory to propagate deletions, and then adding it again.
    _, err := db.Exec("DELETE FROM directories WHERE directory = ?", directory)
    if err != nil {
        return fmt.Errorf("failed to delete existing entries for %q; %w", directory, err)
    }
    return nil
}
