package main

import (
    "os"
    "fmt"
    "time"
    "sync"
    "errors"
    "encoding/json"
    "path/filepath"
    "io/fs"
    "database/sql"
    "strconv"
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
CREATE TABLE paths(pid INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE, user TEXT NOT NULL, time INTEGER NOT NULL, metadata BLOB)
CREATE INDEX index_paths_path ON paths(path)
CREATE INDEX index_paths_time ON paths(time, user)
CREATE INDEX index_paths_user ON paths(user, time)

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
    Token *sql.Stmt
    Field *sql.Stmt
    Link *sql.Stmt
}

func newInsertStatements(tx *sql.Tx) (*insertStatements, error) {
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

    return &insertStatements { Token: t, Field: f, Link: l }, nil
}

func (i *insertStatements) Close() error {
    err := i.Token.Close()
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

func addDirectory(db *sql.DB, directory string, tokenizer *unicodeTokenizer) ([]string, error) {
    all_failures := []string{}

    paths := []string{}
    filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
        // Just skip any directories that we can't access.
        if err != nil {
            all_failures = append(all_failures, fmt.Sprintf("failed to walk %q; %v", path, err))
        } else if !d.IsDir() && filepath.Base(d.Name()) == "_metadata.json" {
            paths = append(paths, d.Name())
        }
        return nil
    })

    // Parsing documents in parallel.
    contents := make([]interface{}, len(paths))
    users := make([]string, len(paths))
    times := make([]time.Time, len(paths))
    payload := make([][]byte, len(paths))
    failures := make([]error, len(paths))

    {
        var wg sync.WaitGroup
        wg.Add(len(paths))

        for i, f := range paths {
            go func(i int, f string) {
                defer wg.Done()

                raw, err := os.ReadFile(f)
                if err != nil {
                    failures[i] = fmt.Errorf("failed to read %q; %w", f, err)
                    return
                }

                var vals interface{}
                err = json.Unmarshal(raw, &vals)
                if err != nil {
                    failures[i] = fmt.Errorf("failed to parse %q; %w", f, err)
                    return
                }

                info, err := os.Stat(f)
                if err != nil {
                    failures[i] = fmt.Errorf("failed to stat %q; %w", f, err)
                    return
                }

                username, err := identifyUser(info)
                if err != nil {
                    failures[i] = fmt.Errorf("failed to determine author of %q; %w", f, err)
                }

                users[i] = username
                times[i] = info.ModTime()
                payload[i] = raw
                contents[i] = vals
            }(i, f)
        }

        wg.Wait()
    }

    {
        tx, err := db.Begin()
        if err != nil {
            return nil, fmt.Errorf("failed to prepare a database transaction; %w", err)
        }
        defer tx.Rollback()

        // Delete all previously registered paths with the directory's prefix for a fresh start;
        // otherwise there's no way to easily get rid of old paths that are no longer here.
        err = deleteDirectory(db, directory)
        if err != nil {
            return nil, fmt.Errorf("failed to delete existing records for %q; %w", directory, err)
        }

        prepped, err := newInsertStatements(tx)
        if err != nil {
            return nil, fmt.Errorf("failed to create prepared insertion statements for %q; %w", directory, err)
        }
        defer prepped.Close()

        // Adding each document to the pile. We do this in serial because I don't think transactions are thread-safe.
        pstmt, err := tx.Prepare("INSERT INTO paths(path, user, time, metadata) VALUES(?, ?, ?, ?) RETURNING pid")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path insertion statement; %w", err)
        }
        defer pstmt.Close()

        for i, f := range contents {
            if failures[i] != nil {
                all_failures = append(all_failures, failures[i].Error())
                continue
            }

            var pid int64
            err := pstmt.QueryRow(f, users[i], times[i].Unix(), payload[i]).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to insert %q into the database; %v", f, err))
                continue
            }

            tokenizeMetadata(tx, contents[i], paths[i], pid, "", prepped, tokenizer, all_failures)
        }

        err = tx.Commit()
        if err != nil {
            return nil, fmt.Errorf("failed to commit the transaction for %q; %w", directory, err)
        }
    }

    return all_failures, nil
}

// Recurse through the metadata structure to disassemble the tokens.
func tokenizeMetadata(tx *sql.Tx, contents interface{}, path string, pid int64, field string, prepped *insertStatements, tokenizer *unicodeTokenizer, failures []string) {
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
    // Trimming the suffix.
    if len(directory) > 0 {
        counter := len(directory) - 1
        for counter >= 0 && directory[counter] == '/' {
            counter--
        }
        directory = directory[:counter]
    }

    pattern, escape, err := escapeWildcards(directory)
    if err != nil {
        return fmt.Errorf("failed to query database for paths to delete; %w", err)
    }

    query := "DELETE FROM paths WHERE path LIKE ?"
    if escape != "" {
        query += " ESCAPE '"
        if escape == "\\" {
            query += "\\" // need to escape the escape.
        } 
        query += escape + "'"
    }

    _, err = db.Exec(query, pattern + "/%")
    if err != nil {
        return fmt.Errorf("failed to delete existing entries for %q; %w", directory, err)
    }
    return nil
}

/**********************************************************************/

func updatePaths(db *sql.DB, tokenizer* unicodeTokenizer) ([]string, error) {
    update_paths := []string{}
    update_users := []string{}
    update_times := []int64{}
    purge_paths := []string{}

    // Wrapping this inside a function so that the 'rows' are closed before further operations.
    err := func() error {
        rows, err := db.Query("SELECT path, time from paths") 
        if err != nil {
            return fmt.Errorf("failed to query the 'paths' table; %w", err)
        }
        defer rows.Close()

        for rows.Next() {
            var path string
            var time int64
            if err := rows.Scan(&path, &time); err != nil {
                return fmt.Errorf("failed to traverse rows of the 'paths' table; %w", err)
            }

            info, err := os.Stat(path)
            if err != nil {
                purge_paths = append(purge_paths, path)
                continue
            }

            if info.ModTime().Unix() != time {
                username, err := identifyUser(info)
                if err != nil {
                    purge_paths = append(purge_paths, path)
                } else {
                    update_paths = append(update_paths, path)
                    update_users = append(update_users, username)
                    update_times = append(update_times, time)
                }
            }
        }

        return nil
    }()
    if err != nil {
        return nil, err
    }

    update_contents := make([]interface{}, len(update_paths))
    update_payloads := make([][]byte, len(update_paths))
    update_failures := make([]error, len(update_paths))
    {
        var wg sync.WaitGroup
        wg.Add(len(update_paths))

        for i, f := range update_paths {
            go func(i int, f string) {
                defer wg.Done()

                raw, err := os.ReadFile(f)
                if err != nil {
                    update_failures[i] = fmt.Errorf("failed to read %q; %w", f, err)
                    return
                }

                var vals interface{}
                err = json.Unmarshal(raw, &vals)
                if err != nil {
                    update_failures[i] = fmt.Errorf("failed to parse %q; %w", f, err)
                    return
                }

                update_payloads[i] = raw
                update_contents[i] = vals
            }(i, f)
        }

        wg.Wait()
    }

    all_failures := []string{}
    {
        tx, err := db.Begin()
        if err != nil {
            return nil, fmt.Errorf("failed to prepare a database transaction; %w", err)
        }
        defer tx.Rollback()

        // Removing all the out-dated paths.
        delstmt, err := tx.Prepare("DELETE FROM paths WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare the delete transaction; %w", err)
        }
        defer delstmt.Close()

        for _, x := range purge_paths {
            _, err := delstmt.Exec("DELETE FROM paths WHERE path = ?", x)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to purge path %q from the database; %v", x, err))
            }
        }

        // Updating the existing files.
        pustmt, err := tx.Prepare("UPDATE paths SET user = ?, time = ?, metadata = ? WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path update statement; %w", err)
        }
        defer pustmt.Close()

        pistmt, err := tx.Prepare("SELECT pid FROM paths WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path ID statement; %w", err)
        }
        defer pistmt.Close()

        prepped, err := newInsertStatements(tx)
        if err != nil {
            return nil, fmt.Errorf("failed to prepare token insertion statements for the update; %w", err)
        }
        defer prepped.Close()

        for i, f := range update_paths {
            if update_failures[i] != nil {
                all_failures = append(all_failures, update_failures[i].Error())
                continue
            }

            _, err := pustmt.Exec(update_users[i], update_times[i], update_payloads[i], f)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to update %q in the database; %v", f, err))
                continue
            }

            var pid int64
            err = pistmt.QueryRow(f).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to inspect path ID for %q; %v", f, err))
                continue
            }

            tokenizeMetadata(tx, update_contents[i], f, pid, "", prepped, tokenizer, all_failures)
        }

        err = tx.Commit()
        if err != nil {
            return nil, fmt.Errorf("failed to commit the update transaction; %w", err)
        }
    }

    return all_failures, nil
}

/**********************************************************************/

func backupDatabase(db *sql.DB, path string) error {
    var existing bool
    _, err := os.Stat(path)
    if err == nil {
        existing = true
        err = os.Rename(path, path + ".backup")
        if err != nil {
            return fmt.Errorf("failed to move the backup database; %w", err) 
        }
    } else if errors.Is(err, os.ErrNotExist) {
        existing = false 
    } else {
        return fmt.Errorf("failed to inspect the backup database; %w", err) 
    }

    _, err = db.Exec("VACUUM INTO ?", path)
    if err != nil {
        all_errors := []error{ fmt.Errorf("failed to create a backup database; %w", path, err) }
        if existing {
            // Move the backup of the backup back to its previous location.
            err = os.Rename(path + ".backup", path)
            if err != nil {
                all_errors = append(all_errors, fmt.Errorf("failed to restore the old backup database; %w", err))
            }
        }
        return errors.Join(all_errors...)
    }

    if existing {
        err := os.Remove(path + ".backup")
        if err != nil {
            return fmt.Errorf("failed to remove the backup of the backup database; %w", err) 
        }
    }

    return nil
}

/**********************************************************************/

type queryResult struct {
    Pid int64 `json:"-"`
    Path string `json:"path"`
    User string `json:"user"`
    Time int64 `json:"time"`
    Metadata json.RawMessage `json:"metadata"`
}

type scrollPosition struct {
    Time int64
    Pid int64
}

func queryTokens(db * sql.DB, query *searchClause, scroll *scrollPosition, page_limit int) ([]queryResult, error) {
    full := "SELECT paths.pid, paths.path, paths.user, paths.time, json_extract(paths.metadata, '$') FROM paths"

    // The query can be nil.
    parameters := []interface{}{}
    query_present := false
    if query != nil {
        query_present = true
        filter := assembleFilter(query, parameters)
        full += " WHERE " + filter 
    }

    // Handling pagination via scrolling window queries, see https://www.sqlite.org/rowvalue.html#scrolling_window_queries.
    // This should be pretty efficient as we have an index on 'time'.
    if scroll != nil {
        if query_present {
            full += " AND"
        } else {
            full += " WHERE"
        }
        full += " (paths.time, paths.pid) < (?, ?)"
        parameters = append(parameters, scroll.Time)
        parameters = append(parameters, scroll.Pid)
    }
    full += " ORDER BY paths.time, paths.pid DESC"
    if page_limit > 0 {
        full += " LIMIT " + strconv.Itoa(page_limit)
    }

    rows, err := db.Query(full, parameters...)
    if err != nil {
        return nil, fmt.Errorf("failed to perform query; %w", err)
    }
    defer rows.Close()

    output := []queryResult{}
    for rows.Next() {
        var pid int64
        var path string
        var user string
        var time int64
        var metadata string
        err = rows.Scan(&pid, &path, &user, &time, &metadata)
        if err != nil {
            return nil, fmt.Errorf("failed to extract row; %w", err)
        }
        output = append(output, queryResult{ Pid: pid, Path: path, User: user, Time: time, Metadata: []byte(metadata) })
    }

    return output, nil
}
