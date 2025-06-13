package main

import (
    "os"
    "fmt"
    "time"
    "log"
    "sync"
    "errors"
    "strings"
    "encoding/json"
    "path/filepath"
    "io/fs"
    "database/sql"
    "strconv"
    "context"
    _ "modernc.org/sqlite"
)

type writeTransaction struct {
    Conn *sql.Conn
    Committed bool
    Ctx context.Context
}

func rollbackOrLog(conn *sql.Conn, ctx context.Context) {
    _, err := conn.ExecContext(ctx, "ROLLBACK TRANSACTION")
    if err != nil {
        log.Printf("failed to rollback a transaction; %v\n", err)
    }
}

func closeOrLog(conn *sql.Conn) {
    err := conn.Close()
    if err != nil {
        log.Printf("failed to close the connection; %v\n", err)
    }
}

func (wt *writeTransaction) Finish() {
    if !wt.Committed {
        rollbackOrLog(wt.Conn, wt.Ctx)
    }
    closeOrLog(wt.Conn)
}

func (wt *writeTransaction) Commit() error {
    _, err := wt.Conn.ExecContext(wt.Ctx, "COMMIT TRANSACTION")
    if err != nil {
        return err
    }
    wt.Committed = true
    return nil
}

func (wt *writeTransaction) Exec(query string, args ...any) (sql.Result, error) {
    res, err := wt.Conn.ExecContext(wt.Ctx, query, args...)
    return res, err
}

func (wt *writeTransaction) Prepare(query string) (*sql.Stmt, error) {
    stmt, err := wt.Conn.PrepareContext(wt.Ctx, query)
    return stmt, err
}

func (wt * writeTransaction) Query(query string, args ...any) (*sql.Rows, error) {
    rows, err := wt.Conn.QueryContext(wt.Ctx, query, args...)
    return rows, err
}

func (wt * writeTransaction) QueryRow(query string, args ...any) *sql.Row {
    return wt.Conn.QueryRowContext(wt.Ctx, query, args...)
}

func createWriteTransaction(db *sql.DB, ctx context.Context) (*writeTransaction, error) {
    success := false

    // We acquire a connection to run all of the pragmas. We don't know whether
    // this is an existing connection or if it's generated anew, as
    // database/sql manages the pool itself; so we have to run the pragmas
    // every time, just in case. I wish we had some connection hooks.
    conn, err := db.Conn(ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to acquire connection; %w", err)
    }
    defer func() {
        if !success {
            closeOrLog(conn)
        }
    }()

    // Foreign key set-up must be done outside of the transaction,
    // see https://sqlite.org/pragma.html#pragma_foreign_keys.
    _, err = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON")
    if err != nil {
        return nil, fmt.Errorf("failed to enable foreign key support; %w", err)
    }

    // Setting a busy timeout for write operations to avoid the SQLITE_BUSY error.
    _, err = conn.ExecContext(ctx, "PRAGMA busy_timeout = 10000")
    if err != nil {
        return nil, fmt.Errorf("failed to enable busy timeout; %w", err)
    }

    // Improve performance in WAL journalling mode. This is not persistent and needs
    // to be changed outside of a transaction and involves a lock, so keep it here,
    // i.e., after the timeout setting but before the transaction start.
    _, err = conn.ExecContext(ctx, "PRAGMA synchronous = NORMAL")
    if err != nil {
        return nil, fmt.Errorf("failed to enable normal synchronous mode; %w", err)
    }

    // We set IMMEDIATE transactions to make debugging of locking issues
    // easier. This should not be a perf problem as 'db' should only have one
    // connection anyway, we just eliminate the possibility of deadlocks.
    _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
    if err != nil {
        return nil, fmt.Errorf("failed to create transaction; %w", err)
    }
    defer func() {
        if !success {
            rollbackOrLog(conn, ctx)
        }
    }()

    success = true
    return &writeTransaction{ Conn: conn, Committed: false, Ctx: ctx }, nil
}

func createTableStatement() string {
    return `
CREATE TABLE dirs(
    did INTEGER PRIMARY KEY, 
    path TEXT NOT NULL UNIQUE, 
    user TEXT NOT NULL, 
    time INTEGER NOT NULL,
    names BLOB
);

CREATE TABLE paths(
    pid INTEGER PRIMARY KEY, 
    did INTEGER NOT NULL,
    path TEXT NOT NULL UNIQUE, 
    user TEXT NOT NULL, 
    time INTEGER NOT NULL, 
    metadata BLOB,
    FOREIGN KEY(did) REFERENCES dirs(did) ON DELETE CASCADE
);

CREATE TABLE tokens(
    tid INTEGER PRIMARY KEY,
    token TEXT NOT NULL UNIQUE
);

CREATE TABLE fields(
    fid INTEGER PRIMARY KEY,
    field TEXT NOT NULL UNIQUE
);

CREATE TABLE links(
    pid INTEGER NOT NULL,
    fid INTEGER NOT NULL,
    tid INTEGER NOT NULL,
    FOREIGN KEY(pid) REFERENCES paths(pid) ON DELETE CASCADE,
    UNIQUE(pid, fid, tid)
);
`
}

func createIndexStatement() string {
    // Here, the general idea is to spam indices so that the query planner has many optimization options.
    // - For 'dirs', we index on everything, but we don't stop each multi-column indices at the first column that only contains unique values.
    //   There's no point in including more columns if there will never be tied values.
    //   (Time is effectively unique at the resolutions we're using.)
    // - For 'paths', the logic is the same as 'dirs'.
    //   We throw in an index on 'did' to enable efficient cascading deletion.
    // - For 'tokens' and 'fields', there is only one column worth indexing, so we just do that.
    // - For 'links', we try both permutations of 'fid' and 'tid', which are (via 'fields' and 'tokens') what we want to search on.
    //   A single 'pid' index created to enable efficient cascading deletion.
    //   I don't think the 'pid' index would otherwise be used because we don't use it in the subqueries and those subqueries can't be flattened into inner joins anyway.
    return `
CREATE INDEX index_dirs_path ON dirs(path);
CREATE INDEX index_dirs_time ON dirs(time);
CREATE INDEX index_dirs_user_time ON dirs(user, time);
CREATE INDEX index_dirs_user_path ON dirs(user, path);

CREATE INDEX index_paths_path ON paths(path);
CREATE INDEX index_paths_time ON paths(time);
CREATE INDEX index_paths_user_time ON paths(user, time);
CREATE INDEX index_paths_user_path ON paths(user, path);
CREATE INDEX index_paths_did ON paths(did);

CREATE INDEX index_tokens ON tokens(token);

CREATE INDEX index_fields ON fields(field);

CREATE INDEX index_links_tid_fid ON links(tid, fid);
CREATE INDEX index_links_fid_tid ON links(fid, tid);
CREATE INDEX index_links_pid ON links(pid);
`
}

func initializeDatabase(path string) (*sql.DB, error) {
    accessible := false
    if _, err := os.Lstat(path); err == nil {
        accessible = true
    }

    db, err := sql.Open("sqlite", path) 
    if err != nil {
        return nil, fmt.Errorf("failed to open read/write SQLite handle; %w", err)
    }

    // Maxing out at one connection so that there can only be one write at any
    // time; everyone else will have to block on the connection availability.
    db.SetMaxOpenConns(1)

    // Write-ahead logging is persistent and doesn't need to be set on every connection,
    // see https://www.sqlite.org/wal.html#persistence_of_wal_mode.
    _, err = db.Exec("PRAGMA journal_mode = WAL")
    if err != nil {
        return nil, fmt.Errorf("failed to enable write-ahead logging; %w", err)
    }

    latest_version := 1

    if (!accessible) {
        err := func () error {
            atx, err := createWriteTransaction(db, context.Background())
            if err != nil {
                return fmt.Errorf("failed to prepare transaction for table setup; %w", err)
            }
            defer atx.Finish()

            _, err = atx.Exec(createTableStatement())
            if err != nil {
                return fmt.Errorf("failed to create tables in %q; %w", path, err)
            }

            _, err = atx.Exec(createIndexStatement())
            if err != nil {
                return fmt.Errorf("failed to create indices in %q; %w", path, err)
            }

            _, err = atx.Exec("PRAGMA user_version = " + strconv.Itoa(latest_version))
            if err != nil {
                return fmt.Errorf("failed to set the latest version in %q; %w", path, err)
            }

            err = atx.Commit()
            if err != nil {
                return fmt.Errorf("failed to commit table creation commands for %s; %w", path, err)
            }

            return nil
        }()

        if err != nil {
            os.Remove(path)
            return nil, fmt.Errorf("failed to create table in %q; %w", path, err)
        }

    } else {
        err := func () error {
            atx, err := createWriteTransaction(db, context.Background())
            if err != nil {
                return fmt.Errorf("failed to prepare transaction for table setup; %w", err)
            }
            defer atx.Finish()

            res := atx.QueryRow("PRAGMA user_version")
            var version int
            err = res.Scan(&version)
            if err != nil {
                if !errors.Is(err, sql.ErrNoRows) {
                    return fmt.Errorf("failed to extract version; %w", err)
                }
                version = 0
            }

            if version < latest_version {
                // Purging all existing (non-automatic) indices.
                res, err := atx.Query("SELECT name FROM sqlite_master WHERE type == 'index' AND name NOT GLOB 'sqlite_autoindex_*'")
                if err != nil {
                    return fmt.Errorf("failed to identify all indices in the database; %w", err)
                }

                all_indices := []string{}
                for res.Next() {
                    var index_name string
                    err := res.Scan(&index_name)
                    if err != nil {
                        return fmt.Errorf("failed to extract name of an index in the database; %w", err)
                    }
                    all_indices = append(all_indices, index_name)
                }

                for _, index_name := range all_indices {
                    _, err = atx.Exec("DROP INDEX " + index_name)
                    if err != nil {
                        return fmt.Errorf("failed to remove index %q from the database; %w", index_name, err)
                    }
                }

                // Adding the new indices.
                _, err = atx.Exec(createIndexStatement())
                if err != nil {
                    return fmt.Errorf("failed to update indices in %q; %w", path, err)
                }

                _, err = atx.Exec("PRAGMA user_version = " + strconv.Itoa(latest_version))
                if err != nil {
                    return fmt.Errorf("failed to set the latest version in %q; %w", path, err)
                }

                err = atx.Commit()
                if err != nil {
                    return fmt.Errorf("failed to commit update commands for %s; %w", path, err)
                }
            }

            return nil
        }()

        if err != nil {
            return nil, fmt.Errorf("failed to update the database in %q; %w", path, err)
        }
    }

    return db, nil
}

func initializeReadOnlyDatabase(path string) (*sql.DB, error) {
    ro_db, err := sql.Open("sqlite", path)
    if err != nil {
        return nil, fmt.Errorf("failed to open SQLite handle; %w", err)
    }

    _, err = ro_db.Exec("PRAGMA query_only=1")
    if err != nil {
        return nil, fmt.Errorf("failed to set SQLite handle as read-only; %w", err)
    }

    return ro_db, nil
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

func (s *insertStatements) Close() {
    if s.Token != nil {
        s.Token.Close()
    }
    if s.Field != nil {
        s.Field.Close()
    }
    if s.Link != nil {
        s.Link.Close()
    }
}

func newInsertStatements(tx *writeTransaction) (*insertStatements, error) {
    output := &insertStatements{}
    success := false
    defer func() {
        if !success {
            output.Close()
        }
    }()

    t, err := tx.Prepare("INSERT OR IGNORE INTO tokens(token) VALUES(?)")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare token insertion statement; %w", err)
    }
    output.Token = t

    f, err := tx.Prepare("INSERT OR IGNORE INTO fields(field) VALUES(?)")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare field insertion statement; %w", err)
    }
    output.Field = f

    l, err := tx.Prepare("INSERT OR IGNORE INTO links(pid, fid, tid) VALUES(?, (SELECT fid FROM fields WHERE field = ?), (SELECT tid FROM tokens WHERE token = ?))")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare link insertion statement; %w", err)
    }
    output.Link = l

    success = true;
    return output, nil
}

/**********************************************************************/

func processToken(path string, pid int64, field, token string, prepped *insertStatements, ctx context.Context) error {
    _, err := prepped.Token.ExecContext(ctx, token)
    if err != nil {
        return fmt.Errorf("failed to insert token %q from %q; %w", token, path, err)
    }

    _, err = prepped.Field.ExecContext(ctx, field)
    if err != nil {
        return fmt.Errorf("failed to insert field %q from %q; %w", field, path, err)
    }

    _, err = prepped.Link.ExecContext(ctx, pid, field, token)
    if err != nil {
        return fmt.Errorf("failed to insert link for field %q to token %q from %q; %w", field, token, path, err)
    }

    return nil
}

// Recurse through the metadata structure to disassemble the tokens.
func tokenizeMetadata(parsed interface{}, path string, pid int64, field string, tokenizer *unicodeTokenizer, prepped *insertStatements, ctx context.Context) []string {
    failures := []string{}

    switch v := parsed.(type) {
    case []interface{}:
        for _, w := range v {
            tokfails := tokenizeMetadata(w, path, pid, field, tokenizer, prepped, ctx)
            failures = append(failures, tokfails...)
        }

    case map[string]interface{}:
        for k, w := range v {
            new_field := k
            if field != "" {
                new_field = field + "." + k
            }
            tokfails := tokenizeMetadata(w, path, pid, new_field, tokenizer, prepped, ctx)
            failures = append(failures, tokfails...)
        }

    case json.Number: 
        // Just treat this as a string for simplicity. This should be fine for integers,
        // but it does result in somewhat unnecessary tokenization for floating-point
        // numbers. There's no real way around it, though, as the queries are always
        // tokenized, so you wouldn't be able to find an exact match anyway.
        tokens, err := tokenizer.Tokenize(string(v))
        if err != nil {
            return []string{ fmt.Sprintf("failed to tokenize %q in %q; %v", v, path, err) }
        }

        for _, t := range tokens {
            _, err := prepped.Token.ExecContext(ctx, t)
            if err != nil {
                failures = append(failures, fmt.Sprintf("failed to insert token %q from %q; %v", t, path, err))
                continue
            }

            _, err = prepped.Field.ExecContext(ctx, field)
            if err != nil {
                failures = append(failures, fmt.Sprintf("failed to insert field %q from %q; %v", field, path, err))
                continue
            }

            _, err = prepped.Link.ExecContext(ctx, pid, field, t)
            if err != nil {
                failures = append(failures, fmt.Sprintf("failed to insert link for field %q to token %q from %q; %v", field, t, path, err))
                continue
            }
        }

    case string:
        tokens, err := tokenizer.Tokenize(v)
        if err != nil {
            return []string{ fmt.Sprintf("failed to tokenize %q in %q; %v", v, path, err) }
        }

        for _, t := range tokens {
            err := processToken(path, pid, field, t, prepped, ctx)
            if err != nil {
                failures = append(failures, err.Error())
                continue
            }
        }

    case bool:
        var t string
        if v {
            t = "true"
        } else {
            t = "false"
        }

        _, err := prepped.Token.ExecContext(ctx, t)
        if err != nil {
            failures = append(failures, fmt.Sprintf("failed to insert token %q from %q; %v", t, path, err))
            break
        }

        _, err = prepped.Field.ExecContext(ctx, field)
        if err != nil {
            failures = append(failures, fmt.Sprintf("failed to insert field %q from %q; %v", field, path, err))
            break
        }

        _, err = prepped.Link.ExecContext(ctx, pid, field, t)
        if err != nil {
            failures = append(failures, fmt.Sprintf("failed to insert link for field %q to token %q from %q; %v", field, t, path, err))
            break
        }
    }

    return failures
}

func tokenizePath(path string, pid int64, field string, tokenizer *unicodeTokenizer, prepped *insertStatements, ctx context.Context) []string {
    tokens, err := tokenizer.Tokenize(path)
    if err != nil {
        return []string{ fmt.Sprintf("failed to tokenize path %q; %v", path, err) }
    }

    failures := []string{}
    for _, t := range tokens {
        err := processToken(path, pid, field, t, prepped, ctx)
        if err != nil {
            failures = append(failures, err.Error())
            continue
        }
    }

    return failures
}

/**********************************************************************/

func deleteDirectory(directory string, db *sql.DB, ctx context.Context) error {
    atx, err := createWriteTransaction(db, ctx)
    if err != nil {
        return fmt.Errorf("failed to prepare transaction for deletion; %w", err)
    }
    defer atx.Finish()

    _, err = atx.Exec("DELETE FROM dirs WHERE path == ?", directory)
    if err != nil {
        return fmt.Errorf("failed to delete %q; %w", directory, err)
    }

    err = atx.Commit()
    if err != nil {
        return fmt.Errorf("failed to commit deletion transaction for %q; %w", directory, err)
    }

    return nil
}

/**********************************************************************/

type FileInfoWithPath struct {
    Path string
    Info fs.FileInfo
}

func compareToExistingPaths(did int64, all_paths map[string]fs.FileInfo, tx *writeTransaction) ([]*FileInfoWithPath, []*FileInfoWithPath, []string, error) {
    rows, err := tx.Query("SELECT path, time from paths WHERE did = ?", did) 
    if err != nil {
        return nil, nil, nil, fmt.Errorf("failed to query the 'paths' table; %w", err)
    }
    defer rows.Close()

    update_paths := []*FileInfoWithPath{}
    found := map[string]bool{}
    purge_paths := []string{}

    for rows.Next() {
        var path string
        var time int64
        if err := rows.Scan(&path, &time); err != nil {
            return nil, nil, nil, fmt.Errorf("failed to traverse rows of the 'paths' table; %w", err)
        }

        if err := tx.Ctx.Err(); err != nil {
            return nil, nil, nil, fmt.Errorf("request canceled while comparing paths; %w", err)
        }

        candidate, ok := all_paths[path]
        if !ok {
            purge_paths = append(purge_paths, path)
            continue
        }
        found[path] = true

        newtime := candidate.ModTime().Unix()
        if newtime == time {
            continue
        }

        update_paths = append(update_paths, &FileInfoWithPath{ Path: path, Info: candidate })
    }

    new_paths := []*FileInfoWithPath{}
    for k, v:= range all_paths {
        if _, ok := found[k]; !ok {
            new_paths = append(new_paths, &FileInfoWithPath{ Path: k, Info: v })
        }
    }

    return new_paths, update_paths, purge_paths, nil
}

type addDirectoryContentsOptions struct {
    Concurrency int
    PathField string
    LinkWhitelist linkWhitelist
}

func addDirectoryContents(path string, did int64, base_names []string, tokenizer* unicodeTokenizer, tx *writeTransaction, options *addDirectoryContentsOptions) ([]string, error) {
    all_failures := []string{}

    dir_contents, dir_failures := listMetadata(path, base_names, options.LinkWhitelist, tx.Ctx)
    all_failures = append(all_failures, dir_failures...)

    new_paths, update_paths, purge_paths, err := compareToExistingPaths(did, dir_contents, tx)
    if err != nil {
        return nil, fmt.Errorf("path comparisons failed for %q; %w", path, err)
    }

    // Loading the metadata into memory; we use a thread pool to avoid opening too many file handles at once.
    new_assets := make([]*loadedMetadata, len(new_paths))
    update_assets := make([]*loadedMetadata, len(update_paths))
    {
        var wg sync.WaitGroup
        wg.Add(options.Concurrency)
        ichannel := make(chan int)
        uchannel := make(chan int)

        for t := 0; t < options.Concurrency; t++ {
            go func() {
                defer wg.Done()
                for i := range ichannel {
                    if tx.Ctx.Err() != nil {
                        continue // don't break; we make sure to empty 'ichannel' so that we don't deadlock trying to put stuff in 'ichannel' below.
                    }
                    e := new_paths[i]
                    new_assets[i] = loadMetadata(e.Path, e.Info)
                }
                for i := range uchannel {
                    if tx.Ctx.Err() != nil {
                        continue // see above.
                    }
                    e := update_paths[i]
                    update_assets[i] = loadMetadata(e.Path, e.Info)
                }
            }()
        }

        for i := 0; i < len(new_paths); i++ {
            ichannel <- i
        }
        close(ichannel)

        for i := 0; i < len(update_paths); i++ {
            uchannel <- i
        }
        close(uchannel)
        wg.Wait()
    }

    if err := tx.Ctx.Err(); err != nil {
        return nil, fmt.Errorf("request canceled while adding contents of %q; %w", path, err)
    }

    token_stmts, err := newInsertStatements(tx)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare token insertion statements for the update; %w", err)
    }
    defer token_stmts.Close()

    if len(new_assets) > 0 {
        new_stmt, err := tx.Prepare("INSERT INTO paths(path, did, user, time, metadata) VALUES(?, ?, ?, ?, ?) RETURNING pid")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path insertion statement; %w", err)
        }
        defer new_stmt.Close()

        for _, loaded := range new_assets {
            if err := tx.Ctx.Err(); err != nil {
                return nil, fmt.Errorf("request canceled while adding %q; %w", loaded.Path, err)
            }

            if loaded.Failure != nil {
                all_failures = append(all_failures, loaded.Failure.Error())
                continue
            }

            var pid int64
            err := new_stmt.QueryRowContext(tx.Ctx, loaded.Path, did, loaded.User, loaded.Time.Unix(), loaded.Raw).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to insert %q into the database; %v", loaded.Path, err))
                continue
            }

            tokfails := tokenizeMetadata(loaded.Parsed, loaded.Path, pid, "", tokenizer, token_stmts, tx.Ctx)
            all_failures = append(all_failures, tokfails...)

            if options.PathField != "" {
                tokfails = tokenizePath(loaded.Path, pid, options.PathField, tokenizer, token_stmts, tx.Ctx)
                all_failures = append(all_failures, tokfails...)
            }
        }
    }

    if len(update_assets) > 0 {
        update_stmt, err := tx.Prepare("UPDATE paths SET user = ?, time = ?, metadata = ? WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path update statement; %w", err)
        }
        defer update_stmt.Close()

        pid_stmt, err := tx.Prepare("SELECT pid FROM paths WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path ID statement; %w", err)
        }
        defer pid_stmt.Close()

        dellnk_stmt, err := tx.Prepare("DELETE FROM links WHERE pid = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare link deletion statement; %w", err)
        }
        defer dellnk_stmt.Close()

        for _, loaded := range update_assets {
            if err := tx.Ctx.Err(); err != nil {
                return nil, fmt.Errorf("request canceled while updating %q; %w", loaded.Path, err)
            }

            if loaded.Failure != nil {
                purge_paths = append(purge_paths, loaded.Path)
                all_failures = append(all_failures, loaded.Failure.Error())
                continue
            }

            var pid int64
            _, err := update_stmt.ExecContext(tx.Ctx, loaded.User, loaded.Time.Unix(), loaded.Raw, loaded.Path)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to update %q in the database; %v", loaded.Path, err))
                continue
            }

            err = pid_stmt.QueryRowContext(tx.Ctx, loaded.Path).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to inspect path ID for %q; %v", loaded.Path, err))
                continue
            }

            _, err = dellnk_stmt.ExecContext(tx.Ctx, pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to delete links for %q; %v", loaded.Path, err))
                continue
            }

            tokfails := tokenizeMetadata(loaded.Parsed, loaded.Path, pid, "", tokenizer, token_stmts, tx.Ctx)
            all_failures = append(all_failures, tokfails...)

            if options.PathField != "" {
                ptokfails := tokenizePath(loaded.Path, pid, options.PathField, tokenizer, token_stmts, tx.Ctx)
                all_failures = append(all_failures, ptokfails...)
            }
        }
    }

    if len(purge_paths) > 0 {
        del_stmt, err := tx.Prepare("DELETE FROM paths WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare the delete transaction; %w", err)
        }
        defer del_stmt.Close()

        for _, x := range purge_paths {
            if err := tx.Ctx.Err(); err != nil {
                return nil, fmt.Errorf("request canceled while deleting %q; %w", x, err)
            }

            _, err := del_stmt.ExecContext(tx.Ctx, x)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to purge path %q from the database; %v", x, err))
            }
        }
    }

    return all_failures, nil
}

/**********************************************************************/

func addNewDirectory(path string, base_names []string, user string, tokenizer* unicodeTokenizer, db *sql.DB, ctx context.Context, options *addDirectoryContentsOptions) ([]string, error) {
    b, err := json.Marshal(base_names)
    if err != nil {
        return nil, fmt.Errorf("failed to encode names as JSON; %w", err)
    }

    atx, err := createWriteTransaction(db, ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare transaction for adding a new directory; %w", err)
    }
    defer atx.Finish()

    var did int64
    row := atx.QueryRow("SELECT did FROM dirs WHERE path = ?", path)
    err = row.Scan(&did)
    if errors.Is(err, sql.ErrNoRows) {
        err = atx.QueryRow(
            "INSERT INTO dirs(path, user, time, names) VALUES(?, ?, ?, ?) RETURNING did",
            path, 
            user, 
            time.Now().Unix(), 
            b,
        ).Scan(&did)

    } else {
        _, err = atx.Exec(
            "UPDATE dirs SET user = ?, time = ?, names = ? WHERE did = ?",
            user, 
            time.Now().Unix(), 
            b,
            did,
        )
    }

    if err != nil {
        return nil, fmt.Errorf("failed to insert new directory; %w", err)
    }

    failures, err := addDirectoryContents(path, did, base_names, tokenizer, atx, options)
    if err != nil {
        return nil, err
    }

    err = atx.Commit()
    if err != nil {
        return nil, fmt.Errorf("failed to commit the transaction to add a new directory; %w", err)
    }

    return failures, err
}

/**********************************************************************/

type registeredDirectory struct {
    Path string
    Id int64
    Names []string
}

func listDirectories(tx *writeTransaction) ([]*registeredDirectory, error) {
    rows, err := tx.Query("SELECT did, path, names from dirs") 
    if err != nil {
        return nil, fmt.Errorf("failed to query the 'dirs' table; %w", err)
    }
    defer rows.Close()

    all_dirs := []*registeredDirectory{}
    for rows.Next() {
        if err := tx.Ctx.Err(); err != nil {
            return nil, fmt.Errorf("request canceled while listing directories; %w", err)
        }

        var id int64
        var path string
        var names_raw []byte
        if err := rows.Scan(&id, &path, &names_raw); err != nil {
            return nil, fmt.Errorf("failed to traverse rows of the 'dirs' table; %w", err)
        }

        var names []string
        err := json.Unmarshal(names_raw, &names)
        if err != nil {
            return nil, fmt.Errorf("failed to parse names of 'dirs' for %q; %w", path, err)
        }

        all_dirs = append(all_dirs, &registeredDirectory{ Id: id, Path: path, Names: names })
    }

    return all_dirs, nil
}

func updateDirectories(tokenizer *unicodeTokenizer, db *sql.DB, ctx context.Context, options *addDirectoryContentsOptions) ([]string, error) {
    atx, err := createWriteTransaction(db, ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare transaction for update; %w", err)
    }
    defer atx.Finish()

    all_dirs, err := listDirectories(atx)
    if err != nil {
        return nil, err
    }

    all_failures := []string{}
    for _, d := range all_dirs {
        if err := ctx.Err(); err != nil {
            return nil, fmt.Errorf("request canceled while updating directory %q; %w", d.Path, err)
        }
        curfailures, err := addDirectoryContents(d.Path, d.Id, d.Names, tokenizer, atx, options)
        if err != nil {
            return nil, err
        }
        all_failures = append(all_failures, curfailures...)
    }

    // Assist the query planner by optimizing the DB after its contents are updated.
    _, err = atx.Exec("PRAGMA optimize=0x10002")
    if err != nil {
        return nil, fmt.Errorf("failed to optimize the database; %w", err)
    }

    err = atx.Commit()
    if err != nil {
        return nil, fmt.Errorf("failed to commit the transaction to update directories; %w", err)
    }

    return all_failures, nil
}

/**********************************************************************/

func removeUnusedTerms(db *sql.DB, ctx context.Context) error {
    atx, err := createWriteTransaction(db, ctx)
    if err != nil {
        return fmt.Errorf("failed to prepare transaction for deletion; %w", err)
    }
    defer atx.Finish()

    _, err = atx.Exec("DELETE FROM tokens WHERE tid NOT IN (SELECT tid FROM links)")
    if err != nil {
        return fmt.Errorf("failed to remove unused tokens; %w", err)
    }

    _, err = atx.Exec("DELETE FROM fields WHERE fid NOT IN (SELECT fid FROM links)")
    if err != nil {
        return fmt.Errorf("failed to remove unused field names; %w", err)
    }

    err = atx.Commit()
    if err != nil {
        return fmt.Errorf("failed to commit clean-up changes; %w", err)
    }

    return nil
}

func cleanDatabase(db *sql.DB, ctx context.Context) error {
    err := removeUnusedTerms(db, ctx)
    if err != nil {
        return err
    }
    _, err = db.ExecContext(ctx, "VACUUM")
    if err != nil {
        return fmt.Errorf("failed to vacuum the database; %w", err)
    }
    return nil
}

func backupDatabase(path string, db *sql.DB, ctx context.Context) error {
    var existing bool
    _, err := os.Lstat(path)
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

    _, err = db.ExecContext(ctx, "VACUUM INTO ?", path)
    if err != nil {
        all_errors := []error{ fmt.Errorf("failed to create a backup database; %w", err) }
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

type queryScroll struct {
    Time int64
    Pid int64
}

type queryOptions struct {
    IncludeMetadata bool
    PageLimit int
    Scroll *queryScroll
}

func newQueryOptions() queryOptions {
    return queryOptions{
        IncludeMetadata: true,
        PageLimit: 0,
        Scroll: nil,
    }
}

type queryResult struct {
    Pid int64 `json:"-"`
    Path string `json:"path"`
    User string `json:"user"`
    Time int64 `json:"time"`
    Metadata json.RawMessage `json:"metadata,omitempty"`
}

func queryTokens(query *searchClause, db * sql.DB, ctx context.Context, options queryOptions) ([]queryResult, error) {
    full := "SELECT paths.pid, paths.path, paths.user, paths.time"
    if options.IncludeMetadata {
        full += ", json_extract(paths.metadata, '$')"
    }
    full += " FROM paths"

    // The query can be nil.
    parameters := []interface{}{}
    query_present := false
    if query != nil {
        query_present = true
        curfilter, curparams := assembleFilter(query)
        full += " WHERE " + curfilter 
        parameters = append(parameters, curparams...)
    }

    // Handling pagination via scrolling window queries, see https://www.sqlite.org/rowvalue.html#scrolling_window_queries.
    // This should be pretty efficient as we have an index on 'time'.
    if options.Scroll != nil {
        if query_present {
            full += " AND"
        } else {
            full += " WHERE"
        }
        full += " (paths.time, paths.pid) < (?, ?)"
        parameters = append(parameters, options.Scroll.Time, options.Scroll.Pid)
    }
    full += " ORDER BY paths.time DESC, paths.pid DESC"
    if options.PageLimit > 0 {
        full += " LIMIT " + strconv.Itoa(options.PageLimit)
    }

    rows, err := db.QueryContext(ctx, full, parameters...)
    if err != nil {
        return nil, fmt.Errorf("failed to perform query; %w", err)
    }
    defer rows.Close()

    output := []queryResult{}
    for rows.Next() {
        if err := ctx.Err(); err != nil {
            return nil, fmt.Errorf("request canceled while scanning query results; %w", err)
        }

        var pid int64
        var path string
        var user string
        var time int64
        var metadata string

        ptrs := []interface{}{ &pid, &path, &user, &time }
        if options.IncludeMetadata {
            ptrs = append(ptrs, &metadata)
        }
        err := rows.Scan(ptrs...)
        if err != nil {
            return nil, fmt.Errorf("failed to extract row; %w", err)
        }

        res := queryResult{ Pid: pid, Path: path, User: user, Time: time }
        if options.IncludeMetadata {
            res.Metadata = []byte(metadata)
        }

        output = append(output, res)
    }

    return output, nil
}

/**********************************************************************/

func retrievePath(path string, include_metadata bool, db * sql.DB, ctx context.Context) (*queryResult, error) {
    hot := ""
    if include_metadata {
        hot = ", json_extract(paths.metadata, '$')"
    }
    full := fmt.Sprintf("SELECT paths.user, paths.time%s FROM paths WHERE paths.path = ?", hot)

    output := &queryResult{}
    var user string
    var time int64
    var metadata string

    var err error
    row := db.QueryRowContext(ctx, full, path)
    if include_metadata {
        err = row.Scan(&user, &time, &metadata)
    } else {
        err = row.Scan(&user, &time)
    }

    if errors.Is(err, sql.ErrNoRows) {
        return nil, nil
    } else if err != nil {
        return nil, err
    }

    output.Path = path
    output.User = user
    output.Time = time
    if include_metadata {
        output.Metadata = []byte(metadata)
    }

    return output, nil
}

/**********************************************************************/

type listRegisteredDirectoriesScroll struct {
    Time int64
    Did int64
}

type listRegisteredDirectoriesOptions struct {
    User *string
    ContainsPath *string
    WithinPath *string
    PathPrefix *string
    Exists *string
    PageLimit int
    Scroll *listRegisteredDirectoriesScroll
}

type listRegisteredDirectoriesResult struct {
    Did int64 `json:"-"`
    Path string `json:"path"`
    User string `json:"user"`
    Time int64 `json:"time"`
    Names json.RawMessage `json:"names"`
}

func listRegisteredDirectories(db *sql.DB, ctx context.Context, options listRegisteredDirectoriesOptions) ([]listRegisteredDirectoriesResult, error) {
    q := "SELECT did, path, user, time, json_extract(names, '$') FROM dirs"

    filters := []string{}
    parameters := []interface{}{}
    if options.User != nil {
        filters = append(filters, "user == ?")
        parameters = append(parameters, *(options.User))
    }

    if options.ContainsPath != nil {
        collected, err := getParentPaths(*(options.ContainsPath))
        if err != nil {
            return nil, err
        }
        query_clause := "?"
        for i := 1; i < len(collected); i++ {
            query_clause += ", ?"
        }
        filters = append(filters, "path IN (" + query_clause + ")")
        parameters = append(parameters, collected...)
    }

    if options.WithinPath != nil {
        filters = append(filters, "path GLOB ?")
        parameters = append(parameters, *(options.WithinPath) + "*")
    }
    if options.PathPrefix != nil { // this is for back-compatibility only.
        filters = append(filters, "path GLOB ?")
        parameters = append(parameters, *(options.PathPrefix) + "*")
    }
    if options.Scroll != nil {
        filters = append(filters, "(time, did) < (?, ?)")
        parameters = append(parameters, options.Scroll.Time, options.Scroll.Did)
    }

    if len(filters) > 0 {
        q += " WHERE " + strings.Join(filters, " AND ")
    }

    q += " ORDER BY time DESC, did DESC"
    if options.PageLimit > 0 {
        q += " LIMIT " + strconv.Itoa(options.PageLimit)
    }

    only_exists := false 
    only_nonexists := false
    if options.Exists != nil {
        only_exists = (*(options.Exists) == "true")
        only_nonexists = (*(options.Exists) == "false")
    }
    check_exists := only_exists || only_nonexists

    rows, err := db.QueryContext(ctx, q, parameters...)
    if err != nil {
        return nil, fmt.Errorf("failed to list registered directories; %w", err)
    }
    defer rows.Close()

    output := []listRegisteredDirectoriesResult{}
    for rows.Next() {
        if err := ctx.Err(); err != nil {
            return nil, fmt.Errorf("request canceled while scanning directory listing; %w", err)
        }

        current := listRegisteredDirectoriesResult{}

        var names string
        err := rows.Scan(&(current.Did), &(current.Path), &(current.User), &(current.Time), &names)
        current.Names = []byte(names)
        if err != nil {
            return nil, fmt.Errorf("failed to traverse rows of the 'dir' table; %w", err)
        }

        if check_exists {
            info, err := os.Lstat(current.Path)
            if err == nil && info.IsDir() {
                if only_nonexists {
                    continue
                }
            } else {
                if only_exists {
                    continue
                }
            }
        }

        if options.WithinPath != nil {
            rel, err := filepath.Rel(*(options.WithinPath), current.Path)
            if err != nil || !filepath.IsLocal(rel) {
                continue
            }
        }

        output = append(output, current)
    }

    return output, nil
}

func getParentPaths(path string) ([]interface{}, error) {
    collected := []interface{}{}
    for {
        // Note that there's no need to defend against '..', as
        // it is assumed that all paths are Cleaned before this point.
        collected = append(collected, path)
        newpath := filepath.Dir(path)
        if newpath == path {
            break
        }
        path = newpath
    }

    return collected, nil
}

func isDirectoryRegistered(path string, db * sql.DB, ctx context.Context) (bool, error) {
    collected, err := getParentPaths(path)
    if err != nil {
        return false, err
    }

    if len(collected) == 0 {
        return false, nil
    }
    query := "?"
    for i := 1; i < len(collected); i++ {
        query += ", ?"
    }

    q := fmt.Sprintf("SELECT COUNT(1) FROM dirs WHERE path IN (%s)", query)
    row := db.QueryRowContext(ctx, q, collected...)
    var num int
    err = row.Scan(&num)

    if err != nil {
        return false, err
    }
    return num > 0, nil
}

func fetchRegisteredDirectoryNames(path string, db *sql.DB, ctx context.Context) ([]string, error) {
    row := db.QueryRowContext(ctx, "SELECT json_extract(names, '$') FROM dirs WHERE path = ?", path)
    var names_as_str string
    err := row.Scan(&names_as_str) 

    if errors.Is(err, sql.ErrNoRows) {
        return nil, nil
    } else if err != nil {
        return nil, fmt.Errorf("failed to extract existing names for %q; %w",  path, err)
    }

    output := []string{}
    err = json.Unmarshal([]byte(names_as_str), &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse existing names for %q; %w",  path, err)
    }

    return output, nil
}

/**********************************************************************/

type listFieldsScroll struct {
    Field string
}

type listFieldsOptions struct {
    Pattern *string
    Count bool
    PageLimit int
    Scroll *listFieldsScroll
}

type listFieldsResult struct {
    Field string `json:"field"`
    Count *int64 `json:"count,omitempty"`
}

func listFields(db *sql.DB, ctx context.Context, options listFieldsOptions) ([]listFieldsResult, error) {
    outputs := []string{ "field" }
    parameters := []interface{}{}
    filters := []string{}
    extra_before := ""
    extra_after := ""

    if options.Count {
        outputs = append(outputs, "COUNT(DISTINCT links.pid)")
        extra_before = " INNER JOIN links ON links.fid = fields.fid"
        extra_after = " GROUP BY links.fid"
    }

    if options.Pattern != nil {
        filters = append(filters, "field GLOB ?")
        parameters = append(parameters, *(options.Pattern))
    }

    if options.Scroll != nil {
        filters = append(filters, "field > ?")
        parameters = append(parameters, options.Scroll.Field)
    }

    query := "SELECT " + strings.Join(outputs, ", ") + " FROM fields" + extra_before
    if len(filters) > 0 {
        query += " WHERE " + strings.Join(filters, " AND ")
    }
    query += extra_after
    query += " ORDER BY field ASC"
    if options.PageLimit > 0 {
        query += " LIMIT " + strconv.Itoa(options.PageLimit)
    }

    results, err := db.QueryContext(ctx, query, parameters...)
    if err != nil {
        return nil, fmt.Errorf("failed to perform query; %w", err)
    }

    output := []listFieldsResult{}
    for results.Next() {
        if err := ctx.Err(); err != nil {
            return nil, fmt.Errorf("request canceled while scanning field listing; %w", err)
        }

        current := listFieldsResult{}
        var err error
        if options.Count {
            var count int64
            err = results.Scan(&(current.Field), &count)
            current.Count = &count
        } else {
            err = results.Scan(&(current.Field))
        }
        if err != nil {
            return nil, fmt.Errorf("failed to extract row; %w", err)
        }
        output = append(output, current)
    }

    return output, nil
}

/**********************************************************************/

type listTokensScroll struct {
    Token string
}

type listTokensOptions struct {
    Pattern *string
    Field *string
    Count bool
    PageLimit int
    Scroll *listTokensScroll
}

type listTokensResult struct {
    Token string `json:"token"`
    Count *int64 `json:"count,omitempty"`
}

func listTokens(db *sql.DB, ctx context.Context, options listTokensOptions) ([]listTokensResult, error) {
    outputs := []string{ "token" }
    parameters := []interface{}{}
    filters := []string{}
    extra_before := ""
    extra_after := ""

    if options.Count || options.Field != nil {
        extra_before += " INNER JOIN links ON links.tid = tokens.tid"
        extra_after += " GROUP BY links.tid" // Avoid duplicates if the inner join is performed.
    }

    if options.Count {
        outputs = append(outputs, "COUNT(DISTINCT links.pid)")
    }

    if options.Field != nil {
        field := *(options.Field)
        action := "="
        if strings.Contains(field, "?") || strings.Contains(field, "*") {
            action = "GLOB"
        }
        extra_before += " INNER JOIN fields ON links.fid = fields.fid"
        filters = append(filters, "fields.field " + action + " ?")
        parameters = append(parameters, field)
    }

    if options.Pattern != nil {
        filters = append(filters, "token GLOB ?")
        parameters = append(parameters, *(options.Pattern))
    }

    if options.Scroll != nil {
        filters = append(filters, "token > ?")
        parameters = append(parameters, options.Scroll.Token)
    }

    query := "SELECT " + strings.Join(outputs, ", ") + " FROM tokens" + extra_before
    if len(filters) > 0 {
        query += " WHERE " + strings.Join(filters, " AND ")
    }
    query += extra_after
    query += " ORDER BY token ASC"
    if options.PageLimit > 0 {
        query += " LIMIT " + strconv.Itoa(options.PageLimit)
    }

    results, err := db.QueryContext(ctx, query, parameters...)
    if err != nil {
        return nil, fmt.Errorf("failed to perform query; %w", err)
    }

    output := []listTokensResult{}
    for results.Next() {
        if err := ctx.Err(); err != nil {
            return nil, fmt.Errorf("request canceled while scanning token listing; %w", err)
        }

        current := listTokensResult{}
        var err error
        if options.Count {
            var count int64
            err = results.Scan(&(current.Token), &count)
            current.Count = &count
        } else {
            err = results.Scan(&(current.Token))
        }
        if err != nil {
            return nil, fmt.Errorf("failed to extract row; %w", err)
        }
        output = append(output, current)
    }

    return output, nil
}
