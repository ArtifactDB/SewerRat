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

func createWriteTransaction(db *sql.DB) (*writeTransaction, error) {
    ctx := context.Background()
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

func initializeDatabase(path string) (*sql.DB, error) {
    accessible := false
    if _, err := os.Stat(path); err == nil {
        accessible = true
    }

    db, err := sql.Open("sqlite", path) 
    if err != nil {
        return nil, fmt.Errorf("failed to open read/write SQLite handle; %w", err)
    }

    // Maxing out at one connection so that there can only be one write at any
    // time; everyone else will have to block on the connection availability.
    db.SetMaxOpenConns(1)

    if (!accessible) {
        err := func () error {
            atx, err := createWriteTransaction(db)
            if err != nil {
                return fmt.Errorf("failed to prepare transaction for table setup; %w", err)
            }
            defer atx.Finish()

            _, err = atx.Exec(`
CREATE TABLE dirs(
    did INTEGER PRIMARY KEY, 
    path TEXT NOT NULL UNIQUE, 
    user TEXT NOT NULL, 
    time INTEGER NOT NULL,
    names BLOB
);
CREATE INDEX index_dirs_path ON dirs(path);
CREATE INDEX index_dirs_time ON dirs(time, user);
CREATE INDEX index_dirs_user ON dirs(user, time);

CREATE TABLE paths(
    pid INTEGER PRIMARY KEY, 
    did INTEGER NOT NULL,
    path TEXT NOT NULL UNIQUE, 
    user TEXT NOT NULL, 
    time INTEGER NOT NULL, 
    metadata BLOB,
    FOREIGN KEY(did) REFERENCES dirs(did) ON DELETE CASCADE
);
CREATE INDEX index_paths_path ON paths(path);
CREATE INDEX index_paths_time ON paths(time, user);
CREATE INDEX index_paths_user ON paths(user, time);

CREATE TABLE tokens(tid INTEGER PRIMARY KEY, token TEXT NOT NULL UNIQUE);
CREATE INDEX index_tokens ON tokens(token);

CREATE TABLE fields(fid INTEGER PRIMARY KEY, field TEXT NOT NULL UNIQUE);
CREATE INDEX index_fields ON fields(field);

CREATE TABLE links(pid INTEGER NOT NULL, fid INTEGER NOT NULL, tid INTEGER NOT NULL, FOREIGN KEY(pid) REFERENCES paths(pid) ON DELETE CASCADE, UNIQUE(pid, fid, tid));
CREATE INDEX index_links ON links(tid, fid);
`)
            if err != nil {
                return fmt.Errorf("failed to create table in %q; %w", path, err)
            }

            err = atx.Commit()
            if err != nil {
                return fmt.Errorf("failed to commit table creation commands for %s; %w", path, err)
            }

            // Write-ahead logging is persistent and doesn't need to be set on every connection,
            // see https://www.sqlite.org/wal.html#persistence_of_wal_mode.
            _, err = atx.Exec("PRAGMA journal_mode = WAL")
            if err != nil {
                return fmt.Errorf("failed to enable write-ahead logging; %w", err)
            }

            return nil
        }()

        if err != nil {
            os.Remove(path)
            return nil, fmt.Errorf("failed to create table in %q; %w", path, err)
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

// Recurse through the metadata structure to disassemble the tokens.
func tokenizeMetadata(parsed interface{}, path string, pid int64, field string, prepped *insertStatements, tokenizer *unicodeTokenizer) []string {
    failures := []string{}

    switch v := parsed.(type) {
    case []interface{}:
        for _, w := range v {
            tokfails := tokenizeMetadata(w, path, pid, field, prepped, tokenizer)
            failures = append(failures, tokfails...)
        }

    case map[string]interface{}:
        for k, w := range v {
            new_field := k
            if field != "" {
                new_field = field + "." + k
            }
            tokfails := tokenizeMetadata(w, path, pid, new_field, prepped, tokenizer)
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

    case string:
        tokens, err := tokenizer.Tokenize(v)
        if err != nil {
            return []string{ fmt.Sprintf("failed to tokenize %q in %q; %v", v, path, err) }
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

    case bool:
        var t string
        if v {
            t = "true"
        } else {
            t = "false"
        }

        _, err := prepped.Token.Exec(t)
        if err != nil {
            failures = append(failures, fmt.Sprintf("failed to insert token %q from %q; %v", t, path, err))
            break
        }

        _, err = prepped.Field.Exec(field)
        if err != nil {
            failures = append(failures, fmt.Sprintf("failed to insert field %q from %q; %v", field, path, err))
            break
        }

        _, err = prepped.Link.Exec(pid, field, t)
        if err != nil {
            failures = append(failures, fmt.Sprintf("failed to insert link for field %q to token %q from %q; %v", field, t, path, err))
            break
        }
    }

    return failures
}

/**********************************************************************/

func deleteDirectory(db *sql.DB, directory string) error {
    atx, err := createWriteTransaction(db)
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

func compareToExistingPaths(tx *writeTransaction, did int64, all_paths map[string]fs.FileInfo) ([]*FileInfoWithPath, []*FileInfoWithPath, []string, error) {
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

func addDirectoryContents(tx *writeTransaction, path string, did int64, base_names []string, tokenizer* unicodeTokenizer, concurrency int) ([]string, error) {
    all_failures := []string{}

    dir_contents, dir_failures := listMetadata(path, base_names)
    all_failures = append(all_failures, dir_failures...)

    new_paths, update_paths, purge_paths, err := compareToExistingPaths(tx, did, dir_contents)
    if err != nil {
        return nil, err
    }

    // Loading the metadata into memory; we use a thread pool to avoid opening too many file handles at once.
    new_assets := make([]*loadedMetadata, len(new_paths))
    update_assets := make([]*loadedMetadata, len(update_paths))
    {
        var wg sync.WaitGroup
        wg.Add(concurrency)
        ichannel := make(chan int)
        uchannel := make(chan int)

        for t := 0; t < concurrency; t++ {
            go func() {
                defer wg.Done()
                for i := range ichannel {
                    e := new_paths[i]
                    new_assets[i] = loadMetadata(e.Path, e.Info)
                }
                for i := range uchannel {
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
            if loaded.Failure != nil {
                all_failures = append(all_failures, loaded.Failure.Error())
                continue
            }

            var pid int64
            err := new_stmt.QueryRow(loaded.Path, did, loaded.User, loaded.Time.Unix(), loaded.Raw).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to insert %q into the database; %v", loaded.Path, err))
                continue
            }

            tokfails := tokenizeMetadata(loaded.Parsed, loaded.Path, pid, "", token_stmts, tokenizer)
            all_failures = append(all_failures, tokfails...)
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
            if loaded.Failure != nil {
                purge_paths = append(purge_paths, loaded.Path)
                all_failures = append(all_failures, loaded.Failure.Error())
                continue
            }

            var pid int64
            _, err := update_stmt.Exec(loaded.User, loaded.Time.Unix(), loaded.Raw, loaded.Path)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to update %q in the database; %v", loaded.Path, err))
                continue
            }

            err = pid_stmt.QueryRow(loaded.Path).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to inspect path ID for %q; %v", loaded.Path, err))
                continue
            }

            _, err = dellnk_stmt.Exec(pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to delete links for %q; %v", loaded.Path, err))
                continue
            }

            tokfails := tokenizeMetadata(loaded.Parsed, loaded.Path, pid, "", token_stmts, tokenizer)
            all_failures = append(all_failures, tokfails...)
        }
    }

    if len(purge_paths) > 0 {
        del_stmt, err := tx.Prepare("DELETE FROM paths WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare the delete transaction; %w", err)
        }
        defer del_stmt.Close()

        for _, x := range purge_paths {
            _, err := del_stmt.Exec(x)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to purge path %q from the database; %v", x, err))
            }
        }
    }

    return all_failures, nil
}

/**********************************************************************/

func addNewDirectory(db *sql.DB, path string, base_names []string, user string, tokenizer* unicodeTokenizer, concurrency int) ([]string, error) {
    b, err := json.Marshal(base_names)
    if err != nil {
        return nil, fmt.Errorf("failed to encode names as JSON; %w", err)
    }

    atx, err := createWriteTransaction(db)
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

    failures, err := addDirectoryContents(atx, path, did, base_names, tokenizer, concurrency)

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
        var id int64
        var path string
        var names_raw []byte
        if err := rows.Scan(&id, &path, &names_raw); err != nil {
            return nil, fmt.Errorf("failed to traverse rows of the 'dirs' table; %w", err)
        }

        var names []string
        err = json.Unmarshal(names_raw, &names)
        if err != nil {
            return nil, fmt.Errorf("failed to parse names of 'dirs' for %q; %w", path, err)
        }

        all_dirs = append(all_dirs, &registeredDirectory{ Id: id, Path: path, Names: names })
    }

    return all_dirs, nil
}

func updateDirectories(db *sql.DB, tokenizer *unicodeTokenizer, concurrency int) ([]string, error) {
    atx, err := createWriteTransaction(db)
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
        curfailures, err := addDirectoryContents(atx, d.Path, d.Id, d.Names, tokenizer, concurrency)
        if err != nil {
            return nil, err
        }
        all_failures = append(all_failures, curfailures...)
    }

    err = atx.Commit()
    if err != nil {
        return nil, fmt.Errorf("failed to commit the transaction to update directories; %w", err)
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

type queryResult struct {
    Pid int64 `json:"-"`
    Path string `json:"path"`
    User string `json:"user"`
    Time int64 `json:"time"`
    Metadata json.RawMessage `json:"metadata,omitempty"`
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
        curfilter, curparams := assembleFilter(query)
        full += " WHERE " + curfilter 
        parameters = append(parameters, curparams...)
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
    full += " ORDER BY paths.time DESC, paths.pid DESC"
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

/**********************************************************************/

func retrievePath(db * sql.DB, path string, include_metadata bool) (*queryResult, error) {
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
    row := db.QueryRow(full, path)
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

type listRegisteredDirectoriesQuery struct {
    User *string `json:"user"`
    ContainsPath *string `json:"contains_path"`
    PathPrefix *string `json:"path_prefix"`
}

type listRegisteredDirectoriesResult struct {
    Path string `json:"path"`
    User string `json:"user"`
    Time int64 `json:"time"`
    Names json.RawMessage `json:"names"`
}

func listRegisteredDirectories(db * sql.DB, query *listRegisteredDirectoriesQuery) ([]listRegisteredDirectoriesResult, error) {
    q := "SELECT path, user, time, json_extract(names, '$') FROM dirs"

    filters := []string{}
    parameters := []interface{}{}
    if query.User != nil {
        filters = append(filters, "user == ?")
        parameters = append(parameters, *(query.User))
    }

    if query.ContainsPath != nil {
        collected, err := getParentPaths(*(query.ContainsPath))
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

    if query.PathPrefix != nil {
        filters = append(filters, "path LIKE ?")
        parameters = append(parameters, *(query.PathPrefix) + "%")
    }

    if len(filters) > 0 {
        q = q + " WHERE " + strings.Join(filters, " AND ")
    }

    rows, err := db.Query(q, parameters...)
    if err != nil {
        return nil, fmt.Errorf("failed to list registered directories; %w", err)
    }
    defer rows.Close()

    output := []listRegisteredDirectoriesResult{}
    for rows.Next() {
        current := listRegisteredDirectoriesResult{}
        var names string
        err := rows.Scan(&(current.Path), &(current.User), &(current.Time), &names)
        current.Names = []byte(names)
        if err != nil {
            return nil, fmt.Errorf("failed to traverse rows of the 'dir' table; %w", err)
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

func isDirectoryRegistered(db * sql.DB, path string) (bool, error) {
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
    row := db.QueryRow(q, collected...)
    var num int
    err = row.Scan(&num)

    if err != nil {
        return false, err
    }
    return num > 0, nil
}

/**********************************************************************/

func fetchRegisteredDirectoryNames(db *sql.DB, path string) ([]string, error) {
    row := db.QueryRow("SELECT json_extract(names, '$') FROM dirs WHERE path = ?", path)
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

