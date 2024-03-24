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
    "context"
    _ "modernc.org/sqlite"
)

type ActiveTransaction struct {
    Conn *sql.Conn
    Tx *sql.Tx
}

func (t *ActiveTransaction) Finish() {
    t.Tx.Rollback() // This is a no-op once committed.
    t.Conn.Close()
}

func createTransaction(db *sql.DB) (*ActiveTransaction, error) {
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
            conn.Close()
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

    tx, err := conn.BeginTx(ctx, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to create transaction; %w", err)
    }
    defer func() {
        if !success {
            tx.Rollback()
        }
    }()

    success = true;
    return &ActiveTransaction{ Conn: conn, Tx: tx }, nil
}

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
        err := func () error {
            atx, err := createTransaction(db)
            if err != nil {
                return fmt.Errorf("failed to prepare transaction for table setup; %w", err)
            }
            defer atx.Finish()

            _, err = atx.Tx.Exec(`
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

            err = atx.Tx.Commit()
            if err != nil {
                return fmt.Errorf("failed to commit table creation commands for %s; %w", path, err)
            }

            // Write-ahead logging is persistent and doesn't need to be set on every connection,
            // see https://www.sqlite.org/wal.html#persistence_of_wal_mode.
            _, err = atx.Conn.ExecContext(context.Background(), "PRAGMA journal_mode = WAL")
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

func newInsertStatements(tx *sql.Tx) (*insertStatements, error) {
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

type loadedMetadata struct {
    Path string
    Failure error
    Raw []byte
    User string
    Time time.Time
    Parsed interface{}
}

func loadMetadata(paths []string, stats []fs.FileInfo) []*loadedMetadata {
    assets := make([]*loadedMetadata, len(paths))

    var wg sync.WaitGroup
    wg.Add(len(paths))

    for i, f := range paths {
        go func(i int, f string) {
            defer wg.Done()
            assets[i] = &loadedMetadata{ Path: f, Failure: nil }

            raw, err := os.ReadFile(f)
            if err != nil {
                assets[i].Failure = fmt.Errorf("failed to read %q; %w", f, err)
                return
            }

            var vals interface{}
            err = json.Unmarshal(raw, &vals)
            if err != nil {
                assets[i].Failure = fmt.Errorf("failed to parse %q; %w", f, err)
                return
            }

            var info fs.FileInfo
            if stats != nil {
                info = stats[i]
            } else {
                raw_info, err := os.Stat(f)
                if err != nil {
                    assets[i].Failure = fmt.Errorf("failed to stat %q; %w", f, err)
                    return
                }
                info = raw_info
            }

            username, err := identifyUser(info)
            if err != nil {
                assets[i].Failure = fmt.Errorf("failed to determine author of %q; %w", f, err)
                return
            }

            assets[i].User = username
            assets[i].Time = info.ModTime()
            assets[i].Raw = raw
            assets[i].Parsed = vals
        }(i, f)
    }

    wg.Wait()
    return assets
}

/**********************************************************************/

func addDirectory(db *sql.DB, directory string, of_interest map[string]bool, user string, tokenizer *unicodeTokenizer) ([]string, error) {
    all_failures := []string{}

    paths := []string{}
    filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
        // Just skip any directories that we can't access.
        if err != nil {
            all_failures = append(all_failures, fmt.Sprintf("failed to walk %q; %v", path, err))
        }
        if !d.IsDir() {
            if _, ok := of_interest[d.Name()]; ok {
                paths = append(paths, path)
            }
        }
        return nil
    })
    assets := loadMetadata(paths, nil)

    atx, err := createTransaction(db)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare transaction for directory addition; %w", err)
    }
    defer atx.Finish()

    // Delete all previously registered paths for this directory for a fresh start;
    // otherwise there's no way to easily get rid of old paths that are no longer here.
    _, err = atx.Tx.Exec("DELETE FROM dirs WHERE path == ?", directory)
    if err != nil {
        return nil, fmt.Errorf("failed to delete %q; %w", directory, err)
    }

    var did int64
    {

        names := []string{}
        for k, _ := range of_interest {
            names = append(names, k)
        }

        b, err := json.Marshal(names)
        if err != nil {
            return nil, fmt.Errorf("failed to encode names as JSON; %w", err)
        }

        err = atx.Tx.QueryRow(
            "INSERT INTO dirs(path, user, time, names) VALUES(?, ?, ?, ?) RETURNING did",
            directory, 
            user, 
            time.Now().Unix(), 
            b,
        ).Scan(&did)
        if err != nil {
            return nil, fmt.Errorf("failed to insert directory; %w", err)
        }
    }

    // Adding the directory contents.
    prepped, err := newInsertStatements(atx.Tx)
    if err != nil {
        return nil, fmt.Errorf("failed to create prepared insertion statements for %q; %w", directory, err)
    }
    defer prepped.Close()

    pstmt, err := atx.Tx.Prepare("INSERT INTO paths(path, did, user, time, metadata) VALUES(?, ?, ?, ?, ?) RETURNING pid")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare path insertion statement; %w", err)
    }
    defer pstmt.Close()

    for _, a := range assets {
        if a.Failure != nil {
            all_failures = append(all_failures, a.Failure.Error())
            continue
        }

        var pid int64
        err := pstmt.QueryRow(a.Path, did, a.User, a.Time.Unix(), a.Raw).Scan(&pid)
        if err != nil {
            all_failures = append(all_failures, fmt.Sprintf("failed to insert %q into the database; %v", a.Path, err))
            continue
        }

        tokfails := tokenizeMetadata(atx.Tx, a.Parsed, a.Path, pid, "", prepped, tokenizer)
        all_failures = append(all_failures, tokfails...)
    }

    err = atx.Tx.Commit()
    if err != nil {
        return nil, fmt.Errorf("failed to commit the transaction for %q; %w", directory, err)
    }

    return all_failures, nil
}

// Recurse through the metadata structure to disassemble the tokens.
func tokenizeMetadata(tx *sql.Tx, parsed interface{}, path string, pid int64, field string, prepped *insertStatements, tokenizer *unicodeTokenizer) []string {
    failures := []string{}

    switch v := parsed.(type) {
    case []interface{}:
        for _, w := range v {
            tokfails := tokenizeMetadata(tx, w, path, pid, field, prepped, tokenizer)
            failures = append(failures, tokfails...)
        }

    case map[string]interface{}:
        for k, w := range v {
            new_field := k
            if field != "" {
                new_field = field + "." + k
            }
            tokfails := tokenizeMetadata(tx, w, path, pid, new_field, prepped, tokenizer)
            failures = append(failures, tokfails...)
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
    }

    return failures
}

/**********************************************************************/

func deleteDirectory(db *sql.DB, directory string) error {
    atx, err := createTransaction(db)
    if err != nil {
        return fmt.Errorf("failed to prepare transaction for deletion; %w", err)
    }
    defer atx.Finish()

    _, err = atx.Tx.Exec("DELETE FROM dirs WHERE path == ?", directory)
    if err != nil {
        return fmt.Errorf("failed to delete %q; %w", directory, err)
    }

    err = atx.Tx.Commit()
    if err != nil {
        return fmt.Errorf("failed to commit deletion transaction for %q; %w", directory, err)
    }

    return nil
}

/**********************************************************************/

type directoryEntry struct {
    Parent int64
    Info fs.FileInfo
}

func scanDirectories(db *sql.DB) (map[string]*directoryEntry, []string, error) {
    type Directory struct {
        Path string
        Id int64
        Names []string
    }

    // Wrapped inside a function so that 'rows' are closed before further ops.
    all_dirs, err := func() ([]*Directory, error) {
        rows, err := db.Query("SELECT did, path, names from dirs") 
        if err != nil {
            return nil, fmt.Errorf("failed to query the 'dirs' table; %w", err)
        }
        defer rows.Close()

        all_dirs := []*Directory{}
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

            all_dirs = append(all_dirs, &Directory{ Id: id, Path: path, Names: names })
        }

        return all_dirs, nil
    }()
    if err != nil {
        return nil, nil, fmt.Errorf("failed to scan directories; %w", err)
    }

    // Listing the contents of all registered directories, in parallel.
    dircontents := make([]map[string]*directoryEntry, len(all_dirs))
    dirfailures := make([][]string, len(all_dirs))

    var wg sync.WaitGroup
    wg.Add(len(all_dirs))

    for i, dir := range all_dirs {
        go func(i int, dir *Directory) {
            defer wg.Done()

            curcontents := map[string]*directoryEntry{}
            curfailures := []string{}
            curnames := map[string]bool{}
            for _, n := range dir.Names {
                curnames[n] = true
            }

            filepath.WalkDir(dir.Path, func(path string, d fs.DirEntry, err error) error {
                // Just skip any directories that we can't access.
                if err != nil {
                    curfailures = append(curfailures, fmt.Sprintf("failed to walk %q; %v", path, err))
                    return nil
                }
                if d.IsDir() {
                    return nil
                }
                _, ok := curnames[filepath.Base(path)]
                if !ok {
                    return nil
                }
                info, err := d.Info()
                if err != nil {
                    curfailures = append(curfailures, fmt.Sprintf("failed to stat %q; %v", path, err))
                    return nil
                }
                curcontents[path] = &directoryEntry{ Parent: dir.Id, Info: info }
                return nil
            })

            dircontents[i] = curcontents 
            dirfailures[i] = curfailures
        }(i, dir)
    }

    wg.Wait()

    out_contents := map[string]*directoryEntry{}
    for _, d := range dircontents {
        for k, v := range d {
            out_contents[k] = v
        }
    }

    out_failures := []string{}
    for _, f := range dirfailures {
        out_failures = append(out_failures, f...)
    }

    return out_contents, out_failures, nil
}

func checkExistingPaths(db *sql.DB, all_contents map[string]*directoryEntry) ([]*loadedMetadata, []string, error) {
    // Scan through all registered paths to check if any of these
    // already exist. This is again wrapped inside a function so that 'rows'
    // are closed before further ops.
    update_paths, update_stats, purge_paths, err := func() ([]string, []fs.FileInfo, []string, error) {
        rows, err := db.Query("SELECT path, time from paths") 
        if err != nil {
            return nil, nil, nil, fmt.Errorf("failed to query the 'paths' table; %w", err)
        }
        defer rows.Close()

        update_paths := []string{}
        update_stats := []fs.FileInfo{}
        purge_paths := []string{}

        for rows.Next() {
            var path string
            var time int64
            if err := rows.Scan(&path, &time); err != nil {
                return nil, nil, nil, fmt.Errorf("failed to traverse rows of the 'paths' table; %w", err)
            }

            candidate, ok := all_contents[path]
            if !ok {
                purge_paths = append(purge_paths, path)
                continue
            }
            delete(all_contents, path) // indicate that this does not need to be updated anymore.

            newtime := candidate.Info.ModTime().Unix()
            if newtime == time {
                continue
            }

            // No need to track the directory ID, as that is already present in the row.
            update_paths = append(update_paths, path)
            update_stats = append(update_stats, candidate.Info)
        }

        return update_paths, update_stats, purge_paths, nil
    }()
    if err != nil {
        return nil, nil, err
    }

    return loadMetadata(update_paths, update_stats), purge_paths, nil
}

func updatePaths(db *sql.DB, tokenizer* unicodeTokenizer) ([]string, error) {
    all_failures := []string{}

    dir_contents, dir_failures, err := scanDirectories(db)
    if err != nil {
        return nil, err
    }
    all_failures = append(all_failures, dir_failures...)

    // This step will also prune 'dir_contents' to remove the paths that
    // are already present in 'db', such that the subsequent creation of
    // 'new_assets' doesn't include paths that are already indexed.
    update_assets, purge_paths, err := checkExistingPaths(db, dir_contents)
    if err != nil {
        return nil, err
    }

    new_ids := []int64{}
    new_stats := []fs.FileInfo{}
    new_paths := []string{}    
    for k, v := range dir_contents {
        new_paths = append(new_paths, k)
        new_ids = append(new_ids, v.Parent)
        new_stats = append(new_stats, v.Info)
    }
    new_assets := loadMetadata(new_paths, new_stats)

    atx, err := createTransaction(db)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare a database transaction for path updates; %w", err)
    }
    defer atx.Finish()

    prepped, err := newInsertStatements(atx.Tx)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare token insertion statements for the update; %w", err)
    }
    defer prepped.Close()

    {
        pustmt, err := atx.Tx.Prepare("UPDATE paths SET user = ?, time = ?, metadata = ? WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path update statement; %w", err)
        }
        defer pustmt.Close()

        pistmt, err := atx.Tx.Prepare("SELECT pid FROM paths WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path ID statement; %w", err)
        }
        defer pistmt.Close()

        delstmt, err := atx.Tx.Prepare("DELETE FROM links WHERE pid = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare link deletion statement; %w", err)
        }
        defer delstmt.Close()

        for _, u := range update_assets {
            if u.Failure != nil {
                all_failures = append(all_failures, u.Failure.Error())
                purge_paths = append(purge_paths, u.Path) // reassign it for purging.
                continue
            }

            _, err := pustmt.Exec(u.User, u.Time.Unix(), u.Raw, u.Path)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to update %q in the database; %v", u.Path, err))
                continue
            }

            var pid int64
            err = pistmt.QueryRow(u.Path).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to inspect path ID for %q; %v", u.Path, err))
                continue
            }

            _, err = delstmt.Exec(pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to delete links for %q; %v", u.Path, err))
                continue
            }

            tokfails := tokenizeMetadata(atx.Tx, u.Parsed, u.Path, pid, "", prepped, tokenizer)
            all_failures = append(all_failures, tokfails...)
        }
    }

    {
        pstmt, err := atx.Tx.Prepare("INSERT INTO paths(path, did, user, time, metadata) VALUES(?, ?, ?, ?, ?) RETURNING pid")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare path insertion statement; %w", err)
        }
        defer pstmt.Close()

        for i, a := range new_assets {
            if a.Failure != nil {
                all_failures = append(all_failures, a.Failure.Error())
                continue
            }

            var pid int64
            err := pstmt.QueryRow(a.Path, new_ids[i], a.User, a.Time.Unix(), a.Raw).Scan(&pid)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to insert %q into the database; %v", a.Path, err))
                continue
            }

            tokfails := tokenizeMetadata(atx.Tx, a.Parsed, a.Path, pid, "", prepped, tokenizer)
            all_failures = append(all_failures, tokfails...)
        }
    }

    {
        delstmt, err := atx.Tx.Prepare("DELETE FROM paths WHERE path = ?")
        if err != nil {
            return nil, fmt.Errorf("failed to prepare the delete transaction; %w", err)
        }
        defer delstmt.Close()

        for _, x := range purge_paths {
            _, err := delstmt.Exec(x)
            if err != nil {
                all_failures = append(all_failures, fmt.Sprintf("failed to purge path %q from the database; %v", x, err))
            }
        }
    }

    err = atx.Tx.Commit()
    if err != nil {
        return nil, fmt.Errorf("failed to commit the update transaction; %w", err)
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
