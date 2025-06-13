package main

import (
    "testing"
    "os"
    "path/filepath"
    "sort"
    "os/user"
    "time"
    "strings"
    "errors"
    "database/sql"
    "encoding/json"
    "context"
)

func TestInitializeDatabase(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    dbpath := filepath.Join(tmp, "db.sqlite3")

    t.Run("simple", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()

        if _, err := os.Lstat(dbpath); err != nil {
            t.Fatalf("database file doesn't seem to exist; %v", err)
        }

        res, err := dbconn.Query("SELECT name FROM sqlite_master WHERE type='table';")
        if err != nil {
            t.Fatalf(err.Error())
        }

        collected := []string{}
        for res.Next() {
            var tabname string
            err = res.Scan(&tabname)
            if err != nil {
                t.Fatalf(err.Error())
            }
            collected = append(collected, tabname)
        }

        sort.Strings(collected)
        if !equalStringArrays(collected, []string{ "dirs", "fields", "links", "paths", "tokens" }) {
            t.Fatalf("not all tables were correctly initialized")
        }

        ver_res := dbconn.QueryRow("PRAGMA user_version")
        var version int
        err = ver_res.Scan(&version)
        if err != nil {
            t.Fatalf("failed to extract version; %v", err)
        }
        if version != 1 {
            t.Error("expected version 1 of the file")
        }
    })

    // Checking that initializeDatabase doesn't wipe our existing information.
    func() {
        dbconn, err := sql.Open("sqlite", dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()

        var did int64
        err = dbconn.QueryRow("INSERT INTO dirs(path, user, time, names) VALUES(?, ?, ?, ?) RETURNING did", "whee/superfoo", "blah", 123456, []byte("[\"a.json\"]")).Scan(&did)
        if err != nil {
            t.Fatalf(err.Error())
        }

        _, err = dbconn.Exec("INSERT INTO paths(path, did, user, time, metadata) VALUES(?, ?, ?, ?, ?)", "whee/superfoo/stuff", did, "blah", 123456, []byte("[1,2,3]"))
        if err != nil {
            t.Fatalf(err.Error())
        }
    }()

    t.Run("reinitialized", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatal(err)
        }
        defer dbconn.Close()

        all_paths, err := listPaths(dbconn, "whee")
        if err != nil {
            t.Fatal(err)
        }
        if len(all_paths) != 1 {
            t.Fatalf("unexpected number of paths in the DB %v", all_paths)
        }
    })

    // Checking that we auto-update the indices based on the version.
    t.Run("updated", func(t *testing.T) {
        dbpath2 := filepath.Join(tmp, "db2.sqlite3")

        err := func() error {
            db, err := sql.Open("sqlite", dbpath2) 
            if err != nil {
                return err
            }
            defer db.Close()

            _, err = db.Exec(createTableStatement())
            if err != nil {
                return err
            }

            _, err = db.Exec("CREATE INDEX super_foo_bar ON dirs(path)")
            if err != nil {
                return err
            }

            _, err = db.Exec("CREATE INDEX ur_mom ON tokens(token)")
            if err != nil {
                return err
            }

            return nil
        }()
        if err != nil {
            t.Fatal(err)
        }

        dbconn, err := initializeDatabase(dbpath2)
        if err != nil {
            t.Fatal(err)
        }
        defer dbconn.Close()

        res, err := dbconn.Query("SELECT name FROM sqlite_master WHERE type == 'index'")
        if err != nil {
            t.Fatalf("failed to identify all indices in the database; %v", err)
        }

        all_indices := map[string]bool{}
        for res.Next() {
            var index_name string
            err := res.Scan(&index_name)
            if err != nil {
                t.Fatalf("failed to extract name of an index in the database; %v", err)
            }
            all_indices[index_name] = true
        }

        for _, previous := range []string{ "super_foo_bar", "ur_mom" } {
            if _, ok := all_indices[previous]; ok {
                t.Errorf("existing index %q should have been deleted on update", previous)
            }
        }

        for _, updated := range []string{ "index_dirs_path", "index_tokens" } {
            if _, ok := all_indices[updated]; !ok {
                t.Errorf("new index %q should have been added on update", updated)
            }
        }
    })
}

func searchForLink(dbconn *sql.DB, path, field, token string) (bool, error) {
    count := -1

    err := dbconn.QueryRow(`
SELECT COUNT(links.pid) FROM links
INNER JOIN paths ON paths.pid = links.pid 
INNER JOIN fields ON fields.fid = links.fid
INNER JOIN tokens ON tokens.tid = links.tid
WHERE paths.path = ? AND fields.field = ? AND tokens.token = ? 
`, path, field, token).Scan(&count)
    if err != nil {
        return false, err
    }

    if count != 0 && count != 1 {
        return false, errors.New("not sure what happened here")
    }

    return count != 0, nil
}

func TestAddNewDirectory(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    self, err := user.Current()
    if err != nil {
        t.Fatalf(err.Error())
    }
    username := self.Username

    dbpath := filepath.Join(tmp, "db.sqlite3")

    t.Run("simple", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        comments, err := addNewDirectory(to_add, []string{ "metadata.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }
        now := time.Now().Unix()

        {
            rows, err := dbconn.Query("SELECT path, user, time, json_extract(names, '$') FROM dirs")
            if err != nil {
                t.Fatalf(err.Error())
            }
            defer rows.Close()

            counter := 0
            for rows.Next() {
                var path, owner, names string
                var creation int64
                err = rows.Scan(&path, &owner, &creation, &names)
                if err != nil {
                    t.Fatalf(err.Error())
                }

                if (path != to_add) {
                    t.Fatalf("invalid registration directory %q", path)
                }

                if creation <= 0 || creation > now {
                    t.Fatalf("invalid registration time %q", creation)
                }
                if owner != "myself" {
                    t.Fatalf("incorrect registering username %q", owner)
                }
                if !strings.HasPrefix(names, "[") || !strings.HasSuffix(names, "]") {
                    t.Fatalf("unexpected names %q", names)
                }

                counter += 1
            }

            if counter != 1 {
                t.Fatalf("expected exactly 1 entry in 'dirs' (got %v)", counter)
            }
        }

        {
            rows, err := dbconn.Query("SELECT path, user, time, json_extract(metadata, '$') FROM paths")
            if err != nil {
                t.Fatalf(err.Error())
            }
            defer rows.Close()

            all_paths := []string{}
            for rows.Next() {
                var path, owner, metadata string
                var creation int64
                err = rows.Scan(&path, &owner, &creation, &metadata)
                if err != nil {
                    t.Fatalf(err.Error())
                }

                if creation <= 0 || creation > now {
                    t.Fatalf("invalid creation time %q", creation)
                }
                if username != owner {
                    t.Fatalf("incorrect username %q", owner)
                }
                if !strings.HasPrefix(metadata, "{") || !strings.HasSuffix(metadata, "}") {
                    t.Fatalf("unexpected metadata %q", metadata)
                }

                rel, err := filepath.Rel(to_add, path)
                if err != nil {
                    t.Fatalf(err.Error())
                }
                all_paths = append(all_paths, rel)
            }

            sort.Strings(all_paths)
            if !equalStringArrays(all_paths, []string{ "metadata.json", "stuff/metadata.json" }) {
                t.Fatalf("unexpected paths in the index %v", all_paths)
            }
        }

        {
            found, err := searchForLink(dbconn, filepath.Join(to_add, "metadata.json"), "foo", "aaron")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !found {
                t.Fatalf("could not find token/field combination")
            }

            found, err = searchForLink(dbconn, filepath.Join(to_add, "metadata.json"), "bar.breed", "leicester")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !found {
                t.Fatalf("could not find token/field combination")
            }

            found, err = searchForLink(dbconn, filepath.Join(to_add, "stuff", "metadata.json"), "characters.first", "hoshino")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !found {
                t.Fatalf("could not find token/field combination")
            }
        }
    })

    t.Run("multi-target", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        // Works with multiple JSON targets.
        comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myslef", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        all_dirs, err := listDirs(dbconn)
        if err != nil {
            t.Fatal(err)
        }
        payload, ok := all_dirs[to_add]
        if len(all_dirs) != 1 || !ok {
            t.Fatalf("unexpected directories in the index %v", all_dirs)
        }
        if !equalStringArrays(payload, []string{ "metadata.json", "other.json" }) {
            t.Fatalf("unexpected names in the index %v", payload)
        }

        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/metadata.json", "to_add/stuff/metadata.json", "to_add/stuff/other.json", "to_add/whee/other.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })

    to_add2 := filepath.Join(tmp, "to_add2")
    err = mockDirectory(to_add2)
    if err != nil {
        t.Fatalf(err.Error())
    }

    t.Run("multi-directory", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        // Works with multiple JSON directories.
        comments, err := addNewDirectory(to_add, []string{ "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        comments, err = addNewDirectory(to_add2, []string{ "metadata.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        all_dirs, err := listDirs(dbconn)
        if err != nil {
            t.Fatal(err)
        }
        payload, ok := all_dirs[to_add]
        payload2, ok2 := all_dirs[to_add2]
        if len(all_dirs) != 2 || !ok || !ok2 {
            t.Fatalf("unexpected directories in the index %v", all_dirs)
        }
        if !equalStringArrays(payload, []string{ "other.json" }) {
            t.Fatalf("unexpected names in the index %v", payload)
        }
        if !equalStringArrays(payload2, []string{ "metadata.json" }) {
            t.Fatalf("unexpected names in the index %v", payload2)
        }

        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/stuff/other.json", "to_add/whee/other.json", "to_add2/metadata.json", "to_add2/stuff/metadata.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }

        // Recalling on an existing directory wipes out existing entries and replaces it.
        comments, err = addNewDirectory(to_add, []string{ "metadata.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        all_paths, err = listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/metadata.json", "to_add/stuff/metadata.json", "to_add2/metadata.json", "to_add2/stuff/metadata.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })

    // Making up an invalid file.
    to_fail := filepath.Join(tmp, "to_fail")
    err = mockDirectory(to_fail)
    if err != nil {
        t.Fatalf(err.Error())
    }

    err = os.WriteFile(filepath.Join(to_fail, "stuff", "metadata.json"), []byte("{ asdasd }"), 0644)
    if err != nil {
        t.Fatalf(err.Error())
    }

    t.Run("failure-comments", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        // Reports the error correctly.
        comments, err := addNewDirectory(to_fail, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) != 1 || !strings.Contains(comments[0], "stuff") {
            t.Fatalf("unexpected (lack of) comments from the directory addition %v", comments)
        }
    })

    // Making up a directory of symbolic links.
    symdir := filepath.Join(tmp, "symlink")
    err = os.MkdirAll(symdir, 0700)
    if err != nil {
        t.Fatalf("failed to create a symlink directory; %v", err)
    }

    err = os.Symlink(filepath.Join(to_add, "metadata.json"), filepath.Join(symdir, "metadata.json"))
    if err != nil {
        t.Fatalf("failed to create a symlink; %v", err)
    }

    err = os.Link(filepath.Join(to_add, "whee", "other.json"), filepath.Join(symdir, "other.json"))
    if err != nil {
        t.Fatalf("failed to create a hardlink; %v", err)
    }

    err = os.Symlink(filepath.Join(to_add, "stuff"), filepath.Join(symdir, "stuff"))
    if err != nil {
        t.Fatalf("failed to create a symlink; %v", err)
    }

    t.Run("symlink protection", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        comments, err := addNewDirectory(symdir, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) != 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatal(err)
        }

        // All symlink paths to directories/files are ignored.
        if !equalStringArrays(all_paths, []string{ "symlink/metadata.json", "symlink/other.json" }) {
            t.Fatalf("unexpected paths %v", all_paths)
        }
    })

    t.Run("canceled", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err = addNewDirectory(to_add, []string{ "metadata.json" }, "myself", tokr, dbconn, canceled, add_options)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("expected add directory job to be canceled")
        }
    })
}

func TestDeleteDirectory(t *testing.T) {
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

    // Mocking up two directories this time.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    to_add2 := filepath.Join(tmp, "to_add2")
    err = mockDirectory(to_add2)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    comments, err = addNewDirectory(to_add2, []string{ "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    // Deleting the first directory; this does not affect the second directory.
    err = deleteDirectory(to_add, dbconn, context.Background())
    if err != nil {
        t.Fatalf(err.Error())
    }

    all_dirs, err := listDirs(dbconn)
    if err != nil {
        t.Fatal(err)
    }
    _, ok := all_dirs[to_add]
    _, ok2 := all_dirs[to_add2]
    if len(all_dirs) != 1 || ok || !ok2 {
        t.Fatalf("unexpected directories in the index %v", all_dirs)
    }

    all_paths, err := listPaths(dbconn, tmp)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if !equalStringArrays(all_paths, []string{ "to_add2/stuff/other.json", "to_add2/whee/other.json"}) {
        t.Fatalf("unexpected paths in the index %v", all_paths)
    }

    {
        // Checking that only the second directory's links are present.
        rows, err := dbconn.Query("SELECT paths.path FROM links INNER JOIN paths ON paths.pid = links.pid")
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer rows.Close()

        for rows.Next() {
            var path string
            err = rows.Scan(&path)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !strings.HasPrefix(path, to_add2 + "/") {
                t.Fatalf("detected unexpected path after deletion %q", path)
            }
        }

        // Queries still work.
        found, err := searchForLink(dbconn, filepath.Join(to_add2, "stuff", "other.json"), "name", "chicken")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("could not find token/field combination")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add2, "stuff", "other.json"), "recipe", "chicken")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("could not find token/field combination")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add2, "stuff", "other.json"), "recipe", "chicken")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("could not find token/field combination")
        }
    }

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        err := deleteDirectory(to_add2, dbconn, canceled)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("expected delete directory to be canceled")
        }

        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatal(err)
        }
        if !equalStringArrays(all_paths, []string{ "to_add2/stuff/other.json", "to_add2/whee/other.json"}) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })
}

func TestUpdateDirectories(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    dbpath := filepath.Join(tmp, "db.sqlite3")

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    getTime := func(dbconn *sql.DB, path string) int64 {
        var val int64
        err := dbconn.QueryRow("SELECT time FROM paths WHERE path = ?", path).Scan(&val)
        if err != nil {
            t.Fatalf(err.Error())
        }
        return val
    }

    // This function checks that a deleted path has no presence in the 'links' table.
    hasAnyLink := func(dbconn *sql.DB, path string) (bool, error) {
        count := -1

        err := dbconn.QueryRow(`
SELECT COUNT(links.pid) FROM links
JOIN paths ON paths.pid = links.pid 
WHERE paths.path = ?
    `, path).Scan(&count)
        if err != nil {
            return false, err
        }

        return count > 0, nil
    }

    // This function assumes that the test scenario involves overwriting 'stuff/metadata.json' and
    // 'whee/other.json'. We need to check that the specific links associated with each overwritten
    // file are absent, as the paths themselves will still be present (and thus hasAnyLink won't work).
    checkUpdatedLinks := func(dbconn *sql.DB, to_add string, should_find bool) bool {
        found, err := searchForLink(dbconn, filepath.Join(to_add, "stuff", "metadata.json"), "anime", "yuru")
        if err != nil {
            t.Fatal(err)
        }
        if found != should_find {
            return false
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add, "whee", "other.json"), "favorites", "biyori")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if found != should_find {
            return false
        }

        return true
    }

    t.Run("modified", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        to_add := filepath.Join(tmp, "to_add")
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer os.RemoveAll(to_add)

        var oldtime1, oldtime2 int64
        {
            comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if len(comments) > 0 {
                t.Fatalf("unexpected comments from the directory addition %v", comments)
            }

            ok := checkUpdatedLinks(dbconn, to_add, true) // positive control.
            if !ok {
                t.Fatal("failed to find an expected link prior to update")
            }

            oldtime1 = getTime(dbconn, filepath.Join(to_add, "stuff", "metadata.json"))
            oldtime2 = getTime(dbconn, filepath.Join(to_add, "whee", "other.json"))
        }

        // Reorganizing stuff by modifying files.
        time.Sleep(time.Second * 2)

        err = os.WriteFile(filepath.Join(to_add, "stuff", "metadata.json"), []byte(`{ "melon": "watermelon", "flesh": "red" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.WriteFile(filepath.Join(to_add, "whee", "other.json"), []byte(`{ "melon": "canteloupe", "flesh": "orange" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        // Now actually running the update.
        comments, err := updateDirectories(tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from updating")
        }

        // Check that the links are present for the updated files. 
        found, err := searchForLink(dbconn, filepath.Join(to_add, "whee", "other.json"), "melon", "canteloupe")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add, "stuff", "metadata.json"), "melon", "watermelon")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        // Check that the times were updated.
        if oldtime1 >= getTime(dbconn, filepath.Join(to_add, "stuff", "metadata.json")) {
            t.Fatalf("time was not updated properly")
        }
        if oldtime2 >= getTime(dbconn, filepath.Join(to_add, "whee", "other.json")) {
            t.Fatalf("time was not updated properly")
        }

        // Check that old links associated with the overwritten files were deleted.
        ok := checkUpdatedLinks(dbconn, to_add, false)
        if !ok {
            t.Fatal("found an unexpected link after the update")
        }

        // Other links are still present though,
        found, err = searchForLink(dbconn, filepath.Join(to_add, "stuff", "other.json"), "variants", "lamb")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatal("missing a link for a non-modified file")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add, "metadata.json"), "bar.breed", "merino")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatal("missing a link for a non-modified file")
        }

        // All paths are present.
        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/metadata.json", "to_add/stuff/metadata.json", "to_add/stuff/other.json", "to_add/whee/other.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })

    t.Run("deleted", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        to_add := filepath.Join(tmp, "to_add")
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer os.RemoveAll(to_add)

        {
            comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if len(comments) > 0 {
                t.Fatalf("unexpected comments from the directory addition %v", comments)
            }
        }

        // Deleting some files.
        err = os.Remove(filepath.Join(to_add, "metadata.json"))
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.Remove(filepath.Join(to_add, "stuff", "other.json"))
        if err != nil {
            t.Fatalf(err.Error())
        }

        // Now actually running the update.
        comments, err := updateDirectories(tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from updating")
        }

        // Check that links for the deleted files have been lost.
        for _, f := range []string{ "metadata.json", filepath.Join("stuff", "other.json") } {
            found, err := hasAnyLink(dbconn, filepath.Join(to_add, f)) 
            if err != nil {
                t.Fatalf(err.Error())
            }
            if found {
                t.Fatal("found unexpected link that should have been purged")
            }
        }

        // Other links are still present. 
        found, err := searchForLink(dbconn, filepath.Join(to_add, "stuff", "metadata.json"), "characters.first", "akari")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatal("missing a link for a non-modified file")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add, "whee", "other.json"), "favorites", "biyori")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatal("missing a link for a non-modified file")
        }

        // All paths are present.
        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/stuff/metadata.json", "to_add/whee/other.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })

    t.Run("deleted directory", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        to_add := filepath.Join(tmp, "to_add")
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer os.RemoveAll(to_add)

        {
            comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if len(comments) > 0 {
                t.Fatalf("unexpected comments from the directory addition %v", comments)
            }
        }

        // Deleting the entire directory altogether.
        err = os.RemoveAll(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }

        comments, err := updateDirectories(tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) == 0 {
            t.Fatalf("unexpected lack of comments from updating")
        }

        // Check that every path has been flushed.
        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(all_paths) != 0 {
            t.Fatalf("found unexpected paths")
        }
    })

    t.Run("new", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        to_add := filepath.Join(tmp, "to_add")
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer os.RemoveAll(to_add)

        {
            comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if len(comments) > 0 {
                t.Fatalf("unexpected comments from the directory addition %v", comments)
            }
        }

        // Reorganizing stuff by modifying files, adding new files.
        err = os.Mkdir(filepath.Join(to_add, "mega"), 0700)
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.WriteFile(filepath.Join(to_add, "mega", "metadata.json"), []byte(`{ "melon": "honeydew", "flesh": "green" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.WriteFile(filepath.Join(to_add, "mega", "other.json"), []byte(`{ "melon": "winter", "flesh": "white" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        // Now actually running the update.
        comments, err := updateDirectories(tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from updating")
        }

        // Check that the insertion was done correctly.
        {
            self, err := user.Current()
            if err != nil {
                t.Fatalf(err.Error())
            }
            username := self.Username
            now := time.Now().Unix()

            for _, base := range []string{ "other.json", "metadata.json" } {
                var user, meta string
                var time int64
                err := dbconn.QueryRow("SELECT user, time, json_extract(metadata, '$') FROM paths WHERE path = ?", filepath.Join(to_add, "mega", base)).Scan(&user, &time, &meta)
                if err != nil {
                    t.Fatal(err)
                }

                if user != username {
                    t.Fatal("unexpected user for a new file")
                }
                if time < now {
                    t.Fatal("unexpected creation time for a new file")
                }
                if !strings.HasPrefix(meta, "{") || !strings.HasSuffix(meta, "}") {
                    t.Fatalf("unexpected metadata %q", meta)
                }
            }
        }

        // Check that the links are present for the new files. 
        found, err := searchForLink(dbconn, filepath.Join(to_add, "mega", "metadata.json"), "melon", "honeydew")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add, "mega", "other.json"), "melon", "winter")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        // All paths are present.
        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/mega/metadata.json", "to_add/mega/other.json", "to_add/metadata.json", "to_add/stuff/metadata.json", "to_add/stuff/other.json", "to_add/whee/other.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })

    t.Run("altogether", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        to_add := filepath.Join(tmp, "to_add")
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer os.RemoveAll(to_add)

        {
            comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if len(comments) > 0 {
                t.Fatalf("unexpected comments from the directory addition %v", comments)
            }
        }

        // Reorganizing stuff by modifying files, adding new files.
        time.Sleep(time.Second * 2)
        err = os.Remove(filepath.Join(to_add, "metadata.json"))
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.WriteFile(filepath.Join(to_add, "stuff", "metadata.json"), []byte(`{ "melon": "watermelon", "flesh": "red" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.Remove(filepath.Join(to_add, "stuff", "other.json"))
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.WriteFile(filepath.Join(to_add, "whee", "other.json"), []byte(`{ "melon": "canteloupe", "flesh": "orange" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.Mkdir(filepath.Join(to_add, "mega"), 0700)
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.WriteFile(filepath.Join(to_add, "mega", "metadata.json"), []byte(`{ "melon": "honeydew", "flesh": "green" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        err = os.WriteFile(filepath.Join(to_add, "mega", "other.json"), []byte(`{ "melon": "winter", "flesh": "white" }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        // Now actually running the update.
        comments, err := updateDirectories(tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from updating")
        }

        // Check that the links are present for the updated files. 
        found, err := searchForLink(dbconn, filepath.Join(to_add, "whee", "other.json"), "melon", "canteloupe")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add, "stuff", "metadata.json"), "melon", "watermelon")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        // Check that the links are present for the new files. 
        found, err = searchForLink(dbconn, filepath.Join(to_add, "mega", "metadata.json"), "melon", "honeydew")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        found, err = searchForLink(dbconn, filepath.Join(to_add, "mega", "other.json"), "melon", "winter")
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !found {
            t.Fatalf("failed to find the link")
        }

        // Check that all other links have been removed.
        ok := checkUpdatedLinks(dbconn, to_add, false)
        if !ok {
            t.Fatal("found an unexpected link after the update")
        }

        for _, f := range []string{ "metadata.json", filepath.Join("stuff", "other.json") } {
            found, err := hasAnyLink(dbconn, filepath.Join(to_add, f)) 
            if err != nil {
                t.Fatalf(err.Error())
            }
            if found {
                t.Fatal("found unexpected link that should have been purged")
            }
        }

        // List out the paths.
        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/mega/metadata.json", "to_add/mega/other.json", "to_add/stuff/metadata.json", "to_add/whee/other.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })

    t.Run("failure", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        to_add := filepath.Join(tmp, "to_add")
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer os.RemoveAll(to_add)

        comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        // Make a path invalid. 
        time.Sleep(time.Second * 2)
        err = os.WriteFile(filepath.Join(to_add, "whee", "other.json"), []byte(`{ melon }`), 0600)
        if err != nil {
            t.Fatalf(err.Error())
        }

        comments, err = updateDirectories(tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) != 1 || !strings.Contains(comments[0], "whee") {
            t.Fatalf("unexpected (lack of) comments from updating %v", comments)
        }

        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/metadata.json", "to_add/stuff/metadata.json", "to_add/stuff/other.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    })

    t.Run("canceled", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err = updateDirectories(tokr, dbconn, canceled, add_options)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel update request")
        }
    })
}

func TestCleanDatabase(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    // Mocking up some contents.
    for i := 0; i < 2; i++ {
        name := "first" 
        base := "metadata.json"
        if i == 1 {
            name = "second"
            base = "other.json"
        }

        to_add := filepath.Join(tmp, name)
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatal(err)
        }
        comments, err := addNewDirectory(to_add, []string{ base }, "myself", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }
    }

    // Deleting the first directory to create some garbage-collectable content.
    err = deleteDirectory(filepath.Join(tmp, "first"), dbconn, context.Background())
    if err != nil {
        t.Fatal(err)
    }

    // Checking that we get the expected results from our queries.
    for i := 0; i < 2; i++ {
        for _, token := range []string{ "aaron", "akari" } {
            res := dbconn.QueryRow("SELECT COUNT(*) FROM tokens WHERE token == '" + token + "'")
            var count int
            err := res.Scan(&count)
            if err != nil {
                t.Fatal(err)
            }
            if i == 0 {
                if count != 1 {
                    t.Errorf("expected one row to be present, got %v", count)
                }
            } else {
                if count != 0 {
                    t.Errorf("expected no rows to be present, got %v", count)
                }
            }
        }

        for _, field := range []string{ "foo", "anime" } {
            res := dbconn.QueryRow("SELECT COUNT(*) FROM fields WHERE field == '" + field + "'")
            var count int
            err := res.Scan(&count)
            if err != nil {
                t.Fatal(err)
            }
            if i == 0 {
                if count != 1 {
                    t.Errorf("expected one row to be present, got %v", count)
                }
            } else {
                if count != 0 {
                    t.Errorf("expected no rows to be present, got %v", count)
                }
            }
        }

        // As a control, we check that the other terms survive.
        for _, token := range []string{ "lamb", "chicken", "biyori" } {
            res := dbconn.QueryRow("SELECT COUNT(*) FROM tokens WHERE token == '" + token + "'")
            var count int
            err := res.Scan(&count)
            if err != nil {
                t.Fatal(err)
            }
            if count != 1 {
                t.Errorf("expected one row to be present, got %v", count)
            }
        }

        for _, field := range []string{ "variants", "category.nsfw" } {
            res := dbconn.QueryRow("SELECT COUNT(*) FROM fields WHERE field == '" + field + "'")
            var count int
            err := res.Scan(&count)
            if err != nil {
                t.Fatal(err)
            }
            if count != 1 {
                t.Errorf("expected one row to be present, got %v", count)
            }
        }

        err = cleanDatabase(dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
    }

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        err := cleanDatabase(dbconn, canceled)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel clean request")
        }
    })
}

func TestBackupDatabase(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    // Mocking up some contents.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    // Running the backup.
    backpath := filepath.Join(tmp, "db.sqlite3.backup")
    err = backupDatabase(backpath, dbconn, context.Background())
    if err != nil {
        t.Fatalf(err.Error())
    }

    dbconn2, err := sql.Open("sqlite", backpath)
    if err != nil {
        t.Fatalf(err.Error())
    }

    all_paths, err := listPaths(dbconn2, tmp)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if !equalStringArrays(all_paths, []string{ "to_add/metadata.json", "to_add/stuff/metadata.json", "to_add/stuff/other.json", "to_add/whee/other.json" }) {
        t.Fatalf("unexpected paths in the backup DB")
    }

    found, err := searchForLink(dbconn2, filepath.Join(to_add, "metadata.json"), "foo", "little")
    if err != nil {
        t.Fatalf(err.Error())
    }
    if !found {
        t.Fatalf("could not find expected link in the backup DB")
    }

    // Backup continues to work when there's an existing file.
    err = backupDatabase(backpath, dbconn, context.Background())
    if err != nil {
        t.Fatalf(err.Error())
    }

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        err := backupDatabase(backpath, dbconn, canceled)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel backup request")
        }
    })
}

func TestQueryTokens(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    // Mocking up some contents.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    extractSortedPaths := func(input []queryResult) []string {
        output := []string{}
        for _, x := range input {
            output = append(output, x.Path)
        }
        sort.Strings(output)
        return output
    }

    t.Run("basic text", func(t *testing.T) {
        res, err := queryTokens(&searchClause{ Type: "text", Text: "lamb" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "stuff/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("text with field", func(t *testing.T) {
        res, err := queryTokens(&searchClause{ Type: "text", Text: "lamb", Field: "variants" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "stuff/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("partial test", func(t *testing.T) {
        res, err := queryTokens(&searchClause{ Type: "text", Text: "*ar*", IsPattern: true }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "stuff/metadata.json", "stuff/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }

        res, err = queryTokens(&searchClause{ Type: "text", Text: "l?mb", IsPattern: true }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "stuff/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("search on numbers", func(t *testing.T) {
        res, err := queryTokens(&searchClause{ Type: "text", Text: "5", Field: "bar.cost" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }

        res, err = queryTokens(&searchClause{ Type: "text", Text: "10495" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "stuff/metadata.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("search on booleans", func(t *testing.T) {
        res, err := queryTokens(&searchClause{ Type: "text", Text: "false" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "whee/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }

        res, err = queryTokens(&searchClause{ Type: "text", Text: "true", Field: "category.iyashikei" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "whee/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("not (simple)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "not", 
                Child: &searchClause{ Type: "text", Text: "yuru" }, 
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "stuff/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("not (complex)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "not", 
                Child: &searchClause{ 
                    Type: "or", 
                    Children: []*searchClause{ 
                        &searchClause{ Type: "text", Text: "yuru" }, 
                        &searchClause{ Type: "text", Text: "lamb" },
                    },
                }, 
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 0 {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("not (partial)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "not", 
                Child: &searchClause{ Type: "text", Text: "*ar*", IsPattern: true },
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 1 || res[0].Path != filepath.Join(to_add, "whee/other.json") {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("not (nested)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "and",
                Children: []*searchClause{ 
                    &searchClause{ Type: "text", Text: "hoshino" },
                    &searchClause{ 
                        Type: "not", 
                        Child: &searchClause{ Type: "text", Text: "lamb" },
                    }, 
                },
            },
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "stuff/metadata.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("and (simple)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "and", 
                Children: []*searchClause{ 
                    &searchClause{ Type: "text", Text: "yuru" }, 
                    &searchClause{ Type: "text", Text: "non" },
                },
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "whee/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("or (simple)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "or", 
                Children: []*searchClause{ 
                    &searchClause{ Type: "text", Text: "yuru" },
                    &searchClause{ Type: "text", Text: "lamb" },
                },
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 4 {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("and (complex)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "and", 
                Children: []*searchClause{ 
                    &searchClause{ Type: "or", Children: []*searchClause{ &searchClause{ Type: "text", Text: "yuru" }, &searchClause{ Type: "text", Text: "border" } } },
                    &searchClause{ Type: "or", Children: []*searchClause{ &searchClause{ Type: "text", Text: "lamb" }, &searchClause{ Type: "text", Text: "non" } } },
                },
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(extractSortedPaths(res), []string{ filepath.Join(to_add, "metadata.json"), filepath.Join(to_add, "whee/other.json") }) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("or (complex)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "or", 
                Children: []*searchClause{ 
                    &searchClause{ Type: "text", Text: "border" },
                    &searchClause{ Type: "and", Children: []*searchClause{ &searchClause{ Type: "text", Text: "yuru" }, &searchClause{ Type: "text", Text: "non" } } },
                },
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "whee/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("or (partial)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "or", 
                Children: []*searchClause{ 
                    &searchClause{ Type: "text", Text: "aar*", IsPattern: true },
                    &searchClause{ Type: "text", Text: "ak*", IsPattern: true },
                },
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "stuff/metadata.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("or (field)", func(t *testing.T) {
        res, err := queryTokens(
            &searchClause{ 
                Type: "or", 
                Children: []*searchClause{ 
                    &searchClause{ Type: "text", Text: "lamb", Field: "bar.type" },
                    &searchClause{ Type: "text", Text: "yuru", Field: "anime" },
                },
            }, 
            dbconn,
            context.Background(),
            newQueryOptions(),
        )
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "stuff/metadata.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("user", func(t *testing.T) {
        self, err := user.Current()
        if err != nil {
            t.Fatalf(err.Error())
        }

        res, err := queryTokens(&searchClause{ Type: "user", User: self.Username }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 4 {
            t.Fatalf("search results are not as expected %v", res)
        }

        res, err = queryTokens(&searchClause{ Type: "user", User: self.Username + "2" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 0 {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("path", func(t *testing.T) {
        res, err := queryTokens(&searchClause{ Type: "path", Path: "*stuff*" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "stuff/metadata.json", "stuff/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }

        // Try a more complex pattern.
        res, err = queryTokens(&searchClause{ Type: "path", Path: "*/metadata.*json" }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalPathArrays(extractSortedPaths(res), []string{ "metadata.json", "stuff/metadata.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("time", func(t *testing.T) {
        res, err := queryTokens(&searchClause{ Type: "time", Time: 0 }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 0 {
            t.Fatalf("search results are not as expected %v", res)
        }

        res, err = queryTokens(&searchClause{ Type: "time", Time: time.Now().Unix() + 10 }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 4 {
            t.Fatalf("search results are not as expected %v", res)
        }

        res, err = queryTokens(&searchClause{ Type: "time", Time: time.Now().Unix() + 10, After: true }, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 0 {
            t.Fatalf("search results are not as expected %v", res)
        }
    })

    t.Run("scrolling", func(t *testing.T) {
        options := newQueryOptions()

        options.PageLimit = 2
        res, err := queryTokens(nil, dbconn, context.Background(), options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 2 {
            t.Fatalf("search results are not as expected")
        }

        collected := []string{}
        for _, x := range res {
            collected = append(collected, x.Path)
        }

        // Picking up from the last position.
        options.PageLimit = 100
        options.Scroll = &queryScroll{ Time: res[1].Time, Pid: res[1].Pid }
        res, err = queryTokens(nil, dbconn, context.Background(), options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 2 {
            t.Fatalf("search results are not as expected")
        }

        for _, x := range res {
            collected = append(collected, x.Path)
        }
        sort.Strings(collected)

        if !equalPathArrays(collected, []string{ "metadata.json", "stuff/metadata.json", "stuff/other.json", "whee/other.json" }, to_add) {
            t.Fatalf("search results are not as expected %v", collected)
        }

        // Checking that it works even after we've exhausted all records.
        options.PageLimit = 2
        options.Scroll = &queryScroll{ Time: res[1].Time, Pid: res[1].Pid }
        res, err = queryTokens(nil, dbconn, context.Background(), options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 0 {
            t.Fatalf("search results are not as expected")
        }
    })

    t.Run("full", func(t *testing.T) {
        res, err := queryTokens(nil, dbconn, context.Background(), newQueryOptions())
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 4 {
            t.Fatalf("search results are not as expected")
        }
    })

    t.Run("no metadata", func(t *testing.T) {
        options := newQueryOptions()

        // First, running a control.
        res, err := queryTokens(nil, dbconn, context.Background(), options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 4 {
            t.Fatalf("search results are not as expected")
        }
        for _, x := range res {
            if x.Metadata == nil {
                t.Error("unexpected nil metadata from default query")
            }
        }

        // Checking that metadata is skipped.
        options.IncludeMetadata = false
        res, err = queryTokens(nil, dbconn, context.Background(), options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(res) != 4 {
            t.Fatalf("search results are not as expected")
        }
        for _, x := range res {
            if x.Metadata != nil {
                t.Error("expected nil metadata from default query")
            }
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := queryTokens(nil, dbconn, canceled, newQueryOptions())
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel query request")
        }
    })
}

func TestRetrievePath(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    // Mocking up some contents.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    self, err := user.Current()
    if err != nil {
        t.Fatal(err)
    }

    t.Run("ok with metadata", func(t *testing.T) {
        res, err := retrievePath(filepath.Join(to_add, "metadata.json"), true, dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if res == nil {
            t.Fatal("should have found one matching path")
        }
        if res.User != self.Username || res.Time == 0 {
            t.Fatal("unexpected results for time and user")
        }
        meta := string(res.Metadata)
        if !strings.HasPrefix(meta, "{") || !strings.HasSuffix(meta, "}") {
            t.Fatal("expected a JSON object in the metadata")
        }
    })

    t.Run("ok without metadata", func(t *testing.T) {
        res, err := retrievePath(filepath.Join(to_add, "metadata.json"), false, dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if res == nil {
            t.Fatal("should have found one matching path")
        }
        if res.User != self.Username || res.Time == 0 {
            t.Fatal("unexpected results for time and user")
        }
        if res.Metadata != nil {
            t.Fatal("expected metadata to be ignored")
        }
    })

    t.Run("missing", func(t *testing.T) {
        res, err := retrievePath("missing.json", false, dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if res != nil {
            t.Fatal("should not have matched anything")
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := retrievePath("foobar.json", false, dbconn, canceled)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel path retrieval request")
        }
    })
}

func TestListRegisteredDirectories(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    // Mocking up some contents.
    for _, name := range []string{ "foo", "bar" } {
        to_add := filepath.Join(tmp, name)
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatalf(err.Error())
        }

        comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, name + "_user", tokr, dbconn, context.Background(), add_options)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }
    }

    t.Run("basic", func(t *testing.T) {
        options := listRegisteredDirectoriesOptions{}
        out, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 {
            t.Fatal("should have found two matching paths")
        }

        for i := 0; i < 2; i++ {
            name := "bar"
            if i == 1 {
                name = "foo"
            }

            if out[i].User != name + "_user" || filepath.Base(out[i].Path) != name || out[i].Time == 0 {
                t.Fatalf("unexpected entry for path %d; %v", i, out[i])
            }

            var payload []string
            err = json.Unmarshal(out[0].Names, &payload)
            if err != nil {
                t.Fatalf("failed to unmashal names; %v", string(out[0].Names))
            }

            if len(payload) != 2 || payload[0] != "metadata.json" || payload[1] != "other.json" {
                t.Fatalf("unexpected value for names; %v", payload)
            }
        }
    })

    t.Run("filtered on user", func(t *testing.T) {
        options := listRegisteredDirectoriesOptions{}
        desired := "bar_user"
        options.User = &desired
        out, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 1 {
            t.Fatal("should have found one matching path")
        }
        if out[0].User != desired {
            t.Fatalf("unexpected entry %v", out[0])
        }

        desired = "bar_user2"
        options.User = &desired
        out, err = listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 0 {
            t.Fatal("should have found no matching paths")
        }
    })

    t.Run("filtered on contains_path", func(t *testing.T) {
        options := listRegisteredDirectoriesOptions{}

        desired := filepath.Join(tmp, "bar")
        options.ContainsPath = &desired

        out, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 1 {
            t.Fatal("should have found one matching path")
        }
        if out[0].Path != desired {
            t.Fatalf("unexpected entry %v", out[0])
        }

        desired = filepath.Join(filepath.Dir(tmp))
        options.ContainsPath = &desired
        out, err = listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 0 {
            t.Fatal("should have found no matching paths")
        }
    })

    t.Run("filtered on has_prefix", func(t *testing.T) {
        options := listRegisteredDirectoriesOptions{}
        options.PathPrefix = &tmp

        out, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 {
            t.Fatal("should have found two matching paths")
        }

        absent := tmp + "_asdasd"
        options.PathPrefix = &absent
        out, err = listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 0 {
            t.Fatal("should have found no matching paths")
        }
    })

    t.Run("filtered on within_path", func(t *testing.T) {
        // Basic checks to see what we pick up or don't.
        {
            options := listRegisteredDirectoriesOptions{}
            options.WithinPath = &tmp

            out, err := listRegisteredDirectories(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            if len(out) != 2 {
                t.Fatal("should have found two matching paths")
            }

            absent := tmp + "_asdasd"
            options.WithinPath = &absent
            out, err = listRegisteredDirectories(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            if len(out) != 0 {
                t.Fatal("should have found no matching paths")
            }
        }

        // Adding a 'fo' directory, and checking that we don't pick up 'foo'.
        {
            to_add := filepath.Join(tmp, "fo")
            err = mockDirectory(to_add)
            if err != nil {
                t.Fatalf(err.Error())
            }
            comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "fo_user", tokr, dbconn, context.Background(), add_options)
            if err != nil {
                t.Fatalf(err.Error())
            }
            defer deleteDirectory(to_add, dbconn, context.Background())
            if len(comments) > 0 {
                t.Fatalf("unexpected comments from the directory addition %v", comments)
            }

            options := listRegisteredDirectoriesOptions{}
            options.WithinPath = &to_add
            out, err := listRegisteredDirectories(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            if len(out) != 1 || filepath.Base(out[0].Path) != "fo" {
                t.Fatal("should have found a single matching path")
            }

            // Checking foo as a control.
            control := filepath.Join(tmp, "foo")
            options.WithinPath = &control
            out, err = listRegisteredDirectories(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            if len(out) != 1 || filepath.Base(out[0].Path) != "foo" {
                t.Fatal("should have found a single matching path")
            }
        }
    })

    t.Run("filtered on existence", func(t *testing.T) {
        // Registering a directory that we delete immediately.
        to_add := filepath.Join(tmp, "transient")
        {
            err = mockDirectory(to_add)
            if err != nil {
                t.Fatalf(err.Error())
            }
            comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
            if err != nil {
                t.Fatalf(err.Error())
            }
            defer deleteDirectory(to_add, dbconn, context.Background()) // resetting the DB for subsequent subtests.
            if len(comments) > 0 {
                t.Fatalf("unexpected comments from the directory addition %v", comments)
            }
            err = os.RemoveAll(to_add)
            if err != nil {
                t.Fatal(err)
            }
        }

        exists := "true"
        options := listRegisteredDirectoriesOptions{}
        options.Exists = &exists
        out, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 || filepath.Base(out[0].Path) == "transient" || filepath.Base(out[1].Path) == "transient" {
            t.Errorf("should have found 4 matching paths; %v", out)
        }

        exists = "false"
        options.Exists = &exists
        out, err = listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 1 || filepath.Base(out[0].Path) != "transient" {
            t.Errorf("should have found 2 matching paths; %v", out)
        }

        // Repeating this options after adding a file there; this is not a
        // directory and so the path is still considered absent.
        err = os.WriteFile(to_add, []byte{}, 0644)
        if err != nil {
            t.Fatal(err)
        }
        out, err = listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 1 || filepath.Base(out[0].Path) != "transient" {
            t.Errorf("should have found 2 matching paths; %v", out)
        }

        // Checking that 'any' queries work as expected.
        exists = "any"
        options.Exists = &exists
        out, err = listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 3 {
            t.Error("should have found 3 matching paths")
        }

        options.Exists = nil
        out, err = listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 3 {
            t.Error("should have found 3 matching paths")
        }
    })

    t.Run("scroll", func(t *testing.T) {
        options := listRegisteredDirectoriesOptions{ PageLimit: 1 }
        out, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 1 {
            t.Fatal("should have found one path")
        }

        options.Scroll = &listRegisteredDirectoriesScroll{ Did: out[0].Did, Time: out[0].Time }
        out2, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out2) != 1 {
            t.Fatal("should have found one path")
        }
        if out[0].Path == out2[0].Path {
            t.Error("should have gotten different paths from the scroll")
        }

        options.Scroll = &listRegisteredDirectoriesScroll{ Did: out2[0].Did, Time: out2[0].Time }
        out3, err := listRegisteredDirectories(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out3) != 0 {
            t.Fatal("should have found no more paths at the end of the scroll")
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := listRegisteredDirectories(dbconn, canceled, listRegisteredDirectoriesOptions{})
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel path retrieval request")
        }
    })
}

func TestIsDirectoryRegistered(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 2 }

    // Mocking up some contents.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    t.Run("present", func(t *testing.T) {
        found, err := isDirectoryRegistered(to_add, dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if !found {
            t.Fatal("should have found one matching path")
        }

        found, err = isDirectoryRegistered(filepath.Join(to_add, "stuff"), dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if !found {
            t.Fatal("should have found one matching path")
        }

        found, err = isDirectoryRegistered(to_add, dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if !found {
            t.Fatal("should have found one matching path")
        }
    })

    t.Run("absent", func(t *testing.T) {
        found, err := isDirectoryRegistered(tmp, dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if found {
            t.Fatal("should not have found a matching path")
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := isDirectoryRegistered(tmp, dbconn, canceled)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel path retrieval request")
        }
    })
}

func TestFetchRegisteredDirectoryNames(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 10 }

    to_add := filepath.Join(tmp, "liella")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "aaron", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    t.Run("basic", func(t *testing.T) {
        out, err := fetchRegisteredDirectoryNames(to_add, dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 || out[0] != "metadata.json" || out[1] != "other.json" {
            t.Fatalf("unexpected names for the registered directory")
        }

        out, err = fetchRegisteredDirectoryNames(filepath.Join(tmp, "margarete"), dbconn, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 0 {
            t.Fatalf("unexpected names for a non-registered directory")
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := fetchRegisteredDirectoryNames(to_add, dbconn, canceled)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel directory names request")
        }
    })
}

func TestInitializeReadOnlyDatabase(t *testing.T) {
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

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatal(err)
    }

    add_options := &addDirectoryContentsOptions{ Concurrency: 10 }

    // Mocking up some contents.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatal(err)
    }

    // Checking that we can't write through a read-only connection.
    ro_dbconn, err := initializeReadOnlyDatabase(dbpath)
    if err != nil {
        t.Fatal(err)
    }
    defer ro_dbconn.Close()

    _, err = addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, ro_dbconn, context.Background(), add_options)
    if err == nil || strings.Index(err.Error(), "read-only") >= 0 {
        t.Error("expected a failure to modify the database through read-only connection")
    }

    // Adding it as a negative control.
    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "myself", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatal(err)
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    // Checking that we can still read from this.
    found, err := isDirectoryRegistered(to_add, ro_dbconn, context.Background())
    if err != nil {
        t.Fatal(err)
    }
    if !found {
        t.Error("failed to find the newly added directory through a read-only connection")
    }
}

func TestTokenizePathField(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    dbpath := filepath.Join(tmp, "db.sqlite3")

    validateTokenPaths := func(dbconn *sql.DB, t *testing.T, tokens []string, field string, dirname string) {
        expected_paths := []string{
            dirname + "/metadata.json",
            dirname + "/stuff/metadata.json",
        }

        for _, token := range tokens {
            rows, err := dbconn.Query(`SELECT paths.path FROM links
INNER JOIN paths ON paths.pid = links.pid
INNER JOIN tokens ON tokens.tid = links.tid
INNER JOIN fields ON fields.fid = links.fid
WHERE tokens.token = ? AND fields.field = ?`, 
                token,
                field,
            )
            if err != nil {
                t.Fatal(err)
            }

            all_paths := []string{}
            for rows.Next() {
                var path string
                err = rows.Scan(&path)
                if err != nil {
                    t.Fatal(err)
                }
                rel, err := filepath.Rel(tmp, path)
                if err != nil {
                    t.Fatal(err)
                }
                all_paths = append(all_paths, rel)
            }

            sort.Strings(all_paths)
            if !equalStringArrays(all_paths, expected_paths) {
                t.Errorf("expected path components to be correctly tokenized; %v", all_paths)
            }
        }
    }

    t.Run("add", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        dirname := "asdasd_qwerty"
        to_add := filepath.Join(tmp, dirname)
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatal(err)
        }

        // Checking that path is not tokenized by default.
        cur_add_options := &addDirectoryContentsOptions{ Concurrency: 2 }
        comments, err := addNewDirectory(to_add, []string{ "metadata.json" }, "myself", tokr, dbconn, context.Background(), cur_add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        found, err := listPaths(dbconn, to_add)
        if err != nil {
            t.Fatal(err)
        }
        if len(found) != 2 {
            t.Error("should have indexed exactly two documents")
        }

        res := dbconn.QueryRow(`SELECT COUNT(*) FROM links
INNER JOIN paths ON paths.pid = links.pid
INNER JOIN tokens ON tokens.tid = links.tid
INNER JOIN fields ON fields.fid = links.fid
WHERE tokens.token = ?`, 
            "asdasd" ,
        )
        count := 0
        err = res.Scan(&count)
        if err != nil {
            t.Fatal(err)
        }
        if count > 0 {
            t.Error("expected no search hits for tokenized path")
        }

        err = deleteDirectory(to_add, dbconn, context.Background()) 
        if err != nil {
            t.Fatal(err)
        }

        // Trying again with path tokenization enabled.
        cur_add_options.PathField = "__path__"
        comments, err = addNewDirectory(to_add, []string{ "metadata.json" }, "myself", tokr, dbconn, context.Background(), cur_add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        validateTokenPaths(dbconn, t, []string{ "asdasd", "qwerty" }, "__path__", "asdasd_qwerty")
    })

    t.Run("update", func(t *testing.T) {
        dbconn, err := initializeDatabase(dbpath)
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer dbconn.Close()
        defer os.Remove(dbpath)

        dirname := filepath.Join("alpha", "bravo")
        to_add := filepath.Join(tmp, dirname)
        err = os.MkdirAll(to_add, 0755)
        if err != nil {
            t.Fatal(err)
        }

        cur_add_options := &addDirectoryContentsOptions{ Concurrency: 2, PathField: "__blah__" }
        comments, err := addNewDirectory(to_add, []string{ "metadata.json" }, "myself", tokr, dbconn, context.Background(), cur_add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        found, err := listPaths(dbconn, to_add)
        if err != nil {
            t.Fatal(err)
        }
        if len(found) > 0 {
            t.Error("should not have found anything in an empty directory")
        }

        // Updating with path tokenization enabled.
        err = mockDirectory(to_add)
        if err != nil {
            t.Fatal(err)
        }

        comments, err = updateDirectories(tokr, dbconn, context.Background(), cur_add_options)
        if err != nil {
            t.Fatal(err)
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        validateTokenPaths(dbconn, t, []string{ "alpha", "bravo" }, "__blah__", "alpha/bravo")
    })
}

func TestListFields(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 10 }

    to_add := filepath.Join(tmp, "liella")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "aaron", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    t.Run("simple", func (t *testing.T) {
        options := listFieldsOptions{}
        out, err := listFields(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        expected := map[string]bool{ "bar.breed": false, "favorites": false, "category.iyashikei": false }
        increasing := true
        last := ""
        for _, x := range out {
            _, ok := expected[x.Field]
            if ok {
                expected[x.Field] = true
            }
            if last != "" && x.Field <= last {
                increasing = false
            }
            last = x.Field
        }

        for f, val := range expected {
            if !val {
                t.Errorf("failed to find '%s' in the fields; %v", f, out)
            }
        }
        if !increasing {
            t.Errorf("field should be sorted in increasing order; %v", out)
        }
    })

    t.Run("count", func (t *testing.T) {
        options := listFieldsOptions{ Count: true }
        out, err := listFields(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        expected := map[string]bool{ "bar.breed": false, "favorites": false, "category.iyashikei": false }
        increasing := true
        last := ""
        for _, x := range out {
            _, ok := expected[x.Field]
            if ok {
                if *(x.Count) != 1 {
                    t.Errorf("expected a count of 1 for %s, got %d instead", x.Field, *(x.Count))
                }
                expected[x.Field] = true
            }
            if last != "" && x.Field <= last {
                increasing = false
            }
            last = x.Field
        }

        for f, val := range expected {
            if !val {
                t.Errorf("failed to find '%s' in the fields; %v", f, out)
            }
        }
        if !increasing {
            t.Errorf("field should be sorted in increasing order; %v", out)
        }
    })

    t.Run("scroll", func (t *testing.T) {
        options := listFieldsOptions{ PageLimit: 2 }
        out, err := listFields(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        // Page limit works as expected.
        expected := map[string]bool{}
        if len(out) != 2 {
            t.Error("expected exactly two fields when limiting to 2")
        }
        for _, x := range out {
            expected[x.Field] = true
        }
        for _, field := range []string{ "anime", "bar.breed" } {
            if _, ok := expected[field]; !ok {
                t.Errorf("expected the '%s' field; %v", field, expected)
            }
        }

        // Scroll picks up from where we were before.
        options.Scroll = &listFieldsScroll{ Field: out[len(out) - 1].Field }
        out, err = listFields(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 {
            t.Error("expected exactly two fields when limiting to 2")
        }
        for _, x := range out {
            if _, ok := expected[x.Field]; ok {
                t.Errorf("unexpected duplicate field %s", x.Field)
            }
            expected[x.Field] = true
        }
        for _, field := range []string{ "bar.cost", "bar.type" } {
            if _, ok := expected[field]; !ok {
                t.Errorf("expected the '%s' field; %v", field, expected)
            }
        }

        // Works okay with count=true.
        options.Count = true
        options.Scroll = &listFieldsScroll{ Field: out[len(out) - 1].Field }
        out, err = listFields(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 {
            t.Error("expected exactly two fields when limiting to 2")
        }
        for _, x := range out {
            if _, ok := expected[x.Field]; ok {
                t.Errorf("unexpected duplicate field; %s", x.Field)
            }
            if *(x.Count) != 1 {
                t.Errorf("expected a count of 1 for %s, got %d instead", x.Field, *(x.Count))
            }
            expected[x.Field] = true
        }
        for _, field := range []string{ "category.nsfw", "category.iyashikei" } {
            if _, ok := expected[field]; !ok {
                t.Errorf("expected the '%s' field; %v", field, expected)
            }
        }
    })

    t.Run("pattern", func (t *testing.T) {
        pattern := "category.*"
        options := listFieldsOptions{ Pattern: &pattern }
        out, err := listFields(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        expected := map[string]bool{ "category.iyashikei": false, "category.nsfw": false }
        for _, x := range out {
            if _, ok := expected[x.Field]; !ok {
                t.Errorf("unexpected field after pattern restriction %v", x.Field)
            }
            expected[x.Field] = true
        }
        for f, val := range expected {
            if !val {
                t.Errorf("failed to find '%s' in the fields; %v", f, out)
            }
        }

        // Works in conjunction with count = true.
        options.Count = true
        out, err = listFields(dbconn, context.Background(), options)
        expected = map[string]bool{ "category.iyashikei": false, "category.nsfw": false }
        for _, x := range out {
            _, ok := expected[x.Field]
            if !ok {
                t.Errorf("unexpected field after pattern restriction %v", x.Field)
            }
            if *(x.Count) != 1 {
                t.Errorf("expected a count of 1 for %s, got %d instead", x.Field, *(x.Count))
            }
            expected[x.Field] = true
        }
        for f, val := range expected {
            if !val {
                t.Errorf("failed to find '%s' in the fields; %v", f, out)
            }
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := listFields(dbconn, canceled, listFieldsOptions{})
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel list fields request")
        }
    })
}

func TestListTokens(t *testing.T) {
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

    add_options := &addDirectoryContentsOptions{ Concurrency: 10 }

    to_add := filepath.Join(tmp, "liella")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err := addNewDirectory(to_add, []string{ "metadata.json", "other.json" }, "aaron", tokr, dbconn, context.Background(), add_options)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    t.Run("simple", func (t *testing.T) {
        options := listTokensOptions{}
        out, err := listTokens(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        found := map[string]bool{}
        increasing := true
        last := ""
        for _, x := range out {
            if _, ok := found[x.Token]; ok {
                t.Errorf("duplicate token returned %s", x.Token)
            } else {
                found[x.Token] = true
            }
            if last != "" && x.Token <= last {
                increasing = false
            }
            last = x.Token
        }

        for _, expected := range []string{ "aaron", "hoshino", "biyori" } {
            if _, ok := found[expected]; !ok {
                t.Errorf("failed to find '%s' in the tokens; %v", expected, out)
            }
        }
        if !increasing {
            t.Errorf("token should be sorted in increasing order; %v", out)
        }
    })

    t.Run("count", func (t *testing.T) {
        options := listTokensOptions{ Count: true }
        out, err := listTokens(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        found := map[string]int64{}
        increasing := true
        last := ""
        for _, x := range out {
            if _, ok := found[x.Token]; ok {
                t.Errorf("duplicate token %s", x.Token)
            } else {
                found[x.Token] = *(x.Count)
            }
            if last != "" && x.Token <= last {
                increasing = false
                t.Log(x.Token) 
            }
            last = x.Token
        }

        expected := map[string]int64{ "akari": 1, "yuru": 2, "lamb": 2 }
        for tok, val := range expected {
            n, ok := found[tok]
            if !ok {
                t.Errorf("failed to find '%s' in the tokens; %v", tok, out)
            }
            if n != val {
                t.Errorf("expected a count of %d for %s, got %d instead", val, tok, n)
            }
        }
        if !increasing {
            t.Errorf("token should be sorted in increasing order; %v", out)
        }
    })

    t.Run("scroll", func (t *testing.T) {
        options := listTokensOptions{ PageLimit: 2 }
        out, err := listTokens(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        // Page limit works as found.
        found := map[string]bool{}
        if len(out) != 2 {
            t.Error("found exactly two tokens when limiting to 2")
        }
        for _, x := range out {
            found[x.Token] = true
        }
        for _, token := range []string{ "1", "10495" } {
            if _, ok := found[token]; !ok {
                t.Errorf("found the '%s' token; %v", token, found)
            }
        }

        // Scroll picks up from where we were before.
        options.Scroll = &listTokensScroll{ Token: out[len(out) - 1].Token }
        out, err = listTokens(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 {
            t.Error("found exactly two tokens when limiting to 2")
        }
        for _, x := range out {
            _, ok := found[x.Token]
            if ok {
                t.Error("unfound duplicate token")
            }
            found[x.Token] = true
        }
        for _, token := range []string{ "5", "a" } {
            if _, ok := found[token]; !ok {
                t.Errorf("found the '%s' token; %v", token, found)
            }
        }

        // Works okay with count=true.
        options.Count = true
        options.Scroll = &listTokensScroll{ Token: out[len(out) - 1].Token }
        out, err = listTokens(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }
        if len(out) != 2 {
            t.Error("found exactly two tokens when limiting to 2")
        }
        for _, x := range out {
            _, ok := found[x.Token]
            if ok {
                t.Errorf("unfound duplicate token; %v", x.Token)
            }
            if *(x.Count) != 1 {
                t.Errorf("found a count of 1 for %s, got %d instead", x.Token, *(x.Count))
            }
            found[x.Token] = true
        }
        for _, token := range []string{ "aaron", "akari" } {
            if _, ok := found[token]; !ok {
                t.Errorf("found the '%s' token; %v", token, found)
            }
        }
    })

    t.Run("pattern", func (t *testing.T) {
        pattern := "a?*"
        options := listTokensOptions{ Pattern: &pattern }
        out, err := listTokens(dbconn, context.Background(), options)
        if err != nil {
            t.Fatal(err)
        }

        expected := []string{ "aaron", "akari", "akaza" }
        if len(out) != len(expected) {
            t.Fatalf("unexpected length of output %v", out)
        }
        for i, x := range out {
            if x.Token != expected[i] {
                t.Errorf("unexpected field after pattern restriction %v", x.Token)
            }
        }

        // Works in conjunction with count = true.
        options.Count = true
        out, err = listTokens(dbconn, context.Background(), options)
        for i, x := range out {
            if x.Token != expected[i] {
                t.Errorf("unexpected field after pattern restriction %v", x.Token)
            }
            if *(x.Count) != 1 {
                t.Errorf("expected a count of 1 for %s, got %d instead", x.Token, *(x.Count))
            }
        }
    })

    t.Run("field", func (t *testing.T) {
        t.Run("wildcard", func(t *testing.T) {
            field := "characters*"
            options := listTokensOptions{ Field: &field }
            out, err := listTokens(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            expected := []string{ "akari", "akaza", "hoshino", "kyouko" }
            if len(out) != len(expected) {
                t.Fatalf("unexpected length of output %v", out)
            }
            for i, x := range out {
                if x.Token != expected[i] {
                    t.Errorf("unexpected field after pattern restriction %v", x.Token)
                }
            }
        })

        t.Run("no wildcard", func(t *testing.T) {
            field := "characters.first"
            options := listTokensOptions{ Field: &field }
            out, err := listTokens(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            expected := []string{ "akari", "hoshino" }
            if len(out) != len(expected) {
                t.Fatalf("unexpected length of output %v", out)
            }
            for i, x := range out {
                if x.Token != expected[i] {
                    t.Errorf("unexpected field after pattern restriction %v", x.Token)
                }
            }
        })

        t.Run("count", func(t *testing.T) {
            field := "characters*"
            options := listTokensOptions{ Field: &field, Count: true }
            out, err := listTokens(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            expected := []string{ "akari", "akaza", "hoshino", "kyouko" }
            if len(out) != len(expected) {
                t.Fatalf("unexpected length of output %v", out)
            }
            for i, x := range out {
                if x.Token != expected[i] {
                    t.Errorf("unexpected field after pattern restriction %v", x.Token)
                }
                if *(x.Count) != 1 {
                    t.Errorf("expected a count of 1 for %s, got %d instead", x.Token, *(x.Count))
                }
            }
        })

        t.Run("multiple options", func(t *testing.T) {
            field := "*a*i*e" // match 'anime' and 'favorites'
            pattern := "yuru*" // match yuru only
            options := listTokensOptions{ Field: &field, Pattern: &pattern } // multiple WHERE conditions being applied here.
            out, err := listTokens(dbconn, context.Background(), options)
            if err != nil {
                t.Fatal(err)
            }
            expected := []string{ "yuru" } // check that 'yuru' only appears once, despite the inner join.
            if len(out) != len(expected) {
                t.Fatalf("unexpected length of output %v", out)
            }
            for i, x := range out {
                if x.Token != expected[i] {
                    t.Errorf("unexpected field after pattern restriction %v", x.Token)
                }
            }
        })
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := listTokens(dbconn, canceled, listTokensOptions{})
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("failed to cancel list fields request")
        }
    })
}
