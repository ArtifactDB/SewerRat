package main

import (
    "testing"
    "os"
    "path/filepath"
    "sort"
    "os/user"
    "time"
    "strings"
    "database/sql"
)

func TestInitializeDatabase(t *testing.T) {
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

    if _, err := os.Stat(dbpath); err != nil {
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
    if !equalStringArrays(collected, []string{ "fields", "links", "paths", "tokens" }) {
        t.Fatalf("not all tables were correctly initialized")
    }
}

func TestAddDirectorySimple(t *testing.T) {
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

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf(err.Error())
    }
    username := self.Username
    now := time.Now().Unix()

    comments, err := addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true }, tokr)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
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
        rows, err := dbconn.Query("SELECT paths.path, fields.field, tokens.token FROM links LEFT JOIN paths ON paths.pid = links.pid LEFT JOIN fields ON fields.fid = links.fid LEFT JOIN tokens ON tokens.tid = links.tid")
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer rows.Close()

        has_aaron := false
        has_leicester := false
        has_hoshino := false
        for rows.Next() {
            var path, field, token string
            err = rows.Scan(&path, &field, &token)
            if err != nil {
                t.Fatalf(err.Error())
            }

            rel, err := filepath.Rel(to_add, path)
            if err != nil {
                t.Fatalf(err.Error())
            }

            if rel == "metadata.json" {
                if field == "foo" && token == "aaron" {
                    has_aaron = true
                } else if field == "bar.breed" && token == "leicester" {
                    has_leicester = true
                }
            } else if rel == "stuff/metadata.json" {
                if field == "characters.first" && token == "hoshino" {
                    has_hoshino = true
                } 
            }
        }

        if !has_aaron || !has_leicester || !has_hoshino {
            t.Fatalf("failed to find the expected tokens")
        }
    }
}

func listPaths(dbconn * sql.DB, scratch string) ([]string, error) {
    rows, err := dbconn.Query("SELECT path FROM paths")
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    all_paths := []string{}
    for rows.Next() {
        var path string
        err = rows.Scan(&path)
        if err != nil {
            return nil, err
        }

        rel, err := filepath.Rel(scratch, path)
        if err != nil {
            return nil, err
        }
        all_paths = append(all_paths, rel)
    }

    sort.Strings(all_paths)
    return all_paths, nil
}

func TestAddDirectoryMultipleBase(t *testing.T) {
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

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    // Works with multiple JSON targets.
    comments, err := addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true, "other.json": true }, tokr)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    all_paths, err := listPaths(dbconn, tmp)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if !equalStringArrays(all_paths, []string{ "to_add/metadata.json", "to_add/stuff/metadata.json", "to_add/stuff/other.json", "to_add/whee/other.json" }) {
        t.Fatalf("unexpected paths in the index %v", all_paths)
    }
}

func TestAddDirectoryMultipleCalls(t *testing.T) {
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

    to_add2 := filepath.Join(tmp, "to_add2")
    err = mockDirectory(to_add2)
    if err != nil {
        t.Fatalf(err.Error())
    }

    // Works with multiple JSON directories.
    comments, err := addDirectory(dbconn, to_add, map[string]bool{ "other.json": true }, tokr)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    comments, err = addDirectory(dbconn, to_add2, map[string]bool{ "metadata.json": true }, tokr)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    all_paths, err := listPaths(dbconn, tmp)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if !equalStringArrays(all_paths, []string{ "to_add/stuff/other.json", "to_add/whee/other.json", "to_add2/metadata.json", "to_add2/stuff/metadata.json" }) {
        t.Fatalf("unexpected paths in the index %v", all_paths)
    }

    // Recalling on an existing directory wipes out existing entries and replaces it.
    comments, err = addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true }, tokr)
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
}

func TestAddDirectoryFailures(t *testing.T) {
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

    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    // Making up an invalid file.
    err = os.WriteFile(filepath.Join(to_add, "stuff", "metadata.json"), []byte("{ asdasd }"), 0644)
    if err != nil {
        t.Fatalf(err.Error())
    }

    tokr, err := newUnicodeTokenizer(false)
    if err != nil {
        t.Fatalf(err.Error())
    }

    // Reports the error correctly.
    comments, err := addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true, "other.json": true }, tokr)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) != 1 || !strings.Contains(comments[0], "stuff") {
        t.Fatalf("unexpected (lack of) comments from the directory addition %v", comments)
    }
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

    comments, err := addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true, "other.json": true }, tokr)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    comments, err = addDirectory(dbconn, to_add2, map[string]bool{ "other.json": true }, tokr)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if len(comments) > 0 {
        t.Fatalf("unexpected comments from the directory addition %v", comments)
    }

    // Deleting the first directory; this does not affect the second directory.
    err = deleteDirectory(dbconn, to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    all_paths, err := listPaths(dbconn, tmp)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if !equalStringArrays(all_paths, []string{ "to_add2/stuff/other.json", "to_add2/whee/other.json"}) {
        t.Fatalf("unexpected paths in the index %v", all_paths)
    }

    {
        rows, err := dbconn.Query("SELECT paths.path, fields.field, tokens.token FROM links LEFT JOIN paths ON paths.pid = links.pid LEFT JOIN fields ON fields.fid = links.fid LEFT JOIN tokens ON tokens.tid = links.tid")
        if err != nil {
            t.Fatalf(err.Error())
        }
        defer rows.Close()

        has_chicken1 := false
        has_chicken2 := false
        has_biyori := false
        for rows.Next() {
            var path, field, token string
            err = rows.Scan(&path, &field, &token)
            if err != nil {
                t.Fatalf(err.Error())
            }

            if !strings.HasPrefix(path, to_add2 + "/") {
                t.Fatalf("detected unexpected path after deletion %q", path)
            }
            if token == "chicken" {
                if field == "name" {
                    has_chicken1 = true
                } else if field == "recipe" {
                    has_chicken2 = true
                }
            } else if token == "biyori" {
                if field == "favorites" {
                    has_biyori = true
                }
            }
        }

        if !has_chicken1 && !has_chicken2 && !has_biyori {
            t.Fatalf("could not find expected tokens after deletion")
        }
    }
}
