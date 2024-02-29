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

func searchForLink(dbconn *sql.DB, path, field, token string) (bool, error) {
    count := -1

    err := dbconn.QueryRow(`
SELECT COUNT(links.pid) FROM links
LEFT JOIN paths ON paths.pid = links.pid 
LEFT JOIN fields ON fields.fid = links.fid
LEFT JOIN tokens ON tokens.tid = links.tid
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
        // Checking that only the second directory's links are present.
        rows, err := dbconn.Query("SELECT paths.path FROM links LEFT JOIN paths ON paths.pid = links.pid")
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
}

func TestUpdatePaths(t *testing.T) {
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

    // Mocking up the first directory.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
    if err != nil {
        t.Fatalf(err.Error())
    }

    checkLinkCohort := func(should_find bool) bool {
        found, err := searchForLink(dbconn, filepath.Join(to_add, "stuff", "metadata.json"), "anime", "yuru")
        if err != nil {
            t.Fatalf(err.Error())
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

        found, err = searchForLink(dbconn, filepath.Join(to_add, "stuff", "other.json"), "variants", "lamb")
        if err != nil {
            t.Fatalf(err.Error())
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

    getTime := func(path string) int64 {
        var val int64
        err := dbconn.QueryRow("SELECT time FROM paths WHERE path = ?", path).Scan(&val)
        if err != nil {
            t.Fatalf(err.Error())
        }
        return val
    }

    var oldtime1, oldtime2 int64
    {
        comments, err := addDirectory(dbconn, to_add, map[string]bool{ "metadata.json": true, "other.json": true }, tokr)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from the directory addition %v", comments)
        }

        if !checkLinkCohort(true) {
            t.Fatalf("failed to find links that should be there")
        }

        oldtime1 = getTime(filepath.Join(to_add, "stuff", "metadata.json"))
        oldtime2 = getTime(filepath.Join(to_add, "whee", "other.json"))
    }

    // Reorganizing stuff and then updating the path.
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

    { 
        comments, err := updatePaths(dbconn, tokr)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if len(comments) > 0 {
            t.Fatalf("unexpected comments from updating")
        }

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
        if oldtime1 >= getTime(filepath.Join(to_add, "stuff", "metadata.json")) {
            t.Fatalf("time was not updated properly")
        }
        if oldtime2 >= getTime(filepath.Join(to_add, "whee", "other.json")) {
            t.Fatalf("time was not updated properly")
        }

        // Check that other links and files were deleted.
        if !checkLinkCohort(false) {
            t.Fatalf("found links that shouldn't be there")
        }

        all_paths, err := listPaths(dbconn, tmp)
        if err != nil {
            t.Fatalf(err.Error())
        }
        if !equalStringArrays(all_paths, []string{ "to_add/stuff/metadata.json", "to_add/whee/other.json" }) {
            t.Fatalf("unexpected paths in the index %v", all_paths)
        }
    }
}

func TestUpdatePathsFailure(t *testing.T) {
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

    // Mocking up the first directory.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
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

    // Make a path invalid. 
    time.Sleep(time.Second * 2)
    err = os.WriteFile(filepath.Join(to_add, "whee", "other.json"), []byte(`{ melon }`), 0600)
    if err != nil {
        t.Fatalf(err.Error())
    }

    comments, err = updatePaths(dbconn, tokr)
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

    // Mocking up some contents.
    to_add := filepath.Join(tmp, "to_add")
    err = mockDirectory(to_add)
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

    // Running the backup.
    backpath := filepath.Join(tmp, "db.sqlite3.backup")
    err = backupDatabase(dbconn, backpath)
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
    err = backupDatabase(dbconn, backpath)
    if err != nil {
        t.Fatalf(err.Error())
    }
}
