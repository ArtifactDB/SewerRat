package main

import (
    "os"
    "os/user"
    "fmt"
    "path/filepath"
    "database/sql"
    "encoding/json"
    "sort"
    "errors"
)

func equalStringArrays(x []string, y []string) bool {
    if (x == nil) != (y == nil) {
        return false
    }
    if x == nil {
        return true
    }

    if len(x) != len(y) {
        return false
    }
    for i, v := range x {
        if v != y[i] {
            return false
        }
    }

    return true
}

func equalPathArrays(x []string, y []string, dir string) bool {
    if (x == nil) != (y == nil) {
        return false
    }
    if x == nil {
        return true
    }

    if len(x) != len(y) {
        return false
    }
    for i, v := range x {
        if v != filepath.Join(dir, y[i]) {
            return false
        }
    }

    return true
}

func mockDirectory(path string) error {
    if _, err := os.Lstat(path); err == nil || !errors.Is(err, os.ErrNotExist) {
        err := os.RemoveAll(path)
        if err != nil {
            return fmt.Errorf("failed to remove the mock directory; %w", err)
        }
    }

    err := os.MkdirAll(path, 0700)
    if err != nil {
        return fmt.Errorf("failed to create the mock directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(path, "metadata.json"), []byte(`{ "foo": "Aaron had a little lamb", "bar": { "breed": [ "merino", "border leicester" ], "type": "lamb", "cost": 1.5 } }`), 0600)
    if err != nil {
        return fmt.Errorf("failed to mock a metadata file; %w", err)
    }

    sub := filepath.Join(path, "stuff")
    err = os.Mkdir(sub, 0700)
    if err != nil {
        return fmt.Errorf("failed to mock a subdirectory; %w", err)
    }

    err = os.WriteFile(filepath.Join(sub, "metadata.json"), []byte(`{ "characters": [ { "first": "Akari", "last": "Akaza" }, { "first": "Hoshino", "last": "Kyouko" }], "anime": "Yuri Yuru", "id": 10495 }`), 0600)
    if err != nil {
        return fmt.Errorf("failed to mock a metadata file; %w", err)
    }

    err = os.WriteFile(filepath.Join(sub, "other.json"), []byte(`{ "name": "chicken tikka masala", "recipe": [ "chicken", "garlic", "rice", "spices" ], "variants": [ "lamb", "fish", "cheese" ] }`), 0600)
    if err != nil {
        return fmt.Errorf("failed to mock a metadata file; %w", err)
    }

    sub2 := filepath.Join(path, "whee")
    err = os.Mkdir(sub2, 0700)
    if err != nil {
        return fmt.Errorf("failed to mock a subdirectory; %w", err)
    }

    err = os.WriteFile(filepath.Join(sub2, "other.json"), []byte(`{ "favorites": [ "Yuru Camp", "Non non biyori" ], "category": { "iyashikei": true, "nsfw": false } }`), 0600)
    if err != nil {
        return fmt.Errorf("failed to mock a metadata file; %w", err)
    }

    return nil
}

func listPaths(dbconn * sql.DB, scratch string) ([]string, error) {
    rows, err := dbconn.Query("SELECT path FROM paths WHERE path LIKE ?", scratch + "/%")
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

func listDirs(dbconn *sql.DB) (map[string][]string, error) {
    rows, err := dbconn.Query("SELECT path, json_extract(names, '$') FROM dirs")
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    output := map[string][]string{}
    for rows.Next() {
        var path string
        var names_raw []byte
        err = rows.Scan(&path, &names_raw)
        if err != nil {
            return nil, err
        }

        var names []string
        err = json.Unmarshal(names_raw, &names)
        if err != nil {
            return nil, err
        }

        sort.Strings(names)
        output[path] = names
    }

    return output, nil
}

func createSelfLinkWhitelist() (linkWhitelist, error) {
    self_user, err := user.Current() 
    if err != nil {
        return nil, err
    }
    self_name := self_user.Username
    out := linkWhitelist{ self_name: true }

    // Also adding all the owners of the temporary directory, 
    // to avoid failures when we operate within that temporary directory.
    tmp := os.TempDir()
    for {
        info, err := os.Lstat(tmp)
        if err != nil {
            return nil, err
        }
        user, err := identifyUser(info)
        if err != nil {
            return nil, err
        }
        out[user] = true
        parent := filepath.Dir(tmp)
        if parent == tmp {
            break
        }
        tmp = parent
    }

    return out, nil
}
