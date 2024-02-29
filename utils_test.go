package main

import (
    "os"
    "fmt"
    "path/filepath"
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

func mockDirectory(path string) error {
    err := os.MkdirAll(path, 0700)
    if err != nil {
        return fmt.Errorf("failed to create the mock directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(path, "metadata.json"), []byte(`{ "foo": "Aaron had a little lamb", "bar": { "breed": [ "merino", "border leicester" ], "type": "lamb", "number": 1 } }`), 0600)
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

    err = os.WriteFile(filepath.Join(sub2, "other.json"), []byte(`{ "favorites": [ "Yuru Camp", "Non non biyori" ] }`), 0600)
    if err != nil {
        return fmt.Errorf("failed to mock a metadata file; %w", err)
    }

    return nil
}
