package main

import (
    "testing"
    "os"
    "path/filepath"
    "time"
    "strings"
)

func TestLoadMetadata(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    t.Run("success", func(t *testing.T) {
        path := filepath.Join(dir, "A")
        err = os.WriteFile(path, []byte("{ \"a\": 1 }"), 0644)
        if err != nil {
            t.Fatalf("failed to create a mock file; %v", err)
        }

        info, err := os.Stat(path)
        if err != nil {
            t.Fatal(err)
        }

        loaded := loadMetadata(path, info)
        if loaded.Failure != nil {
            t.Fatal(loaded.Failure)
        }

        if loaded.Path != path || len(loaded.User) == 0 || len(loaded.Raw) == 0 || time.Now().Sub(loaded.Time) < 0 {
            t.Fatalf("unexpected values from loaded metadata %v", *loaded)
        }

        conv, ok := loaded.Parsed.(map[string]interface{})
        if !ok {
            t.Fatal("unexpected parsed object")
        }

        found, ok := conv["a"]
        if !ok {
            t.Fatal("unexpected parsed object")
        }

        target, ok := found.(float64)
        if !ok || target != 1 {
            t.Fatal("unexpected parsed object")
        }
    })

    t.Run("reading failure", func(t *testing.T) {
        path := filepath.Join(dir, "missing")
        loaded := loadMetadata(path, nil)
        if loaded.Failure == nil || !strings.Contains(loaded.Failure.Error(), "failed to read") {
            t.Fatal("expected a reading error")
        }
    })

    t.Run("parsing failure", func(t *testing.T) {
        path := filepath.Join(dir, "B")
        err = os.WriteFile(path, []byte("{ whee }"), 0644)
        if err != nil {
            t.Fatalf("failed to create a mock file; %v", err)
        }

        info, err := os.Stat(path)
        if err != nil {
            t.Fatal(err)
        }

        loaded := loadMetadata(path, info)
        if loaded.Failure == nil || !strings.Contains(loaded.Failure.Error(), "failed to parse") {
            t.Fatal("expected a parsing error")
        }
    })

    t.Run("symlink okay", func(t *testing.T) {
        path := filepath.Join(dir, "C")
        err = os.Symlink(filepath.Join(dir, "A"), path)
        if err != nil {
            t.Fatalf("failed to create a symlink; %v", err)
        }

        info, err := os.Lstat(path) // check that symlinks are correctly loaded.
        if err != nil {
            t.Fatal(err)
        }

        loaded := loadMetadata(path, info)
        if loaded.Failure != nil {
            t.Fatal(loaded.Failure)
        }

        if loaded.Path != path || len(loaded.User) == 0 || len(loaded.Raw) == 0 || time.Now().Sub(loaded.Time) < 0 {
            t.Fatalf("unexpected values from symlink-loaded metadata %v", *loaded)
        }
    })
}
