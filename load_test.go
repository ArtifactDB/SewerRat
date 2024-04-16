package main

import (
    "testing"
    "os"
    "path/filepath"
    "time"
    "strings"
)

func TestNormalizePath(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir, "A")
    err = os.WriteFile(path, []byte("foobar"), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    ref, err := normalizePath(path)
    if err != nil {
        t.Fatal(err)
    }

    contents, err := os.ReadFile(path)
    if err != nil {
        t.Fatal(err)
    }
    if string(contents) != "foobar" {
        t.Fatal("unexpected contents of file")
    }

    t.Run("simple", func (t *testing.T) {
        path2 := filepath.Join(dir, "B")
        err := os.Symlink(path, path2)
        if err != nil {
            t.Fatal(err)
        }

        norm, err := normalizePath(path2)
        if err != nil {
            t.Fatal(err)
        }

        if ref != norm {
            t.Fatalf("unexpected normalized path %q", norm)
        }
    })

    t.Run("relative", func (t *testing.T) {
        path2 := filepath.Join(dir, "C")
        err := os.Symlink("A", path2)
        if err != nil {
            t.Fatal(err)
        }

        norm, err := normalizePath(path2)
        if err != nil {
            t.Fatal(err)
        }

        if ref != norm {
            t.Fatalf("unexpected normalized path %q", norm)
        }
    })

    t.Run("indirect", func (t *testing.T) {
        path2 := filepath.Join(dir, "D")
        err := os.Symlink("C", path2)
        if err != nil {
            t.Fatal(err)
        }

        norm, err := normalizePath(path2)
        if err != nil {
            t.Fatal(err)
        }

        if ref != norm {
            t.Fatalf("unexpected normalized path %q", norm)
        }
    })
}

func TestIsWhitelisted(t *testing.T) {
    dir1, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir1, "A")
    err = os.WriteFile(path, []byte("foobar"), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    dir2, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    norm1, err := normalizePath(dir1)
    if err != nil {
        t.Fatal(err)
    }

    norm2, err := normalizePath(dir2)
    if err != nil {
        t.Fatal(err)
    }

    t.Run("simple", func(t * testing.T) {
        path2 := filepath.Join(dir2, "B")
        err := os.Symlink(path, path2)
        if err != nil {
            t.Fatal(err)
        }

        okay, err := isWhitelisted(path2, nil)
        if err != nil {
            t.Fatal(err)
        }
        if okay {
            t.Fatal("no whitelist provided")
        }

        okay, err = isWhitelisted(path2, []string{ norm2 })
        if err != nil {
            t.Fatal(err)
        }
        if okay {
            t.Fatal("should not be in whitelist")
        }

        okay, err = isWhitelisted(path2, []string{ norm2, norm1 })
        if err != nil {
            t.Fatal(err)
        }
        if !okay {
            t.Fatal("should be in whitelist")
        }
    })

    t.Run("indirect", func(t * testing.T) {
        path2 := filepath.Join(dir2, "C")
        err := os.Symlink("B", path2)
        if err != nil {
            t.Fatal(err)
        }

        okay, err := isWhitelisted(path2, []string{ norm1 })
        if err != nil {
            t.Fatal(err)
        }
        if !okay {
            t.Fatal("should be in whitelist")
        }
    })
}


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

        loaded := loadMetadata(path, info, nil)
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

        loaded := loadMetadata(path, info, nil)
        if loaded.Failure == nil || !strings.Contains(loaded.Failure.Error(), "failed to parse") {
            t.Fatal("expected a parsing error")
        }
    })

    t.Run("symlink failure", func(t *testing.T) {
        path := filepath.Join(dir, "C")
        err = os.Symlink(filepath.Join(dir, "A"), path)
        if err != nil {
            t.Fatalf("failed to create a symlink; %v", err)
        }

        info, err := os.Lstat(path)
        if err != nil {
            t.Fatal(err)
        }

        loaded := loadMetadata(path, info, nil)
        if loaded.Failure == nil || !strings.Contains(loaded.Failure.Error(), "symbolic link") {
            t.Fatalf("expected a symbolic link error %v", *loaded)
        }

        // But we can load it if we put its (normalized path to the) parent directory in the whitelist.
        normed, err := normalizePath(dir)
        if err != nil {
            t.Fatal(err)
        }

        loaded = loadMetadata(path, info, []string{ "/foo", normed, "/bar" })
        if loaded.Failure != nil {
            t.Fatal(loaded.Failure)
        }

        _, ok := loaded.Parsed.(map[string]interface{})
        if !ok {
            t.Fatal("unexpected parsed object")
        }

    })
}
