package main

import (
    "testing"
    "os"
    "path/filepath"
    "sort"
    "strings"
)

func TestListFiles(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir, "A")
    err = os.WriteFile(path, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    subdir := filepath.Join(dir, "sub")
    err = os.Mkdir(subdir, 0755)
    if err != nil {
        t.Fatalf("failed to create a temporary subdirectory; %v", err)
    }

    subpath := filepath.Join(subdir, "B")
    err = os.WriteFile(subpath, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    // Checking that we pull out all the files.
    t.Run("basic", func(t *testing.T) {
        all, err := listFiles(dir, true)
        if (err != nil) {
            t.Fatal(err)
        }

        sort.Strings(all)
        if len(all) != 2 || all[0] != "A" || all[1] != "sub/B" {
            t.Errorf("unexpected results from the listing (%q)", all)
        }
    })

    // Checking that the directories are properly listed.
    t.Run("non-recursive", func(t *testing.T) {
        all, err := listFiles(dir, false)
        if (err != nil) {
            t.Fatal(err)
        }

        sort.Strings(all)
        if len(all) != 2 || all[0] != "A" || all[1] != "sub/" {
            t.Errorf("unexpected results from the listing (%q)", all)
        }
    })

    // Checking that we skip symbolic links inside the directory.
    more_symdir, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    err = os.Symlink(filepath.Join(dir, "A"), filepath.Join(more_symdir, "A"))
    if err != nil {
        t.Fatal(err)
    }

    err = os.Link(filepath.Join(dir, "A"), filepath.Join(more_symdir, "extra"))
    if err != nil {
        t.Fatal(err)
    }

    err = os.Symlink(filepath.Join(dir, "sub"), filepath.Join(more_symdir, "sub"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("skip symbolic nested", func(t *testing.T) {
        all, err := listFiles(more_symdir, true)
        if err != nil {
            t.Fatal(err)
        }

        if !equalStringArrays(all, []string{ "A", "extra", "sub" }) {
            t.Fatalf("should not list symbolic links %q", all)
        }
    })
}

func TestListMetadata(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir, "A.json")
    err = os.WriteFile(path, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    subdir := filepath.Join(dir, "sub")
    err = os.Mkdir(subdir, 0755)
    if err != nil {
        t.Fatalf("failed to create a temporary subdirectory; %v", err)
    }

    subpath1 := filepath.Join(subdir, "A.json")
    err = os.WriteFile(subpath1, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    subpath2 := filepath.Join(subdir, "B.json")
    err = os.WriteFile(subpath2, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    t.Run("simple", func(t *testing.T) {
        found, fails, err := listMetadata(dir, []string{ "A.json" })
        if err != nil {
            t.Fatal(err)
        }

        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }

        if info, ok := found[path]; !ok || info.Name() != "A.json" {
            t.Fatal("missing file")
        }

        if info, ok := found[subpath1]; !ok || info.Name() != "A.json" {
            t.Fatal("missing file")
        }

        if _, ok := found[subpath2]; ok {
            t.Fatal("unexpected file")
        }
    })

    t.Run("multiple", func(t *testing.T) {
        found, fails, err := listMetadata(dir, []string{ "A.json", "B.json" })
        if err != nil {
            t.Fatal(err)
        }

        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }

        if info, ok := found[path]; !ok || info.Name() != "A.json" {
            t.Fatal("missing file")
        }

        if info, ok := found[subpath1]; !ok || info.Name() != "A.json" {
            t.Fatal("missing file")
        }

        if info, ok := found[subpath2]; !ok || info.Name() != "B.json" {
            t.Fatal("missing file")
        }
    })

    t.Run("walk failure", func(t *testing.T) {
        found, fails, err := listMetadata("missing", []string{ "A.json", "B.json" })
        if err != nil {
            t.Fatal(err)
        }

        if len(fails) != 1 || !strings.Contains(fails[0], "failed to walk") {
            t.Fatalf("expected a walking failure %v", fails)
        }

        if len(found) != 0 {
            t.Fatal("unexpected file")
        }
    })

    // Throwing in some symbolic links.
    err = os.Symlink(path, filepath.Join(dir, "foo.json"))
    if err != nil {
        t.Fatal(err)
    }

    err = os.Symlink(subdir, filepath.Join(dir, "bar.json"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("symlink", func(t *testing.T) {
        found, fails, err := listMetadata(dir, []string{ "foo.json", "bar.json" })
        if err != nil {
            t.Fatal(err)
        }

        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }

        if len(found) != 1 {
            t.Fatal("expected exactly one file")
        }

        info, ok := found[filepath.Join(dir, "foo.json")]
        if !ok {
            t.Fatal("missing file")
        }
        if info.Mode() & os.ModeSymlink != 0 { // uses information from the link.
            t.Fatal("expected file info from link target")
        }
    })
}
