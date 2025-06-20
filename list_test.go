package main

import (
    "testing"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "context"
    "errors"
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
        all, err := listFiles(dir, true, nil, context.Background())
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
        all, err := listFiles(dir, false, nil, context.Background())
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
        all, err := listFiles(more_symdir, true, nil, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if !equalStringArrays(all, []string{ "A", "extra", "sub" }) {
            t.Fatalf("should not recurse into symbolic links; %v", all)
        }

        all, err = listFiles(more_symdir, false, nil, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if !equalStringArrays(all, []string{ "A", "extra", "sub" }) {
            t.Fatalf("should not treat symbolic links as directories; %v", all)
        }
    })

    // Unless they've been whitelisted.
    t.Run("whitelisted symbolic", func(t *testing.T) {
        all, err := listFiles(more_symdir, true, linkWhitelist{ dir: nil }, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if !equalStringArrays(all, []string{ "A", "extra", "sub/B" }) {
            t.Fatalf("should recurse into whitelisted symbolic links; %v", all)
        }

        all, err = listFiles(more_symdir, false, linkWhitelist{ dir: nil }, context.Background())
        if err != nil {
            t.Fatal(err)
        }
        if !equalStringArrays(all, []string{ "A", "extra", "sub/" }) {
            t.Fatalf("should list whitelisted symbolic links as directories; %v", all)
        }
    })

    t.Run("top-level symlink", func(t *testing.T) {
        // If the directory itself is a symlink, we get sensible behavior...
        staging, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }
        host := filepath.Join(staging, "FOO")
        err = os.Symlink(dir, host)
        if err != nil {
            t.Fatal(err)
        }

        all, err := listFiles(host, false, nil, context.Background())
        if len(all) != 0 {
            t.Errorf("unexpected results from listing via an un-whitelisted symlink (%q)", all)
        }

        // But if it is in the whitelist, it will be entered.
        all, err = listFiles(more_symdir, false, linkWhitelist{ dir: nil }, context.Background())
        sort.Strings(all)
        if len(all) != 3 || all[0] != "A" || all[1] != "extra" || all[2] != "sub/" {
            t.Errorf("unexpected results from listing via a whitelisted symlink (%q)", all)
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, err := listFiles(dir, false, nil, canceled)
        if err == nil || !errors.Is(err, context.Canceled) {
            t.Error("expected list files job to be canceled")
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
        found, fails := listMetadata(dir, []string{ "A.json" }, nil, context.Background())
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
        found, fails := listMetadata(dir, []string{ "A.json", "B.json" }, nil, context.Background())
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

    t.Run("invalid directories", func(t *testing.T) {
        _, fails := listMetadata("missing", []string{ "A.json", "B.json" }, nil, context.Background())
        if len(fails) != 1 || !strings.Contains(fails[0], "no such file") {
            t.Fatalf("should report a failure when directory is missing")
        }

        // Providing a file instead of a directory.
        _, fails = listMetadata(filepath.Join(dir, "A.json"), []string{ "A.json", "B.json" }, nil, context.Background())
        if len(fails) != 1 || !strings.Contains(fails[0], "not a directory") {
            t.Fatalf("should report a failure when supplied path is not a directory")
        }
    })

    t.Run("canceled", func(t *testing.T) {
        canceled, cancelfun := context.WithCancel(context.Background())
        cancelfun()

        _, fails := listMetadata(dir, nil, nil, canceled)
        if len(fails) == 0 || !strings.Contains(fails[0], "canceled") {
            t.Error("expected list metadata job to be canceled")
        }
    })
}

func TestListMetadataSymlink(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir, "A.json")
    err = os.WriteFile(path, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    hostdir, err := os.MkdirTemp("", "")
    hostpath := filepath.Join(hostdir, "B.json")
    err = os.WriteFile(hostpath, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    // Throwing in some symbolic links.
    err = os.Symlink(path, filepath.Join(dir, "foo.json"))
    if err != nil {
        t.Fatal(err)
    }

    err = os.Symlink(hostdir, filepath.Join(dir, "symlinked"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("symlink", func(t *testing.T) {
        found, fails := listMetadata(dir, []string{ "foo.json", "B.json" }, nil, context.Background())
        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }

        // B.json in the linked directory should be ignored as we don't recurse into them.
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

        // Checking that the basename of the link is correctly verified.
        found, fails = listMetadata(dir, []string{ "B.json" }, nil, context.Background())
        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }
        if len(found) != 0 { // foo.json isn't considered anymore, and B.json can't be reached through the symlink.
            t.Fatal("expected no files")
        }
    })

    t.Run("whitelisted", func(t *testing.T) {
        found, fails := listMetadata(dir, []string{ "foo.json", "B.json" }, linkWhitelist{ hostdir: nil }, context.Background())
        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }

        // B.json in the linked directory should now be detected.
        if len(found) != 2 {
            t.Fatalf("expected two files; %v", found)
        }

        _, ok := found[filepath.Join(dir, "symlinked/B.json")]
        if !ok {
            t.Fatal("missing file")
        }
    })

    t.Run("top-level symlink", func(t *testing.T) {
        // If the directory itself is a symlink, we get sensible behavior...
        staging, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }
        host := filepath.Join(staging, "FOO")
        err = os.Symlink(dir, host)
        if err != nil {
            t.Fatal(err)
        }

        found, fails := listMetadata(host, []string{ "A.json" }, nil, context.Background())
        if len(fails) > 0 {
            t.Fatalf("unexpected failures; %v", fails)
        }
        if len(found) != 0 {
            t.Errorf("unexpected results from listing via an un-whitelisted symlink (%q)", found)
        }

        // But if it is in the whitelist, it will be entered.
        found, fails = listMetadata(host, []string{ "A.json" }, linkWhitelist{ dir: nil }, context.Background())
        if len(fails) > 0 {
            t.Fatalf("unexpected failures; %v", fails)
        }
        if len(found) != 1 {
            t.Fatalf("expected a metadata file; %v", found)
        }
        _, ok := found[filepath.Join(host, "A.json")]
        if !ok {
            t.Fatal("missing file")
        }
    })
}

func TestListMetadataDot(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir, "A.json")
    err = os.WriteFile(path, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    // Throwing in a hidden directory.
    subdir := filepath.Join(dir, ".git")
    err = os.Mkdir(subdir, 0755)
    if err != nil {
        t.Fatalf("failed to create a temporary subdirectory; %v", err)
    }

    subpath1 := filepath.Join(subdir, "A.json")
    err = os.WriteFile(subpath1, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    t.Run("dot", func(t *testing.T) {
        found, fails := listMetadata(dir, []string{ "A.json" }, nil, context.Background())
        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }

        // A.json in the subdirectory should be ignored as we don't recurse into dots.
        if len(found) != 1 {
            t.Fatal("expected exactly one file")
        }
        _, ok := found[filepath.Join(dir, "A.json")]
        if !ok {
            t.Fatal("missing file")
        }
    })
}

func TestListMetadataIgnored(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir, "A.json")
    err = os.WriteFile(path, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    // Throwing in a hidden directory.
    subdir := filepath.Join(dir, "super")
    err = os.Mkdir(subdir, 0755)
    if err != nil {
        t.Fatalf("failed to create a temporary subdirectory; %v", err)
    }

    subpath1 := filepath.Join(subdir, "A.json")
    err = os.WriteFile(subpath1, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    t.Run("control", func(t *testing.T) {
        found, fails := listMetadata(dir, []string{ "A.json" }, nil, context.Background())
        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }
        if len(found) != 2 {
            t.Fatal("expected exactly two files")
        }
    })

    err = os.WriteFile(filepath.Join(subdir, ".SewerRatignore"), []byte{}, 0644)
    if err != nil {
        t.Fatalf("failed to create the ignore file; %v", err)
    }

    t.Run("ignored", func(t *testing.T) {
        found, fails := listMetadata(dir, []string{ "A.json" }, nil, context.Background())
        if len(fails) > 0 {
            t.Fatal("unexpected failures")
        }
        if len(found) != 1 {
            t.Fatal("expected exactly one file")
        }
        _, ok := found[filepath.Join(dir, "A.json")]
        if !ok {
            t.Fatal("missing file")
        }
    })
}

func TestReadSymlink(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatal(err)
    }

    subdir := filepath.Join(dir, "sub")
    err = os.Mkdir(subdir, 0755)
    if err != nil {
        t.Fatal(err)
    }

    path := filepath.Join(subdir, "A")
    err = os.WriteFile(path, []byte("blah blah"), 0644)
    if err != nil {
        t.Fatal(err)
    }

    // Resolves symlink to its absolute path.
    err = os.Symlink(filepath.Join("sub", "A"), filepath.Join(dir, "B"))
    if err != nil {
        t.Fatal(err)
    }

    out, err := readSymlink(filepath.Join(dir, "B"))
    if err != nil {
        t.Fatal(err)
    }
    if !filepath.IsAbs(out) {
        t.Errorf("expected an absolute file path; %v", out)
    }

    contents, err := os.ReadFile(out)
    if err != nil || string(contents) != "blah blah" {
        t.Error("unexpected contents of the file")
    }

    // Does the right thing if the symlink is already absolute.
    err = os.Symlink(filepath.Join(dir, "B"), filepath.Join(dir, "C"))
    if err != nil {
        t.Fatal(err)
    }
    out, err = readSymlink(filepath.Join(dir, "C"))
    if err != nil {
        t.Fatal(err)
    }
    if out != filepath.Join(dir, "B") {
        t.Errorf("unexpected file path; %v", out)
    }
}
