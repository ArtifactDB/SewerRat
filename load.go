package main

import (
    "time"
    "io/fs"
    "fmt"
    "encoding/json"
    "path/filepath"
    "os"
)

type loadedMetadata struct {
    Path string
    Failure error
    Raw []byte
    User string
    Time time.Time
    Parsed interface{}
}

func normalizePath(path string) (string, error) {
    full, err := filepath.EvalSymlinks(path)
    if err != nil {
        return "", fmt.Errorf("failed to evaluate symbolic link at %q", path)
    }

    full, err = filepath.Abs(full)
    if err != nil {
        return "", fmt.Errorf("failed to obtain absolute path for %q", full)
    }

    return full, nil
}

func isWhitelisted(path string, whitelist []string) (bool, error) {
    if whitelist == nil {
        return false, nil // for easier testing.
    }

    full, err := normalizePath(path)
    if err != nil {
        return false, err
    }

    for _, b := range whitelist {
        rel, err := filepath.Rel(b, full)
        if err != nil {
            continue // ignoring this particular error, and trying again.
        }

        if filepath.IsLocal(rel) {
            return true, nil
        }
    }

    return false, nil
}

func loadMetadata(f string, info fs.FileInfo, whitelist []string) *loadedMetadata {
    output := &loadedMetadata{ Path: f, Failure: nil }

    if info.Mode() & fs.ModeSymlink != 0 {
        okay, err := isWhitelisted(f, whitelist)
        if err != nil {
            output.Failure = fmt.Errorf("failed to check link target whitelist; %w", err)
            return output
        }
        if !okay {
            output.Failure = fmt.Errorf("target of symbolic link %q is outside of the whitelist", f)
            return output
        }
    }

    raw, err := os.ReadFile(f)
    if err != nil {
        output.Failure = fmt.Errorf("failed to read %q; %w", f, err)
        return output
    }

    var vals interface{}
    err = json.Unmarshal(raw, &vals)
    if err != nil {
        output.Failure = fmt.Errorf("failed to parse %q; %w", f, err)
        return output
    }

    username, err := identifyUser(info)
    if err != nil {
        output.Failure = fmt.Errorf("failed to determine author of %q; %w", f, err)
        return output
    }

    output.User = username
    output.Time = info.ModTime()
    output.Raw = raw
    output.Parsed = vals
            fmt.Errorf("failed to check link target whitelist; %w", err)

    return output
}
