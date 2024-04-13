package main

import (
    "fmt"
    "io/fs"
    "path/filepath"
    "os"
)

func safeWalkDir(dir string, fn fs.WalkDirFunc) error {
    info, err := os.Lstat(dir)
    if err != nil {
        return err
    }

    // Just skipping the path if it turns out to be a symbolic link.
    if info.Mode() & fs.ModeSymlink != 0 {
        return nil
    }

    return filepath.WalkDir(dir, fn)
}

func listFiles(dir string, recursive bool) ([]string, error) {
    to_report := []string{}

    err := safeWalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return err
        }

        is_dir := info.IsDir()
        if is_dir {
            if recursive || dir == path {
                return nil
            }
        }

        if info.Type() & fs.ModeSymlink != 0 {
            return nil
        }

        rel, err := filepath.Rel(dir, path)
        if err != nil {
            return err
        }

        if !recursive && is_dir {
            to_report = append(to_report, rel + "/")
            return fs.SkipDir
        } else {
            to_report = append(to_report, rel)
            return nil
        }
    })

    if err != nil {
        return nil, fmt.Errorf("failed to obtain a directory listing; %w", err)
    }

    return to_report, nil
}