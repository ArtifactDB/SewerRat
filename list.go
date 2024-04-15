package main

import (
    "io/fs"
    "path/filepath"
)

func listFiles(dir string, recursive bool) ([]string, error) {
    to_report := []string{}
    err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return err
        }

        is_dir := info.IsDir()
        if is_dir {
            if recursive || dir == path {
                return nil
            }
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

    return to_report, err
}
