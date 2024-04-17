package main

import (
    "fmt"
    "io/fs"
    "os"
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

    if err != nil {
        return nil, fmt.Errorf("failed to obtain a directory listing; %w", err)
    }

    return to_report, nil
}

func listMetadata(dir string, base_names []string) (map[string]fs.FileInfo, []string, error) {
    curcontents := map[string]fs.FileInfo{}
    curfailures := []string{}
    curnames := map[string]bool{}
    for _, n := range base_names {
        curnames[n] = true
    }

    // Just skip any directories that we can't access, no need to check the error.
    err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
        if err != nil {
            curfailures = append(curfailures, fmt.Sprintf("failed to walk %q; %v", path, err))
            return nil
        }

        if d.IsDir() {
            return nil
        }

        if _, ok := curnames[filepath.Base(path)]; !ok {
            return nil
        }

        var info fs.FileInfo
        if d.Type() & os.ModeSymlink == 0 {
            info, err = d.Info()
        } else {
            // Resolve any symbolic links to files at this point. This is important
            // as it ensures that we include the target file's modification time in
            // our index, so that the index is updated when the target is changed
            // (rather than when the link itself is changed).
            info, err = os.Stat(path)
            if err == nil && info.IsDir() {
                return nil
            }
        }

        if err != nil {
            curfailures = append(curfailures, fmt.Sprintf("failed to stat %q; %v", path, err))
            return nil
        }

        curcontents[path] = info
        return nil
    })

    return curcontents, curfailures, err
}
