package main

import (
    "fmt"
    "io/fs"
    "os"
    "path/filepath"
    "strings"
    "errors"
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

/* This function can NEVER fail. All errors are simply reported as failures and
 * the associated paths are ignored. Even if the supplied directory path
 * doesn't exist or is invalid, we simply return an empty list of metadata
 * files, and report the failure.
 */
func listMetadata(dir string, base_names []string) (map[string]fs.FileInfo, []string) {
    curcontents := map[string]fs.FileInfo{}
    curfailures := []string{}
    curnames := map[string]bool{}
    for _, n := range base_names {
        curnames[n] = true
    }

    // Don't list anything if it's not even a directory.
    info, err := os.Stat(dir)
    if err != nil {
        curfailures = append(curfailures, fmt.Sprintf("failed to inspect %q; %v", dir, err))
        return curcontents, curfailures
    }
    if !info.IsDir() {
        curfailures = append(curfailures, fmt.Sprintf("%q is not a directory", dir))
        return curcontents, curfailures
    }

    // Just skip any subdirectories that we can't access, no need to check the error.
    err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
        if err != nil {
            curfailures = append(curfailures, fmt.Sprintf("failed to walk %q; %v", path, err))
            return nil
        }

        if d.IsDir() {
            base := filepath.Base(path)
            if strings.HasPrefix(base, ".") {
                return fs.SkipDir
            }

            _, err := os.Stat(filepath.Join(path, ".SewerRatignore"))
            if err != nil && errors.Is(err, fs.ErrNotExist) {
                return nil
            } else {
                return fs.SkipDir
            }
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

            // We don't recurse into symlinked directories, though.
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

    if err != nil {
        curfailures = append(curfailures, fmt.Sprintf("failed to walk %q; %v", dir, err))
    }
    return curcontents, curfailures
}
