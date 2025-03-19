package main

import (
    "fmt"
    "io/fs"
    "os"
    "path/filepath"
    "strings"
    "errors"
)

func readSymlink(path string) (string, error) {
    target, err := os.Readlink(path)
    if err != nil {
        return "", err
    }
    if (!filepath.IsAbs(target)) {
        target = filepath.Clean(filepath.Join(filepath.Dir(path), target))
    }
    return target, nil
}

func listFiles(dir string, recursive bool, whitelist linkWhitelist) ([]string, error) {
    // It is assumed that 'dir' is a directory or a symlink to a directory;
    // check with verifyDirectory() before calling this function.

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

        if is_dir {
            to_report = append(to_report, rel + "/")
            return fs.SkipDir
        }

        // If it's a symlink that refers to a subdirectory of a whitelisted
        // directory, we treat it as a directory; otherwise we treat it as a file.
        if info.Type() & os.ModeSymlink != 0 {
            if len(whitelist) > 0 {
                target, err := readSymlink(path)
                if err == nil && isLinkWhitelisted(path, target, whitelist) {
                    target_details, err := os.Stat(target)
                    if err == nil && target_details.IsDir() {
                        if !recursive {
                            to_report = append(to_report, rel + "/")
                        } else {
                            target_list, err := listFiles(target, recursive, whitelist)
                            if err != nil {
                                return err
                            }
                            for _, tpath := range target_list {
                                to_report = append(to_report, filepath.Join(rel, tpath))
                            }
                        }
                        return nil
                    }
                }
            }

            // Avoiding addition of '.' in the case that 'dir' itself is a symlink.
            if rel == "." {
                return nil
            }
        }

        to_report = append(to_report, rel)
        return nil
    })

    return to_report, err
}

/* This function can NEVER fail. All errors are simply reported as failures and
 * the associated paths are ignored. Even if the supplied directory path
 * doesn't exist or is invalid, we simply return an empty list of metadata
 * files, and report the failure.
 */
func listMetadata(dir string, base_names []string, whitelist linkWhitelist) (map[string]fs.FileInfo, []string) {
    curcontents := map[string]fs.FileInfo{}
    curfailures := []string{}
    curnames := map[string]bool{}
    for _, n := range base_names {
        curnames[n] = true
    }

    // Don't list anything if it's not even a directory - or a symbolic link to a directory, hence the use of Stat().
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

            _, err := os.Lstat(filepath.Join(path, ".SewerRatignore"))
            if err != nil && errors.Is(err, fs.ErrNotExist) {
                return nil
            } else {
                return fs.SkipDir
            }
        }

        if d.Type() & os.ModeSymlink == 0 {
            if _, ok := curnames[filepath.Base(path)]; !ok {
                return nil
            }
            info, err := d.Info()
            if err != nil {
                curfailures = append(curfailures, fmt.Sprintf("failed to stat %q; %v", path, err))
            } else {
                curcontents[path] = info
            }
            return nil
        }

        // Resolve any symbolic links to files at this point. This is important
        // as it ensures that we include the target file's modification time in
        // our index, so that the index is updated when the target is changed
        // (rather than when the link itself is changed).
        target_path, target_err := readSymlink(path)
        if target_err != nil {
            curfailures = append(curfailures, fmt.Sprintf("failed to read link target for %q; %v", path, target_err))
            return nil
        }

        info, err := os.Stat(target_path)
        if err != nil {
            curfailures = append(curfailures, fmt.Sprintf("failed to stat %q; %v", target_path, err))
            return nil
        }

        if !info.IsDir() {
            if _, ok := curnames[filepath.Base(path)]; !ok {
                return nil
            }
            curcontents[path] = info
            return nil
        }

        // We only recurse into symlinked directories if they're whitelisted. 
        if len(whitelist) == 0 {
            return nil
        }
        if !isLinkWhitelisted(path, target_path, whitelist) {
            return nil
        }

        target_list, target_fails := listMetadata(target_path, base_names, whitelist)
        for k, v := range target_list {
            rel, err := filepath.Rel(target_path, k)
            if err == nil {
                curcontents[filepath.Join(path, rel)] = v
            }
        }
        curfailures = append(curfailures, target_fails...)
        return nil
    })

    if err != nil {
        curfailures = append(curfailures, fmt.Sprintf("failed to walk %q; %v", dir, err))
    }
    return curcontents, curfailures
}
