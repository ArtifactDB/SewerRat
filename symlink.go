package main

import (
    "fmt"
    "os"
    "errors"
    "path/filepath"
    "io/fs"
    "net/http"
)

func checkSymlinkTarget(path string, cache map[string]bool) (fs.FileInfo, error) {
    // Fully resolve the symlink.
    path, err := filepath.EvalSymlinks(path)
    if err != nil {
        return nil, fmt.Errorf("failed to evaluate symlinks; %w", err)
    }

    // Checking that the target itself is world-readable and not a directory.
    info, err := os.Stat(path)
    if err != nil {
        return nil, fmt.Errorf("failed to stat symlink target; %w", err)
    }

    if info.IsDir() {
        return nil, newHttpError(http.StatusBadRequest, errors.New("symlink to a directory is not supported"))
    }

    perms := info.Mode().Perm()
    if perms & 0o004 == 0 {
        return nil, newHttpError(http.StatusForbidden, errors.New("symlink target is not a world-readable file"))
    }

    // Now checking that all parent directories are world-readable.
    for {
        parent := filepath.Dir(path)
        if parent == path {
            break
        }
        path = parent

        if cache != nil {
            found, ok := cache[path]
            if ok {
                if !found {
                    return nil, newHttpError(http.StatusForbidden, errors.New("parent directory of symlink target is not world-readable"))
                } else {
                    continue
                }
            }
        }

        dinfo, err := os.Stat(path)
        if err != nil {
            return nil, fmt.Errorf("failed to stat parent directory of symlink target; %w", err)
        }

        perms := dinfo.Mode().Perm()
        allowed := (perms & 0x001 == 0)
        if cache != nil {
            cache[path] = allowed
        }
        if (!allowed) {
            return nil, newHttpError(http.StatusForbidden,errors.New("parent directory of symlink target is not world-readable"))
        }
    }

    return info, nil
}
