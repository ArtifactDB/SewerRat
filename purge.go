package main

import (
    "time"
    "os"
    "fmt"
    "errors"
    "path/filepath"
)

func purgeOldFiles(scratch string, maxlife time.Duration) error {
    listing, err := os.ReadDir(scratch)
    if err != nil {
        return fmt.Errorf("failed to read contents of %q; %w", scratch, err) 
    }

    threshold := time.Now().Add(-maxlife)
    all_errors := []error{}
    for _, f := range listing {
        info, err := f.Info()
        if err != nil {
            all_errors = append(all_errors, fmt.Errorf("failed to extract info for %q; %w", info.Name(), err))
            continue
        }

        timestamp := info.ModTime()
        if threshold.After(timestamp) {
            err := os.Remove(filepath.Join(scratch, f.Name()))
            if err != nil {
                all_errors = append(all_errors, fmt.Errorf("failed to remove %q; %w", f.Name(), err))
            }
        }
    }

    if len(all_errors) > 0 {
        return errors.Join(all_errors...)
    } else {
        return nil
    }
}
