package main

import (
    "testing"
    "os"
    "path/filepath"
    "errors"
    "time"
)

func TestPurgeOldFiles(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }

    f1 := filepath.Join(tmp, "foo.txt")
    err = os.WriteFile(f1, []byte("aaron lun"), 0644)
    if err != nil {
        t.Fatalf(err.Error())
    }

    f2 := filepath.Join(tmp, "bar.txt")
    err = os.WriteFile(f2, []byte("jayaram kancherla"), 0644)
    if err != nil {
        t.Fatalf(err.Error())
    }

    err = purgeOldFiles(tmp, time.Minute)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if _, err := os.Stat(f1); err != nil {
        t.Fatalf("should not have purged %q", f1)
    }
    if _, err := os.Stat(f2); err != nil {
        t.Fatalf("should not have purged %q", f2)
    }

    err = purgeOldFiles(tmp, 0 * time.Minute)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if _, err := os.Stat(f1); !errors.Is(err, os.ErrNotExist) {
        t.Fatalf("should have purged %q", f1)
    }
    if _, err := os.Stat(f2); !errors.Is(err, os.ErrNotExist) {
        t.Fatalf("should have purged %q", f2)
    }
}
