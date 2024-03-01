package main

import (
    "os"
    "testing"
    "strings"
    "path/filepath"
)

func TestCreateVerificationCode(t *testing.T) {
    dir := os.TempDir()
    out, err := createVerificationCode(dir)
    if err != nil {
        t.Fatalf(err.Error())
    }

    if !strings.HasPrefix(out, ".sewer_") {
        t.Fatalf("expected code to have a '.sewer_' prefix")
    }
    if len(out) < 32 {
        t.Fatalf("expected code to be at least 32 characters long")
    }

    // Get a different code on another invocation.
    out2, err := createVerificationCode(dir)
    if err != nil {
        t.Fatalf(err.Error())
    }
    if out == out2 {
        t.Fatalf("expected to get different codes")
    }
}

func TestDepositVerificationCode(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf(err.Error())
    }
    defer os.RemoveAll(tmp)

    err = depositVerificationCode(tmp, "/foo/bar", "aaron")
    if err != nil {
        t.Fatalf(err.Error())
    }
    if _, err := os.Stat(filepath.Join(tmp, "%2Ffoo%2Fbar")); err != nil {
        t.Fatalf("failed to deposit the verification code")
    }

    code, err := fetchVerificationCode(tmp, "/foo/bar")
    if err != nil {
        t.Fatalf("failed to fetch the verification code")
    }
    if code != "aaron" {
        t.Fatalf("mismatch in the verification code")
    }
}
