package main

import (
    "os"
    "testing"
    "strings"
    "path/filepath"
)

func TestValidateRequestPath(t *testing.T) {
    _, err := validateRequestPath("")
    if err == nil || !strings.Contains(err.Error(), "empty string") {
        t.Fatalf("expected an empty string error")
    }

    _, err = validateRequestPath("foobar")
    if err == nil || !strings.Contains(err.Error(), "absolute path") {
        t.Fatalf("expected an absolute path error")
    }

    out, err := validateRequestPath("%2Ffoo%2Fbar")
    if err != nil {
        t.Fatalf(err.Error())
    }
    if out != "/foo/bar" {
        t.Fatalf("unexpected output %q", out)
    }

    _, err = validateRequestPath("%%foo%2Fbar")
    if err == nil || !strings.Contains(err.Error(), "URL-encoded") {
        t.Fatalf("expected a decoding error")
    }
}

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
