package main

import (
    "os"
    "fmt"
    "net/url"
    "encoding/base64"
    "path/filepath"
    "crypto/rand"
    "errors"
)

func validateRequestPath(encoded string) (string, error) {
    if encoded == "" {
        return "", errors.New("path parameter should be a non-empty string")
    }

    regpath, err := url.QueryUnescape(encoded)
    if err != nil {
        return "", errors.New("path parameter should be a URL-encoded path")
    }

    if !filepath.IsAbs(regpath) {
        return "", errors.New("path parameter should be an absolute path")
    }

    return regpath, nil
}

func createVerificationCode(path string) (string, error) {
    var candidate string
    buff := make([]byte, 40)
    found := false

    for i := 0; i < 10; i++ {
        _, err := rand.Read(buff)
        if err != nil {
            return "", fmt.Errorf("random generation failed; %w", err)
        }

        candidate = ".sewer_" + base64.RawURLEncoding.EncodeToString(buff)
        _, err = os.Stat(filepath.Join(path, candidate))

        if err != nil {
            if errors.Is(err, os.ErrNotExist) {
                found = true
                break
            } else if errors.Is(err, os.ErrPermission) {
                return "", fmt.Errorf("path is not accessible; %w", err)
            } else {
                return "", fmt.Errorf("failed to inspect path; %w", err)
            }
        }
    }

    if !found {
        return "", errors.New("exhausted attempts")
    }

    return candidate, nil
}

func depositVerificationCode(scratch, path, code string) error {
    encpath := url.QueryEscape(path) // re-encoding it to guarantee that there isn't any weirdness.
    return os.WriteFile(filepath.Join(scratch, encpath), []byte(code), 0600)
}

func fetchVerificationCode(scratch, path string) (string, error) {
    encpath := url.QueryEscape(path) // re-encoding it to guarantee that there isn't any weirdness.
    target_path := filepath.Join(scratch, encpath)
    val, err := os.ReadFile(target_path)
    if err != nil {
        return "", err
    }
    return string(val), nil
}
