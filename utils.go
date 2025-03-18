package main

import (
    "fmt"
    "os"
    "net/http"
    "errors"
)

type httpError struct {
    Status int
    Reason error
}

func (r *httpError) Error() string {
    return r.Reason.Error()
}

func (r *httpError) Unwrap() error {
    return r.Reason
}

func newHttpError(status int, reason error) *httpError {
    return &httpError{ Status: status, Reason: reason }
}

func verifyDirectory(dir string) error {
    // We're willing to accept symlinks to directories, hence the use of Stat().
    info, err := os.Stat(dir)
    if errors.Is(err, os.ErrNotExist) {
        return newHttpError(http.StatusNotFound, fmt.Errorf("path %q does not exist", dir))
    }

    if err != nil {
        return fmt.Errorf("failed to check %q; %w", dir, err)
    }

    if !info.IsDir() {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("%q is not a directory", dir))
    }

    return nil
}
