package main

import (
    "os"
    "errors"
    "net/http"
    "io/fs"
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

func wrapStatError(err error) error {
    if errors.Is(err, os.ErrNotExist) {
        return newHttpError(http.StatusNotFound, err)
    } else {
        return err
    }
}

func checkValidDirectory(path string) error {
    info, err := os.Lstat(path)
    if err != nil {
        return wrapStatError(err)
    }

    if info.Mode() & fs.ModeSymlink != 0 {
        return newHttpError(http.StatusBadRequest, errors.New("symbolic link to a directory is not supported"))
    }
    if !info.IsDir() {
        return newHttpError(http.StatusBadRequest, errors.New("path should be a directory"))
    }

    return nil
}

