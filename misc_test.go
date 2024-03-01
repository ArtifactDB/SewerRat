package main

import (
    "testing"
    "os/user"
    "os"
)

func TestIdentifyUser(t *testing.T) {
    tmp, err := os.CreateTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a temporary file")
    }
    tmp.Close()
    defer os.Remove(tmp.Name())

    details, err := os.Stat(tmp.Name())
    if err != nil {
        t.Fatalf("failed to inspect the temp file")
    }

    username, err := identifyUser(details)
    if err != nil {
        t.Fatalf(err.Error())
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf(err.Error())
    }

    if self.Username != username {
        t.Fatalf("mismatch between current user and the file author (%v, %v)", self.Name, username)
    }
}
