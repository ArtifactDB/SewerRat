package main

import (
    "time"
    "io/fs"
    "fmt"
    "encoding/json"
    "os"
)

type loadedMetadata struct {
    Path string
    Failure error
    Raw []byte
    User string
    Time time.Time
    Parsed interface{}
}

func loadMetadata(f string, info fs.FileInfo) *loadedMetadata {
    output := &loadedMetadata{ Path: f, Failure: nil }

    raw, err := os.ReadFile(f)
    if err != nil {
        output.Failure = fmt.Errorf("failed to read %q; %w", f, err)
        return output
    }

    var vals interface{}
    err = json.Unmarshal(raw, &vals)
    if err != nil {
        output.Failure = fmt.Errorf("failed to parse %q; %w", f, err)
        return output
    }

    if info.Mode() & fs.ModeSymlink != 0 {
        output.Failure = fmt.Errorf("not following symbolic link at %q", f)
        return output
    }

    username, err := identifyUser(info)
    if err != nil {
        output.Failure = fmt.Errorf("failed to determine author of %q; %w", f, err)
        return output
    }

    output.User = username
    output.Time = info.ModTime()
    output.Raw = raw
    output.Parsed = vals
    return output
}
