package main

import (
    "time"
    "io/fs"
    "fmt"
    "encoding/json"
    "os"
    "bytes"
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
    dec := json.NewDecoder(bytes.NewReader(raw))
    dec.UseNumber() // preserve numbers as strings for tokenization.
    err = dec.Decode(&vals)
    if err != nil {
        output.Failure = fmt.Errorf("failed to parse %q; %w", f, err)
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
