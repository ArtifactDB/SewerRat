package main

import (
    "os"
    "fmt"
    "sync"
    "encoding/base64"
    "path/filepath"
    "crypto/rand"
    "errors"
    "time"
    "net/http"
)

type verificationSession struct {
    Code string
    Created time.Time
}

type verificationRegistry struct {
    Lock sync.Mutex
    Sessions map[string]verificationSession
    Lifespan time.Duration
}

func newVerificationRegistry(lifespan time.Duration) *verificationRegistry {
    return &verificationRegistry { 
        Sessions: map[string]verificationSession{},
        Lifespan: lifespan,
    }
}

func (vr *verificationRegistry) Provision(path string) (string, error) {
    var candidate string
    buff := make([]byte, 32) // 256 bits of entropy should be enough.
    found := false

    for i := 0; i < 10; i++ {
        _, err := rand.Read(buff)
        if err != nil {
            return "", fmt.Errorf("random generation failed; %w", err)
        }

        candidate = ".sewer_" + base64.RawURLEncoding.EncodeToString(buff)

        _, err = os.Lstat(filepath.Join(path, candidate))
        if err != nil {
            if errors.Is(err, os.ErrNotExist) {
                found = true
                break
            } else if errors.Is(err, os.ErrPermission) {
                return "", newHttpError(http.StatusBadRequest, fmt.Errorf("path is not accessible; %w", err))
            } else {
                return "", fmt.Errorf("failed to inspect path; %w", err)
            }
        }
    }

    if !found {
        return "", errors.New("exhausted attempts")
    }

    vr.Lock.Lock()
    defer vr.Lock.Unlock()
    vr.Sessions[path] = verificationSession{ 
        Code: candidate,
        Created: time.Now(),
    }

    // Automatically deleting it after some time has expired.
    go func() {
        time.Sleep(vr.Lifespan)
        vr.Lock.Lock()
        defer vr.Lock.Unlock()
        delete(vr.Sessions, path)
    }()

    return candidate, nil
}

func (vr *verificationRegistry) Pop(path string) (string, bool) {
    vr.Lock.Lock()
    defer vr.Lock.Unlock()

    found, ok := vr.Sessions[path]
    if !ok {
        return "", false
    }

    delete(vr.Sessions, path)
    return found.Code, true
}
