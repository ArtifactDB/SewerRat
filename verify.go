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

// We use a multi-pool approach to improve parallelism across requests.
// The idea is that each path's length (modulo the number of pools) is
// used to determine the pool in which its verification codes are stored.
// This should distribute requests fairly evenly among multiple locks.
type verificationRegistry struct {
    NumPools int
    Locks []sync.Mutex
    Sessions []map[string]verificationSession
}

func newVerificationRegistry(num_pools int) *verificationRegistry {
    return &verificationRegistry {
        NumPools: num_pools,
        Locks: make([]sync.Mutex, num_pools),
        Sessions: make([]map[string]verificationSession, num_pools),
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

        _, err = os.Stat(filepath.Join(path, candidate))
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

    i := len(path) % vr.NumPools
    vr.Locks[i].Lock()
    defer vr.Locks[i].Unlock()

    if vr.Sessions[i] == nil {
        vr.Sessions[i] = map[string]verificationSession{}
    }
    vr.Sessions[i][path] = verificationSession{ Code: candidate, Created: time.Now() }

    return candidate, nil
}

func (vr *verificationRegistry) Pop(path string) (string, bool) {
    i := len(path) % vr.NumPools
    vr.Locks[i].Lock()
    defer vr.Locks[i].Unlock()

    if vr.Sessions[i] == nil {
        return "", false
    }

    found, ok := vr.Sessions[i][path]
    if !ok {
        return "", false
    }

    delete(vr.Sessions[i], path)
    return found.Code, true
}

func (vr *verificationRegistry) Flush(lifespan time.Duration) {
    threshold := time.Now().Add(-lifespan)
    var wg sync.WaitGroup
    wg.Add(vr.NumPools)

    for i := 0; i < vr.NumPools; i++ {
        go func(i int) {
            defer wg.Done()
            vr.Locks[i].Lock()
            defer vr.Locks[i].Unlock()
            if vr.Sessions[i] != nil {
                for k, v := range vr.Sessions[i] {
                    if threshold.After(v.Created) {
                        delete(vr.Sessions[i], k)
                    }
                }
            }
        }(i)
    }

    wg.Wait()
    return
}
