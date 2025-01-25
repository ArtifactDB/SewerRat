package main

import (
    "testing"
    "strings"
    "time"
)

func TestVerificationRegistry(t *testing.T) {
    t.Run("basic", func(t *testing.T) {
        v := newVerificationRegistry(time.Minute)

        path := "adasdasdasd"
        candidate, err := v.Provision(path)
        if err != nil {
            t.Fatal(err)
        }

        if !strings.HasPrefix(candidate, ".sewer_") {
            t.Fatalf("expected code to have a '.sewer_' prefix")
        }
        if len(candidate) < 32 {
            t.Fatalf("expected code to be at least 32 characters long")
        }

        reload, ok := v.Pop(path)
        if !ok || reload != candidate {
            t.Fatal("failed to report the right code")
        }

        reload, ok = v.Pop(path)
        if ok {
            t.Fatal("code should have been popped on first use")
        }

        // Get a different code on another invocation.
        candidate2, err := v.Provision(path)
        if err != nil {
            t.Fatal(err)
        }
        if candidate == candidate2 {
            t.Fatalf("expected to get different codes")
        }
    })

    t.Run("expired", func(t *testing.T) {
        v := newVerificationRegistry(time.Millisecond * 200)

        path := "foobar"
        candidate, err := v.Provision(path)
        if err != nil {
            t.Fatal(err)
        }

        // Positive control works as expected.
        reload, ok := v.Pop(path)
        if !ok || reload != candidate {
            t.Fatal("failed to report the right code")
        }

        // Expiry works as expected.
        _, err = v.Provision(path)
        time.Sleep(time.Millisecond * 500)
        reload, ok = v.Pop(path)
        if ok {
            t.Fatal("code should have expired already")
        }
    })
}
