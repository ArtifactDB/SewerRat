package main

import (
    "flag"
    "log"
    "os"
    "time"
    "context"
    "net/http"
)

func main() {
    dbpath0 := flag.String("db", "", "Path to the SQLite file for the metadata")
    scratch0 := flag.String("scratch", "", "Path to a scratch directory")
    port0 := flag.String("port", "", "Port to listen to for requests")
    flag.Parse()

    dbpath := *dbpath0
    port := *port0
    scratch := *scratch0
    if dbpath == "" || port == "" || scratch == "" {
        flag.Usage()
        os.Exit(1)
    }

    err := os.MkdirAll(scratch, 700)
    if err != nil {
        log.Fatalf("failed to create the scratch directory at %q; %v", scratch, err)
    }

    db, err := initializeDatabase(dbpath)
    if err != nil {
        log.Fatalf("failed to create the initial SQLite file at %q; %v", dbpath, err)
    }
    defer db.Close()

    tokenizer, err := newUnicodeTokenizer(false)
    if err != nil {
        log.Fatalf("failed to create the default tokenizer; %v", err)
    }
    _, err = newUnicodeTokenizer(true)
    if err != nil {
        log.Fatalf("failed to create the wildcard tokenizer; %v", err)
    }

    http.HandleFunc("POST /register/start/", newRegisterStartHandler(scratch))
    http.HandleFunc("POST /register/finish/", newRegisterFinishHandler(db, scratch, tokenizer))
    http.HandleFunc("POST /deregister/start/", newDeregisterStartHandler(db, scratch))
    http.HandleFunc("POST /deregister/finish/", newDeregisterStartHandler(db, scratch))

    // Adding a per-hour job that purges various old files in the scratch.
    {
        ticker := time.NewTicker(time.Hour)
        defer ticker.Stop()
        go func() {
            for {
                <-ticker.C
                err := purgeOldFiles(scratch, time.Hour)
                if err != nil {
                    log.Println(err)
                }
            }
        }()
    }

    // Adding a per-day job that updates the paths.
    {
        ticker := time.NewTicker(24 * time.Hour)
        defer ticker.Stop()
        var ctx context.Context
        go func() {
            for {
                <-ticker.C
                fails, err := updatePaths(db, ctx, tokenizer)
                if err != nil {
                    log.Println(err)
                } else {
                    for _, f := range fails {
                        log.Println(f)
                    }
                }
            }
        }()
    }

    log.Fatal(http.ListenAndServe(":" + port, nil))
}
