package main

import (
    "flag"
    "log"
    "os"
    "time"
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

    err := os.MkdirAll(scratch, 0700)
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
    wild_tokenizer, err := newUnicodeTokenizer(true)
    if err != nil {
        log.Fatalf("failed to create the wildcard tokenizer; %v", err)
    }

    // Setting up the endpoints.
    http.HandleFunc("/register/start", newRegisterStartHandler(scratch))
    http.HandleFunc("/register/finish", newRegisterFinishHandler(db, scratch, tokenizer))
    http.HandleFunc("/deregister/start", newDeregisterStartHandler(db, scratch))
    http.HandleFunc("/deregister/finish", newDeregisterFinishHandler(db, scratch))
    http.HandleFunc("/query", newQueryHandler(db, tokenizer, wild_tokenizer, "/query"))

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
        go func() {
            for {
                <-ticker.C
                fails, err := updatePaths(db, tokenizer)
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

    // Adding another per-day job that does the backup.
    {
        ticker := time.NewTicker(24 * time.Hour)
        defer ticker.Stop()
        go func() {
            time.Sleep(time.Hour * 12) // start at a different cycle from the path updates.
            for {
                <-ticker.C
                err := backupDatabase(db, dbpath + ".backup")
                if err != nil {
                    log.Println(err)
                }
            }
        }()
    }

    log.Fatal(http.ListenAndServe("0.0.0.0:" + port, nil))
}
