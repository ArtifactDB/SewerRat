package main

import (
    "flag"
    "log"
    "time"
    "net/http"
    "strconv"
    "fmt"
    "os"
)

func main() {
    dbpath0 := flag.String("db", "index.sqlite3", "Path to the SQLite file for the metadata")
    port0 := flag.Int("port", 8080, "Port to listen to for requests")
    backup0 := flag.Int("backup", 24, "Frequency of creating or updating back-ups, in hours")
    update0 := flag.Int("update", 24, "Frequency of updating the index by scanning registered directories, in hours")
    timeout0 := flag.Int("finish", 30, "Maximum time spent polling for the verification code when finishing (de)registration, in seconds")
    prefix0 := flag.String("prefix", "", "Prefix to add to each endpoint, after removing any leading or trailing slashes (default \"\")")
    lifetime0 := flag.Int("session", 10, "Maximum lifetime of a (de)registration session from start to finish, in minutes")
    checkpoint0 := flag.Int("checkpoint", 60, "Frequency of checkpoints to synchronise the WAL journal with the SQLite file, in minutes")
    concurrency0 := flag.Int("concurrency", 10, "Number of concurrent reads from the filesystem.")
    flag.Parse()

    dbpath := *dbpath0
    port := *port0

    db, err := initializeDatabase(dbpath)
    if err != nil {
        fmt.Printf("failed to initialize SQLite file at %q; %v\n", dbpath, err)
        os.Exit(1)
    }
    defer db.Close()

    ro_db, err := initializeReadOnlyDatabase(dbpath)
    if err != nil {
        fmt.Printf("failed to create read-only connections to %q; %v\n", dbpath, err)
        os.Exit(1)
    }
    defer ro_db.Close()

    tokenizer, err := newUnicodeTokenizer(false)
    if err != nil {
        fmt.Printf("failed to create the default tokenizer; %v\n", err)
        os.Exit(1)
    }
    wild_tokenizer, err := newUnicodeTokenizer(true)
    if err != nil {
        fmt.Printf("failed to create the wildcard tokenizer; %v\n", err)
        os.Exit(1)
    }

    verifier := newVerificationRegistry(time.Duration(*lifetime0) * time.Minute)

    prefix := *prefix0
    if prefix != "" {
        prefix = "/" + prefix
    }

    timeout := time.Duration(*timeout0) * time.Second

    // Setting up the endpoints.
    http.HandleFunc("POST " + prefix + "/register/start", newRegisterStartHandler(verifier))
    http.HandleFunc("POST " + prefix + "/register/finish", newRegisterFinishHandler(db, verifier, tokenizer, *concurrency0, timeout))
    http.HandleFunc("POST " + prefix + "/deregister/start", newDeregisterStartHandler(db, verifier))
    http.HandleFunc("POST " + prefix + "/deregister/finish", newDeregisterFinishHandler(db, verifier, timeout))

    http.HandleFunc("GET " + prefix + "/registered", newListRegisteredDirectoriesHandler(ro_db))
    http.HandleFunc("POST " + prefix + "/query", newQueryHandler(ro_db, tokenizer, wild_tokenizer, "/query"))
    http.HandleFunc("GET " + prefix + "/retrieve/metadata", newRetrieveMetadataHandler(ro_db))
    http.HandleFunc("GET " + prefix + "/retrieve/file", newRetrieveFileHandler(ro_db))
    http.HandleFunc("HEAD " + prefix + "/retrieve/file", newRetrieveFileHandler(ro_db))
    http.HandleFunc("GET " + prefix + "/list", newListFilesHandler(ro_db))

    http.HandleFunc("GET " + prefix + "/", newDefaultHandler())
    http.HandleFunc("OPTIONS " + prefix + "/", newOptionsHandler())

    // Adding an hourly job that does a full checkpoint.
    {
        lifetime := time.Duration(*checkpoint0) * time.Minute
        ticker := time.NewTicker(lifetime)
        defer ticker.Stop()
        go func() {
            for {
                <-ticker.C
                _, err := db.Exec("PRAGMA wal_checkpoint(PASSIVE)")
                if err != nil {
                    log.Printf("[ERROR] failed to perform WAL checkpoint; %v\n", err)
                }
            }
        }()
    }

    // Adding a per-day job that updates the paths.
    {
        ticker := time.NewTicker(time.Duration(*update0) * time.Hour)
        defer ticker.Stop()
        go func() {
            for {
                <-ticker.C
                fails, err := updateDirectories(db, tokenizer, *concurrency0)
                if err != nil {
                    log.Printf("[ERROR] failed to update directories; %v\n", err.Error())
                } else {
                    for _, f := range fails {
                        log.Printf("update failure: %s\n", f)
                    }
                }
            }
        }()
    }

    // Adding another per-day job that does the backup.
    {
        ticker := time.NewTicker(time.Duration(*backup0) * time.Hour)
        defer ticker.Stop()
        go func() {
            time.Sleep(time.Hour * 12) // start at a different cycle from the path updates.
            for {
                <-ticker.C
                err := backupDatabase(db, dbpath + ".backup")
                if err != nil {
                    log.Printf("[ERROR] failed to back up database; %v\n", err)
                }
            }
        }()
    }

    log.Fatal(http.ListenAndServe("0.0.0.0:" + strconv.Itoa(port), nil))
}
