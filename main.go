package main

import (
    "flag"
    "log"
    "time"
    "net/http"
    "strconv"
)

func main() {
    dbpath0 := flag.String("db", "index.sqlite3", "Path to the SQLite file for the metadata")
    port0 := flag.Int("port", 8080, "Port to listen to for requests")
    backup0 := flag.Int("backup", 24, "Frequency of creating or updating back-ups, in hours")
    update0 := flag.Int("update", 24, "Frequency of updating the index by scanning registered directories, in hours")
    timeout0 := flag.Int("finish", 30, "Maximum time spent polling for the verification code when finishing (de)registration, in seconds")
    prefix0 := flag.String("prefix", "", "Prefix to add to each endpoint, after removing any leading or trailing slashes (default \"\")")
    lifetime0 := flag.Int("session", 10, "Maximum lifetime of a (de)registration session from start to finish, in minutes")
    flag.Parse()

    dbpath := *dbpath0
    port := *port0

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

    const num_verification_threads = 64
    verifier := newVerificationRegistry(num_verification_threads)

    prefix := *prefix0
    if prefix != "" {
        prefix = "/" + prefix
    }

    timeout := time.Duration(*timeout0) * time.Second

    // Setting up the endpoints.
    http.HandleFunc("POST " + prefix + "/register/start", newRegisterStartHandler(verifier))
    http.HandleFunc("POST " + prefix + "/register/finish", newRegisterFinishHandler(db, verifier, tokenizer, timeout))
    http.HandleFunc("POST " + prefix + "/deregister/start", newDeregisterStartHandler(db, verifier))
    http.HandleFunc("POST " + prefix + "/deregister/finish", newDeregisterFinishHandler(db, verifier, timeout))

    http.HandleFunc(prefix + "/registered", newListRegisteredDirectoriesHandler(db))
    http.HandleFunc(prefix + "/query", newQueryHandler(db, tokenizer, wild_tokenizer, "/query"))
    http.HandleFunc(prefix + "/retrieve/metadata", newRetrieveMetadataHandler(db))
    http.HandleFunc(prefix + "/retrieve/file", newRetrieveFileHandler(db))
    http.HandleFunc(prefix + "/list", newListFilesHandler(db))

    http.HandleFunc(prefix + "/", newDefaultHandler())

    // Adding a hour job that purges various old verification sessions.
    {
        lifetime := time.Duration(*lifetime0) * time.Minute
        ticker := time.NewTicker(lifetime)
        defer ticker.Stop()
        go func() {
            for {
                <-ticker.C
                verifier.Flush(lifetime)
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
                fails, err := updateDirectories(db, tokenizer)
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
        ticker := time.NewTicker(time.Duration(*backup0) * time.Hour)
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

    log.Fatal(http.ListenAndServe("0.0.0.0:" + strconv.Itoa(port), nil))
}
