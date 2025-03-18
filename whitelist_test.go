package main

import (
    "testing"
    "path/filepath"
    "os"
)

func TestIsLinkWhitelisted(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    t.Run("simple", func(t *testing.T) {
        link_path := filepath.Join(tmp, "akari")
        err := os.Symlink("/foo/bar", link_path)
        if err != nil {
            t.Fatal(err)
        }
        if !isLinkWhitelisted(link_path, "/foo/bar", linkWhitelist{ "/foo/": nil }) {
            t.Error("expected link to be whitelisted")
        }

        // Still works if there's multiple options.
        err = os.Remove(link_path)
        if err != nil {
            t.Fatal(err)
        }
        err = os.Symlink("/foo/bar", link_path)
        if err != nil {
            t.Fatal(err)
        }
        if !isLinkWhitelisted(link_path, "/foo/bar", linkWhitelist{ "/bar": nil, "/foo": nil }) {
            t.Error("expected link to be whitelisted")
        }

        // Still works when the link destination is missing a trailing slash.
        err = os.Remove(link_path)
        if err != nil {
            t.Fatal(err)
        }
        err = os.Symlink("/foo", link_path)
        if err != nil {
            t.Fatal(err)
        }
        if !isLinkWhitelisted(link_path, "/foo", linkWhitelist{ "/foo/": nil }) {
            t.Error("expected link to be whitelisted")
        }

        // Rejects it if the link destination is outside of bounds.
        if isLinkWhitelisted(link_path, "/foo", linkWhitelist{ "/bar/": nil }) {
            t.Error("expected link to not be whitelisted")
        }
    })

    t.Run("user-restricted", func(t *testing.T) {
        lstat, err := os.Lstat(tmp)
        if err != nil {
            t.Fatal(err)
        }

        self, err := identifyUser(lstat)
        if err != nil {
            t.Fatal(err)
        }

        link_path := filepath.Join(tmp, "alicia")
        err = os.Symlink("/foo/bar", link_path)
        if err != nil {
            t.Fatal(err)
        }

        if isLinkWhitelisted(link_path, "/foo/bar", linkWhitelist{ "/foo/": map[string]bool{}}) {
            t.Error("expected link to not be whitelisted under the wrong user")
        }

        // Just as a control:
        if !isLinkWhitelisted(link_path, "/foo/bar", linkWhitelist{ "/foo/": map[string]bool{ self: true }}) {
            t.Error("expected link to be whitelisted")
        }
    })
}

func TestLoadLinkWhitelist(t *testing.T) {
    other, err := os.CreateTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    message := "/alpha/\n/bravo/,aika,alice\n/charlie/delta/,athena\n/echo/foxtrot/golf"
    if _, err := other.WriteString(message); err != nil {
        t.Fatal(err)
    }
    other_name := other.Name()
    if err := other.Close(); err != nil {
        t.Fatal(err)
    }

    loaded, err := loadLinkWhitelist(other_name)
    if err != nil {
        t.Fatal(err)
    }

    if len(loaded) != 4 {
        t.Error("unexpected content from the loaded whitelist file")
    }

    ahits, afound := loaded["/alpha/"]
    if !afound || ahits != nil {
        t.Error("unexpected content for alpha")
    }

    bhits, bfound := loaded["/bravo/"]
    if !bfound || len(bhits) != 2 || !bhits["aika"] || !bhits["alice"] {
        t.Error("unexpected content for bravo")
    }

    chits, cfound := loaded["/charlie/delta/"]
    if !cfound || len(chits) != 1 || !chits["athena"] {
        t.Error("unexpected content for charlie")
    }

    ehits, efound := loaded["/echo/foxtrot/golf"]
    if !efound || ehits != nil {
        t.Error("unexpected content for echo")
    }
}
