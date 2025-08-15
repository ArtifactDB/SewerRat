package main

import (
    "testing"
    "path/filepath"
    "os"
    "os/user"
)

func TestIsLinkWhitelisted(t *testing.T) {
    tmp, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    self_user, err := user.Current()
    if err != nil {
        t.Fatal(err)
    }
    self_name := self_user.Username

    link_path := filepath.Join(tmp, "akari")
    err = os.Symlink("/foo/bar", link_path)
    if err != nil {
        t.Fatal(err)
    }

    link_info, err := os.Lstat(link_path)
    if err != nil {
        t.Fatal(err)
    }

    if !isLinkWhitelisted(link_info, linkWhitelist{ self_name: true }) {
        t.Error("expected link to be whitelisted")
    }
    if isLinkWhitelisted(link_info, linkWhitelist{}) {
        t.Error("expected link to not be whitelisted")
    }
}

func TestCreateLinkWhitelist(t *testing.T) {
    wl := createLinkWhitelist("")
    if len(wl) != 0 {
        t.Error("expected an empty whitelist from an empty string")
    }

    wl = createLinkWhitelist("foo")
    _, found := wl["foo"]
    if len(wl) != 1 || !found {
        t.Error("expected whitelist to contain only 'foo'")
    }

    wl = createLinkWhitelist("foo,bar")
    _, ffound := wl["foo"]
    _, bfound := wl["bar"]
    if len(wl) != 2 || !ffound || !bfound {
        t.Error("expected whitelist to contain 'foo' and 'bar'")
    }
}
