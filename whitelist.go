package main

import (
    "os"
    "fmt"
    "strings"
    "bufio"
    "path/filepath"
)

type linkWhitelist = map[string]map[string]bool

func isLinkWhitelisted(link_path, target_path string, whitelist linkWhitelist) bool {
    for dir, allowed_users := range whitelist {
        rel, err := filepath.Rel(dir, target_path)
        if err == nil && filepath.IsLocal(rel) {
            // if it's nil, any user is allowed.
            if allowed_users == nil {
                return true
            }

            // Otherwise, we check if the link itself was created by the approved subset of users.
            link_stat, err := os.Lstat(link_path)
            if err == nil {
                user, err := identifyUser(link_stat)
                if err == nil {
                    _, found := allowed_users[user]
                    return found
                }
            }

            return false
        }
    }
    return false
}

func loadLinkWhitelist(path string) (linkWhitelist, error) {
    whandle, err := os.Open(path)
    if err != nil {
        return nil, fmt.Errorf("failed to open the whitelist file; %v", err)
    }
    defer whandle.Close()

    output := linkWhitelist{}
    scanner := bufio.NewScanner(whandle)
    for scanner.Scan() {
        values := strings.Split(scanner.Text(), ",")
        if len(values) == 1 {
            output[values[0]] = nil
        } else {
            allowed_users := map[string]bool{}
            for i := 1; i < len(values); i++ {
                allowed_users[values[i]] = true
            }
            output[values[0]] = allowed_users
        }
    }

    if err := scanner.Err(); err != nil {
        return nil, fmt.Errorf("failed to parse the whitelist file; %v", err)
    }
    return output, nil
}
