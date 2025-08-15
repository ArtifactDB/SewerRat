package main

import (
    "io/fs"
    "strings"
)

type linkWhitelist = map[string]bool

func isLinkWhitelisted(link_info fs.FileInfo, whitelist linkWhitelist) bool {
    user, err := identifyUser(link_info)
    if err != nil {
        return false
    }
    _, ok := whitelist[user]
    return ok
}

func createLinkWhitelist(users string) linkWhitelist {
    output := linkWhitelist{}
    if (users != "") {
        all_users := strings.Split(users, ",")
        for _, user := range all_users {
            output[user] = true
        }
    }
    return output
}
