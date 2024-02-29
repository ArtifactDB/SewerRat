package main

import (
    "io/fs"
    "fmt"
    "errors"
    "syscall"
    "strconv"
    "os/user"
)

func identifyUser(info fs.FileInfo) (string, error) {
    stat, ok := info.Sys().(*syscall.Stat_t)
    if !ok {
        return "", errors.New("failed to extract system information");
    }

    uinfo, err := user.LookupId(strconv.Itoa(int(stat.Uid)))
    if !ok {
        return "", fmt.Errorf("failed to find user name for author; %w", err)
    }
    return uinfo.Username, nil
}
