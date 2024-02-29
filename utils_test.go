package main

func equalStringArrays(x []string, y []string) bool {
    if (x == nil) != (y == nil) {
        return false
    }
    if x == nil {
        return true
    }

    if len(x) != len(y) {
        return false
    }
    for i, v := range x {
        if v != y[i] {
            return false
        }
    }

    return true
}
