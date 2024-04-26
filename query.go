package main

import (
    "fmt"
    "strings"
)

type searchClause struct {
    Type string `json:"type"`

    // Only relevant for type = path.
    // - Before sanitization: if Escape is empty, Path is assumed to contain a substring of the path, to be extended at the front/back depending on IsPrefix and IsSuffix.
    //   If Escape is not empty, Path is assumed to be a wildcard-containing pattern.
    // - After sanitization: Path is a wildcard-containing pattern.
    //   Escape may or may not be an empty string, depending on whether Path needed escaping of wildcard characters.
    //   IsPrefix and IsSuffix are no longer used.
    Path string `json:"path"`
    Escape string `json:"escape"`
    IsPrefix bool `json:"is_prefix"`
    IsSuffix bool `json:"is_suffix"`

    // Only relevant for type = user.
    User string `json:"user"`

    // Only relevant for type = time.
    Time int64 `json:"time"`
    After bool `json:"after"`

    // Only relevant for text.
    Text string `json:"text"`
    Field string `json:"field"`
    Partial bool `json:"partial"`

    // Only relevant for type = and/or.
    Children []*searchClause `json:"children"`

    // Only relevant for type = not.
    Child *searchClause `json:"child"`
}

func escapeWildcards(input string) (string, string, error) {
    all_characters := map[rune]bool{}
    for _, x := range input {
        all_characters[x] = true
    }

    _, has_under := all_characters['_']
    _, has_percent := all_characters['%']
    if !has_under && !has_percent {
        return input, "", nil
    }

    // Choosing an escape character for wildcards.
    var escape rune
    found_escape := false
    for _, candidate := range []rune{ '\\', '~', '!', '@', '#', '$', '^', '&' } {
        _, has_escape := all_characters[candidate]
        if !has_escape {
            escape = candidate
            found_escape = true
            break
        }
    }

    if !found_escape {
        return "", "", fmt.Errorf("failed to escape wildcards in %q", input)
    }
    escape_str := string(escape)

    // Need to escape all existing wildcards in the name.
    pattern := ""
    for _, x := range input {
        if x == '%' || x == '_' {
            pattern += escape_str
        }
        pattern += string(x)
    }

    return pattern, escape_str, nil
}

func sanitizeQuery(original *searchClause, deftok, wildtok *unicodeTokenizer) (*searchClause, error) {
    if original.Type == "and" || original.Type == "or" {
        if original.Children == nil {
            return nil, fmt.Errorf("search clauses of type %s should have non-empty 'children'", original.Type)
        }

        new_kids := []*searchClause{} 
        for _, x := range original.Children {
            if x == nil {
                continue
            }

            san, err := sanitizeQuery(x, deftok, wildtok)
            if err != nil {
                return nil, err
            }
            if san == nil {
                continue
            }

            if san.Type == original.Type {
                for _, grandkid := range san.Children {
                    new_kids = append(new_kids, grandkid)
                }
            } else {
                new_kids = append(new_kids, san)
            }
        }

        if len(new_kids) == 0 {
            return nil, nil
        } else if len(new_kids) == 1 {
            return new_kids[0], nil
        } else {
            return &searchClause { Type: original.Type, Children: new_kids }, nil
        }
    }

    if original.Type == "not" {
        if original.Child == nil {
            return nil, fmt.Errorf("search clause of type %q should have non-empty 'child'", original.Type)
        }

        san, err := sanitizeQuery(original.Child, deftok, wildtok)
        if err != nil {
            return nil, err
        }

        if san.Type == "not" {
            return san.Child, nil
        } else {
            return &searchClause { Type: original.Type, Child: san }, nil
        }
    }

    if original.Type == "text" {
        var tokens []string
        var err error
        if original.Partial {
            tokens, err = wildtok.Tokenize(original.Text)
        } else {
            tokens, err = deftok.Tokenize(original.Text)
        }
        if err != nil {
            return nil, fmt.Errorf("failed to tokenize %q; %w", original.Text, err)
        }
        if len(tokens) == 0 {
            return nil, nil
        }

        replacements := []*searchClause{}
        for _, tok := range tokens {
            replacements = append(replacements, &searchClause{ Type: "text", Partial: original.Partial, Field: original.Field, Text: tok })
        }
        if len(replacements) == 1 {
            return replacements[0], nil
        } 
        return &searchClause{ Type: "and", Children: replacements }, nil
    }

    if original.Type == "user" || original.Type == "time" {
        return original, nil
    }

    if original.Type == "path" {
        if original.Escape != "" {
            if len(original.Escape) != 1 {
                return nil, fmt.Errorf("'escape' must be a single character (got %s)", original.Escape)
            }
            return &searchClause { Type: "path", Path: original.Path, Escape: original.Escape }, nil
        } else {
            pattern, escape, err := escapeWildcards(original.Path)
            if err != nil {
                return nil, fmt.Errorf("failed to escape wildcards for path %q; %w", original.Path, err)
            }
            if !original.IsPrefix {
                pattern = "%" + pattern
            }
            if !original.IsSuffix {
                pattern += "%"
            }
            return &searchClause { Type: "path", Path: pattern, Escape: escape }, nil
        }
    }

    return nil, fmt.Errorf("unknown search type %q", original.Type)
}

func assembleFilter(query *searchClause) (string, []interface{}) {
    if query.Type == "text" {
        parameters := []interface{}{}

        filter := "paths.pid IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid"
        if query.Field != "" {
            filter += " LEFT JOIN fields ON fields.fid = links.fid WHERE fields.field = ? AND"
            parameters = append(parameters, query.Field)
        } else {
            filter += " WHERE"
        }

        filter += " tokens.token"
        if query.Partial {
            filter += " LIKE"
        } else {
            filter += " ="
        }
        filter += " ?"
        parameters = append(parameters, query.Text)
        filter += ")"

        return filter, parameters
    }

    if query.Type == "user" {
        return "paths.user = ?", []interface{}{ query.User }
    }

    if query.Type == "path" {
        if query.Escape != "" {
            return "paths.path LIKE ? ESCAPE ?", []interface{}{ query.Path, query.Escape }
        } else {
            return "paths.path LIKE ?", []interface{}{ query.Path }
        }
    }

    if query.Type == "time" {
        filter := "paths.time" 
        if query.After {
            filter += " >"
        } else {
            filter += " <="
        }
        filter += " ?"
        return filter, []interface{}{ query.Time }
    }

    if query.Type == "not" {
        curfilt, curpar := assembleFilter(query.Child)
        return "NOT " + curfilt, curpar
    }

    if query.Type == "and" {
        collected := []string{}
        parameters := []interface{}{}
        for _, child := range query.Children {
            curfilt, curpar := assembleFilter(child)
            collected = append(collected, curfilt)
            parameters = append(parameters, curpar...)
        }
        return "(" + strings.Join(collected, " AND ") + ")", parameters
    }

    // Implicitly, the rest is type 'or'.
    text := []*searchClause{}
    other := []*searchClause{}
    for _, child := range query.Children {
        if child.Type == "text" {
            text = append(text, child) 
        } else {
            other = append(other, child)
        }
    }

    collected := []string{}
    parameters := []interface{}{}

    if len(text) > 0 {
        subfilters := []string{}
        has_field := false

        for _, tchild := range text {
            current := "tokens.token"
            if tchild.Partial {
                current += " LIKE"
            } else {
                current += " ="
            }
            current += " ?"
            parameters = append(parameters, tchild.Text)

            if tchild.Field != "" {
                current = "(" + current + " AND fields.field = ?)"
                parameters = append(parameters, tchild.Field)
                has_field = true
            }

            subfilters = append(subfilters, current)
        }

        filter := "paths.pid IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid"
        if has_field {
            filter += " LEFT JOIN fields ON fields.fid = links.fid"
        }
        filter += " WHERE " + strings.Join(subfilters, " OR ") + ")"
        collected = append(collected, filter)
    }

    for _, ochild := range other {
        curfilt, curpar := assembleFilter(ochild)
        collected = append(collected, curfilt)
        parameters = append(parameters, curpar...)
    }
    return "(" + strings.Join(collected, " OR ") + ")", parameters
}
