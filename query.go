package main

import (
    "fmt"
    "strings"
)

type searchClause struct {
    Type string `json:"type"`

    // Only relevant for type = path.
    // - Before sanitization: if IsPattern = false, Path is assumed to contain a substring of the path, to be extended at the front/back depending on IsPrefix and IsSuffix.
    // - After sanitization: Path is a SQLite-wildcard-containing pattern.
    Path string `json:"path"`
    IsPrefix bool `json:"is_prefix"`
    IsSuffix bool `json:"is_suffix"`

    // Only relevant for type = user.
    // Unchanged before/after sanitization.
    User string `json:"user"`

    // Only relevant for type = time.
    // Unchanged before/after sanitization.
    Time int64 `json:"time"`
    After bool `json:"after"`

    // Only relevant for text.
    // - Before sanitization: Text may consist of multiple tokens, effectively combined with an AND statement.
    //   Each term may have conventional (non-SQLite) wildcards, i.e., ?, *.
    // - After sanitization: Text will consist of only one token.
    //   The token may contain SQLite wildcards if IsPattern = true, otherwise there will be no wildcards.
    Text string `json:"text"`
    Field string `json:"field"`
    IsPattern bool `json:"is_pattern"`

    // Only relevant for type = and/or.
    // - Before sanitization: any child may be an AND (for type = and) or OR (for type = or) clause, and there may be any number of children.
    // - After sanitization: no child will be an AND (for type = and) or OR (for type = or) clause as any nesting is flattened, and there must be at least two children.
    Children []*searchClause `json:"children"`

    // Only relevant for type = not.
    // - Before sanitization: the child may be a NOT clause.
    // - After sanitization: no child will be a NOT clause.
    Child *searchClause `json:"child"`
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
        if original.IsPattern {
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
            replacements = append(replacements, &searchClause{ Type: "text", IsPattern: original.IsPattern, Field: original.Field, Text: tok })
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
        pattern := original.Path
        if !original.IsPrefix && !strings.HasPrefix(pattern, "*") {
            pattern = "*" + pattern
        }
        if !original.IsSuffix && !strings.HasSuffix(pattern, "*") {
            pattern += "*"
        }
        return &searchClause { Type: "path", Path: pattern }, nil
    }

    return nil, fmt.Errorf("unknown search type %q", original.Type)
}

func assembleFilter(query *searchClause) (string, []interface{}) {
    if query.Type == "text" {
        parameters := []interface{}{}

        filter := "paths.pid IN (SELECT pid from links INNER JOIN tokens ON tokens.tid = links.tid"
        if query.Field != "" {
            filter += " INNER JOIN fields ON fields.fid = links.fid WHERE fields.field = ? AND"
            parameters = append(parameters, query.Field)
        } else {
            filter += " WHERE"
        }

        filter += " tokens.token"
        if query.IsPattern {
            filter += " GLOB"
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
        return "paths.path GLOB ?", []interface{}{ query.Path }
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
            if tchild.IsPattern {
                current += " GLOB"
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

        filter := "paths.pid IN (SELECT pid from links INNER JOIN tokens ON tokens.tid = links.tid"
        if has_field {
            filter += " INNER JOIN fields ON fields.fid = links.fid"
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
