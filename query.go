package main

import (
    "errors"
    "fmt"
    "strings"
)

type searchClause struct {
    Type string `json:"type"`
    Text string `json:"text"`
    Field string `json:"field"`
    Partial bool `json:"partial"`
    Children []*searchClause `json:"children"`
}

func sanitizeQuery(original *searchClause, deftok, wildtok *unicodeTokenizer) (*searchClause, error) {
    if original.Type == "and" || original.Type == "or" {
        if original.Children == nil {
            return nil, fmt.Errorf("search clauses of type %s should have non-empty 'children'", original.Type)
        }

        new_kids := []*searchClause{} 
        for _, x := range original.Children {
            if x == nil {
                return nil, errors.New("'children' should not contain null values")
            }

            san, err := sanitizeQuery(x, deftok, wildtok)
            if err != nil {
                return nil, err
            }
            if san != nil {
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

    if original.Type != "text" {
        return nil, fmt.Errorf("unknown search clause type %q", original.Type)
    }

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

func assembleFilter(query *searchClause, parameters []string) string {
    if query.Type == "text" {
        filter := "paths.pid IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid"
        if query.Field != "" {
            filter += " LEFT JOIN fields ON fields.fid = links.fid WHERE fields.field = ?"
            parameters = append(parameters, query.Text)
        } else {
            filter += " WHERE"
        }

        if query.Partial {
            filter += " tokens.token LIKE ?"
        } else {
            filter += " tokens.token = ?"
        }
        parameters = append(parameters, query.Text)
        filter += ")"

        return filter
    }

    if query.Type == "and" {
        collected := []string{}
        for _, child := range query.Children {
            collected = append(collected, assembleFilter(child, parameters))
        }
        return "(" + strings.Join(collected, " AND ") + ")"
    }

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
        filter += " WHERE" + strings.Join(subfilters, " OR ")
        collected = append(collected, filter)
    }

    for _, ochild := range other {
        collected = append(collected, assembleFilter(ochild, parameters))
    }
    return "(" + strings.Join(collected, " OR ") + ")"
}
