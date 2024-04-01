package main

import (
    "fmt"
    "strings"
    "unicode"
)

func translateStringQuery(query string) (*searchClause, error) {
    out, _, err := translateStringQueryInternal([]rune(query), 0, false)
    return out, err
}

type translationStatus struct {
    Word []rune
    Words [][]rune
    Clauses []*searchClause
    Operations []string
    Negation bool
}

func isWordEqualTo(word []rune, target string) bool {
    if len(word) != len(target) {
        return false
    }

    // Loop over 'target' so we can easily extract runes.
    for i, x := range target {
        if x != word[i] {
            return false
        }
    }

    return true
}

func translateStringQueryInternal(query []rune, at int, open_par bool) (*searchClause, int, error) {
    var status translationStatus 
    status.Word = []rune{}
    status.Words = [][]rune{}
    status.Clauses = []*searchClause{}
    status.Operations = []string{}

    closing_par := false;
    original := at

    for at < len(query) {
        c := query[at];

        if c == '(' {
            if isWordEqualTo(status.Word, "AND") || isWordEqualTo(status.Word, "OR") {
                err := translateAddOperation(&status, at)
                if err != nil {
                    return nil, 0, err
                }
            } else if isWordEqualTo(status.Word, "NOT") {
                status.Negation = true
                status.Word = []rune{}
            } else if len(status.Word) > 0 || len(status.Words) > 0 {
                return nil, 0, fmt.Errorf("search clauses must be separated by AND or OR at position %d", at)
            }

            nested, at2, err := translateStringQueryInternal(query, at + 1, true)
            if err != nil {
                return nil, 0, err
            }

            at = at2
            if status.Negation {
                nested = &searchClause{ Type: "not", Child: nested }
                status.Negation = false
            }
            status.Clauses = append(status.Clauses, nested)
            continue
        }

        if c == ')' {
            if !open_par {
                return nil, 0, fmt.Errorf("unmatched closing parenthesis at position %d", at)
            }
            closing_par = true
            at++
            break
        }

        if unicode.IsSpace(c) {
            if isWordEqualTo(status.Word, "AND") || isWordEqualTo(status.Word, "OR") {
                err := translateAddOperation(&status, at)
                if err != nil {
                    return nil, 0, err
                }
            } else if isWordEqualTo(status.Word, "NOT") {
                if len(status.Words) > 0 || len(status.Operations) < len(status.Clauses) {
                    return nil, 0, fmt.Errorf("illegal placement of NOT at position %d", at) 
                }
                status.Negation = true
                status.Word = []rune{}
            } else if len(status.Word) > 0 {
                status.Words = append(status.Words, status.Word)
                status.Word = []rune{}
            }
        } else {
            status.Word = append(status.Word, c)
        }

        at++
    }

    if len(status.Operations) == len(status.Clauses) {
        if len(status.Word) > 0 {
            if isWordEqualTo(status.Word, "AND") || isWordEqualTo(status.Word, "OR") {
                return nil, 0, fmt.Errorf("trailing AND/OR at position %d", at)
            }
            status.Words = append(status.Words, status.Word)
        }
        err := translateTextClause(&status, at)
        if err != nil {
            return nil, 0, err
        }
    }

    if open_par && !closing_par {
        return nil, 0, fmt.Errorf("unmatched opening parenthesis at position %d", original - 1)
    }

    // Finding the stretches of ANDs first.
    if len(status.Operations) > 0 {
        tmp_clauses := [][]*searchClause{}
        active_clauses := []*searchClause{ status.Clauses[0] }
        for o, op := range status.Operations {
            if op == "AND" {
                active_clauses = append(active_clauses, status.Clauses[o + 1])
            } else {
                tmp_clauses = append(tmp_clauses, active_clauses)
                active_clauses = []*searchClause{ status.Clauses[o + 1] }
            }
        }
        tmp_clauses = append(tmp_clauses, active_clauses)

        status.Clauses = []*searchClause{}
        for _, tmp := range tmp_clauses {
            if len(tmp) > 1 {
                status.Clauses = append(status.Clauses, &searchClause{ Type: "and", Children: tmp })
            } else {
                status.Clauses = append(status.Clauses, tmp[0])
            }
        }
    }

    // Finally, resolving the ORs.
    var output *searchClause
    if len(status.Clauses) > 1 {
        output = &searchClause{ Type: "or", Children: status.Clauses }
    } else {
        output = status.Clauses[0]
    }
    return output, at, nil
}

func translateTextClause(status *translationStatus, at int) error {
    new_component := &searchClause{}
    new_component.Type = "text"

    if len(status.Words) == 0 {
        return fmt.Errorf("no search terms at position %d", at)
    }

    first_word := status.Words[0]
    fi := -1
    for i, x := range first_word {
        if x == ':' {
            fi = i
        }
    }
    if fi == 0 {
        return fmt.Errorf("search field should be non-empty for terms ending at %d", at)
    }

    if fi > 0 {
        new_component.Field = string(first_word[:fi])
        leftover := first_word[fi+1:]
        if len(leftover) == 0 {
            if len(status.Words) == 1 {
                return fmt.Errorf("no search terms at position %d after removing the search field", at)
            }
            status.Words = status.Words[1:]
        } else {
            status.Words[0] = leftover
        }
    }

    converted := []string{}
    for _, x := range status.Words {
        converted = append(converted, string(x))
    }
    new_component.Text = strings.Join(converted, " ")
    if strings.Index(new_component.Text, "%") >= 0 {
        new_component.Partial = true
    }

    if status.Negation {
        // Two-step replacement, otherwise it becomes a circular reference.
        new_component = &searchClause{ Type: "not", Child: new_component }
        status.Negation = false
    }

    status.Clauses = append(status.Clauses, new_component)
    status.Words = [][]rune{}
    return nil
}

func translateAddOperation(status *translationStatus, at int) error {
    if len(status.Operations) > len(status.Clauses) {
        return fmt.Errorf("multiple AND or OR keywords at position %d", at)
    }

    if len(status.Operations) == len(status.Clauses) {
        // Operations are binary, so if there wasn't already a preceding
        // clause, then we must try to add a text clause.
        err := translateTextClause(status, at)
        if err != nil {
            return err
        }
    }

    status.Operations = append(status.Operations, string(status.Word))
    status.Word = []rune{}
    return nil
}
