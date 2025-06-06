package main

import (
    "testing"
    "strings"
)

func TestSanitizeQuery(t *testing.T) {
    deftok, _ := newUnicodeTokenizer(false) // err=nil should be tested elsewhere.
    wildtok, _ := newUnicodeTokenizer(true)

    t.Run("and", func(t *testing.T) {
        {
            query := &searchClause { Type: "and" }
            _, err := sanitizeQuery(query, deftok, wildtok)
            if err == nil || !strings.Contains(err.Error(), "non-empty 'children'") {
                t.Fatalf("expected an error about non-empty children")
            }
        }

        {
            query := &searchClause { Type: "and", Children: []*searchClause{} }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san != nil {
                t.Fatalf("expected a nil search clause with empty children")
            }
        }

        {
            query := &searchClause { Type: "and", Children: []*searchClause{ nil, nil } }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san != nil {
                t.Fatalf("expected a nil search clause with empty children")
            }
        }

        {
            query := &searchClause { Type: "and", Children: []*searchClause{ &searchClause{ Type: "text", Text: "Aaron" } } }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            // The child is sanitized correctly and promoted to the output.
            if san == nil || !(san.Type == "text" && san.Text == "aaron") {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            query := &searchClause { Type: "and", Children: []*searchClause{ &searchClause{ Type: "text", Text: "Aaron" }, &searchClause{ Type: "text", Text: "Lun" } } }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            // Each child is sanitized correctly.
            if san == nil || san.Type != "and" || len(san.Children) != 2 || !(san.Children[0].Type == "text" && san.Children[0].Text == "aaron") || !(san.Children[1].Type == "text" && san.Children[1].Text == "lun") {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            query := &searchClause { Type: "and", Children: []*searchClause{ &searchClause{ Type: "text", Text: "Aaron Lun" }, &searchClause{ Type: "text", Text: "Jayaram Kancherla" } } }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            // Each child is sanitized correctly and the nested ANDs are collapsed.
            if san == nil || san.Type != "and" || len(san.Children) != 4 || 
                !(san.Children[0].Type == "text" && san.Children[0].Text == "aaron") || 
                !(san.Children[1].Type == "text" && san.Children[1].Text == "lun") ||
                !(san.Children[2].Type == "text" && san.Children[2].Text == "jayaram") ||
                !(san.Children[3].Type == "text" && san.Children[3].Text == "kancherla") {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            query := &searchClause {
                Type: "and",
                Children: []*searchClause{ 
                    &searchClause{ Type: "and", Children: []*searchClause{ &searchClause{ Type: "text", Text: "whee" }, &searchClause{ Type: "text", Text: "stuff" } } },
                    &searchClause{ Type: "or",  Children: []*searchClause{ &searchClause{ Type: "text", Text: "foo" },  &searchClause{ Type: "text", Text: "bar" } } },
                },
            }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            // The nested AND is collapsed, but the OR is retained.
            if san == nil || san.Type != "and" || len(san.Children) != 3 || 
                !(san.Children[0].Type == "text" && san.Children[0].Text == "whee") || 
                !(san.Children[1].Type == "text" && san.Children[1].Text == "stuff") ||
                !(san.Children[2].Type == "or" && len(san.Children[2].Children) == 2) {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }
    })

    t.Run("or", func(t *testing.T) {
        query := &searchClause { 
            Type: "or",
            Children: []*searchClause{ 
                &searchClause{ Type: "and", Children: []*searchClause{ &searchClause{ Type: "text", Text: "whee" }, &searchClause{ Type: "text", Text: "stuff" } } },
                &searchClause{ Type: "or",  Children: []*searchClause{ &searchClause{ Type: "text", Text: "foo" },  &searchClause{ Type: "text", Text: "bar" } } },
            },
        }

        san, err := sanitizeQuery(query, deftok, wildtok)
        if err != nil {
            t.Fatalf(err.Error())
        }

        // Now the nested OR is collapsed, but the AND is retained.
        if san == nil || san.Type != "or" || len(san.Children) != 3 || 
            !(san.Children[0].Type == "and" && len(san.Children[0].Children) == 2) ||
            !(san.Children[1].Type == "text" && san.Children[1].Text == "foo") || 
            !(san.Children[2].Type == "text" && san.Children[2].Text == "bar") {
            t.Fatalf("unexpected result from sanitization %v", san)
        }
    })

    t.Run("not", func(t *testing.T) {
        {
            query := &searchClause {
                Type: "not",
                Child: &searchClause{ Type: "text", Text: "whee blah" },
            }

            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }

            if san == nil || san.Type != "not" || san.Child == nil || san.Child.Type != "and" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        // Nested 'not's are collapsed.
        {
            query := &searchClause {
                Type: "not",
                Child: &searchClause{ Type: "not", Child: &searchClause{ Type: "text", Text: "foobar" } },
            }

            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }

            if san == nil || san.Type != "text" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }
    })

    t.Run("text", func(t *testing.T) {
        {
            // Single token.
            query := &searchClause { Type: "text", Text: "FOOBAR" }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "text" || san.Text != "foobar" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            // Creates multiple tokens.
            query := &searchClause { Type: "text", Text: " Aaron Lun had\na little LAMB " }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "and" || len(san.Children) != 6 ||
                !(san.Children[0].Type == "text" && san.Children[0].Text == "aaron") ||
                !(san.Children[5].Type == "text" && san.Children[5].Text == "lamb") {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            // Tokenization returns nil.
            query := &searchClause { Type: "text", Text: "     " }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san != nil {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            // Tokenization returns nil.
            query := &searchClause { Type: "text", Text: "     " }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san != nil {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        t.Run("wildcards", func(t *testing.T) {
            // Wildcards are respected.
            query := &searchClause { Type: "text", Text: " Harvest*", IsPattern: true }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "text" || san.Text != "harvest*" || !san.IsPattern {
                t.Fatalf("unexpected result from sanitization %v", san)
            }

            query = &searchClause { Type: "text", Text: "mo?n ", IsPattern: true }
            san, err = sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "text" || san.Text != "mo?n" || !san.IsPattern {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        })

        {
            // Fields are respected.
            query := &searchClause { Type: "text", Text: "Aaron", Field: "first_name" }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "text" || san.Text != "aaron" || san.Field != "first_name" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }
    })

    t.Run("path", func(t *testing.T) {
        {
            query := &searchClause { Type: "path", Path: "foo/bar" }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "path" || san.Path != "*foo/bar*" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            query := &searchClause { Type: "path", Path: "foo/bar", IsSuffix: true }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "path" || san.Path != "*foo/bar" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            query := &searchClause { Type: "path", Path: "foo/bar", IsPrefix: true }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "path" || san.Path != "foo/bar*" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        t.Run("more wildcards", func(t *testing.T) {
            query := &searchClause { Type: "path", Path: "foo*ba?r" }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "path" || san.Path != "*foo*ba?r*" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        })
    })

    t.Run("other", func(t *testing.T) {
        {
            query := &searchClause { Type: "user", User: "LTLA" }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "user" || san.User != "LTLA" {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            query := &searchClause { Type: "time", Time: 12345 }
            san, err := sanitizeQuery(query, deftok, wildtok)
            if err != nil {
                t.Fatalf(err.Error())
            }
            if san == nil || san.Type != "time" || san.Time != 12345 {
                t.Fatalf("unexpected result from sanitization %v", san)
            }
        }

        {
            query := &searchClause { Type: "other" }
            _, err := sanitizeQuery(query, deftok, wildtok)
            if err == nil || !strings.Contains(err.Error(), "unknown") {
                t.Fatalf("expected an unknown type error")
            }
        }
    })
}
