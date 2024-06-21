package main

import (
    "strings"
    "testing"
)

func TestTranslateTextQuerySimple(t *testing.T) {
    out, err := translateTextQuery("foobar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "foobar" {
        t.Fatal("unexpected text query")
    }

    // Works with spaces.
    out, err = translateTextQuery("foo\nbar   whee")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || out.Children[0].Text != "foo" || out.Children[1].Text != "bar" || out.Children[2].Text != "whee" {
        t.Fatal("unexpected text query")
    }

    // Works with fields.
    out, err = translateTextQuery("stuff:blah")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "blah" || out.Field != "stuff" {
        t.Fatal("unexpected text query")
    }

    // Works with a space between the colon.
    out, err = translateTextQuery("stuff: yay blah")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || 
        out.Children[0].Text != "yay" || out.Children[0].Field != "stuff" || 
        out.Children[1].Text != "blah" || out.Children[1].Field != "stuff" {
        t.Fatal("unexpected text query")
    }

    // Recognizes partial hits.
    out, err = translateTextQuery("foo%")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "foo%" || !out.Partial {
        t.Fatal("unexpected text query")
    }

    out, err = translateTextQuery("foo% bar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || 
        out.Children[0].Text != "foo%" || !out.Children[0].Partial ||
        out.Children[1].Text != "bar" || out.Children[1].Partial {
        t.Fatal("unexpected text query")
    }

    // Fails correctly.
    out, err = translateTextQuery("\n")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateTextQueryNot(t *testing.T) {
    out, err := translateTextQuery("NOT foobar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "not" || out.Child.Type != "text" || out.Child.Text != "foobar" {
        t.Fatal("unexpected NOT query")
    }

    // Works with multiple words.
    out, err = translateTextQuery("NOT foo bar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "not" || out.Child.Type != "and" || out.Child.Children[0].Text != "foo" || out.Child.Children[1].Text != "bar" {
        t.Fatal("unexpected NOT query")
    }

    // Adding some parentheses and spaces.
    out, err = translateTextQuery("NOT ( foobar )")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "not" || out.Child.Type != "text" || out.Child.Text != "foobar" {
        t.Fatal("unexpected NOT query")
    }

    // Fails correctly.
    out, err = translateTextQuery("NOT ")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("foo NOT bar") // ... should be foo AND NOT bar
    if err == nil || strings.Index(err.Error(), "illegal placement") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("(foo) NOT (bar)") // ... should be (foo) AND NOT (bar)
    if err == nil || strings.Index(err.Error(), "illegal placement") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("(foo)NOT(bar)") // ... should be (foo) AND NOT (bar)
    if err == nil || strings.Index(err.Error(), "illegal placement") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("(foo)NOT bar") // ... should be (foo) AND NOT bar
    if err == nil || strings.Index(err.Error(), "illegal placement") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateTextQueryAnd(t *testing.T) {
    out, err := translateTextQuery("foo AND bar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected AND query")
    }

    // Works with multiple words and conditions.
    out, err = translateTextQuery("foo bar AND whee stuff AND other")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 3 || 
        out.Children[0].Type != "and" || out.Children[0].Children[0].Text != "foo" || out.Children[0].Children[1].Text != "bar" ||
        out.Children[1].Type != "and" || out.Children[1].Children[0].Text != "whee" || out.Children[1].Children[1].Text != "stuff" ||
        out.Children[2].Type != "text" || out.Children[2].Text != "other" {
        t.Fatal("unexpected AND query")
    }

    // Works with parentheses.
    out, err = translateTextQuery("(foo) AND (bar)")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected AND query")
    }

    // Fails correctly.
    out, err = translateTextQuery("AND")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("asdasd AND")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("AND asdasd")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateTextQueryOr(t *testing.T) {
    out, err := translateTextQuery("foo OR bar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected OR query")
    }

    // Works with multiple words and conditions.
    out, err = translateTextQuery("foo bar OR whee stuff OR other")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 3 || 
        out.Children[0].Type != "and" || out.Children[0].Children[0].Text != "foo" || out.Children[0].Children[1].Text != "bar" ||
        out.Children[1].Type != "and" || out.Children[1].Children[0].Text != "whee" || out.Children[1].Children[1].Text != "stuff" ||
        out.Children[2].Type != "text" || out.Children[2].Text != "other" {
        t.Fatal("unexpected OR query")
    }

    // Works with parentheses.
    out, err = translateTextQuery("(foo) OR (bar)")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected OR query")
    }

    // Fails correctly.
    out, err = translateTextQuery("OR")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("asdasd OR")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateTextQuery("OR asdasd")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateTextQueryComplex(t *testing.T) {
    out, err := translateTextQuery("foo AND bar OR NOT whee")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 2 || 
        out.Children[0].Type != "and" || len(out.Children[0].Children) != 2 || 
        out.Children[0].Children[0].Type != "text" || out.Children[0].Children[0].Text != "foo" || 
        out.Children[0].Children[1].Type != "text" || out.Children[0].Children[1].Text != "bar" ||
        out.Children[1].Type != "not" || out.Children[1].Child.Type != "text" || out.Children[1].Child.Text != "whee" {
        t.Fatal("unexpected complex query")
    }

    out, err = translateTextQuery("foo AND (bar OR NOT (whee))")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 2 || 
        out.Children[0].Type != "text" || out.Children[0].Text != "foo" ||
        out.Children[1].Type != "or" || len(out.Children[1].Children) != 2 || 
        out.Children[1].Children[0].Type != "text" || out.Children[1].Children[0].Text != "bar" || 
        out.Children[1].Children[1].Type != "not" || out.Children[1].Children[1].Child.Type != "text" || out.Children[1].Children[1].Child.Text != "whee" {
        t.Fatal("unexpected complex query")
    }
}

func TestTranslateQuery(t *testing.T) {
    out, err := translateQuery(&searchClause{ Type: "text", Text: "(foo AND bar) OR (NOT whee)" })
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 2 || 
        out.Children[0].Type != "and" || len(out.Children[0].Children) != 2 || 
        out.Children[0].Children[0].Type != "text" || out.Children[0].Children[0].Text != "foo" || 
        out.Children[0].Children[1].Type != "text" || out.Children[0].Children[1].Text != "bar" ||
        out.Children[1].Type != "not" || out.Children[1].Child.Type != "text" || out.Children[1].Child.Text != "whee" {
        t.Fatal("unexpected translation of a full query")
    }

    // Works with AND operations.
    out, err = translateQuery(&searchClause{ Type: "and", Children: []*searchClause{ &searchClause{ Type: "user", User: "Aaron" }, &searchClause{ Type: "text", Text: "NOT whee" } } })
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 2 || 
        out.Children[0].Type != "user" ||
        out.Children[1].Type != "not" || out.Children[1].Child.Type != "text" || out.Children[1].Child.Text != "whee" {
        t.Fatal("unexpected translation of a full query")
    }

    // Works with OR operations.
    out, err = translateQuery(&searchClause{ Type: "or", Children: []*searchClause{ &searchClause{ Type: "text", Text: "NOT whee" }, &searchClause{ Type: "time", Time: 123456 } } })
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 2 || 
        out.Children[0].Type != "not" || out.Children[0].Child.Type != "text" || out.Children[0].Child.Text != "whee" ||
        out.Children[1].Type != "time" {
        t.Fatal("unexpected translation of a full query")
    }

    // Works with NOT operations.
    out, err = translateQuery(&searchClause{ Type: "not", Child: &searchClause{ Type: "text", Text: "aaron OR foo" } })
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "not" || out.Child.Type != "or" {
        t.Fatal("unexpected translation of a full query")
    }
}
