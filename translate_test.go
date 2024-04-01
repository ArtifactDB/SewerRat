package main

import (
    "strings"
    "testing"
)

func TestTranslateStringQuerySimple(t *testing.T) {
    out, err := translateStringQuery("foobar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "foobar" {
        t.Fatal("unexpected text query")
    }

    // Works with spaces.
    out, err = translateStringQuery("foo\nbar   whee")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "foo bar whee" {
        t.Fatal("unexpected text query")
    }

    // Works with fields.
    out, err = translateStringQuery("stuff:blah")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "blah" || out.Field != "stuff" {
        t.Fatal("unexpected text query")
    }

    // Works with a space between the colon.
    out, err = translateStringQuery("stuff: yay blah")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "yay blah" || out.Field != "stuff" {
        t.Fatal("unexpected text query")
    }

    // Recognizes partial hits.
    out, err = translateStringQuery("foo%")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "text" || out.Text != "foo%" || !out.Partial {
        t.Fatal("unexpected text query")
    }

    // Fails correctly.
    out, err = translateStringQuery("\n")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateStringQueryNot(t *testing.T) {
    out, err := translateStringQuery("NOT foobar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "not" || out.Child.Type != "text" || out.Child.Text != "foobar" {
        t.Fatal("unexpected NOT query")
    }

    // Works with multiple words.
    out, err = translateStringQuery("NOT foo bar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "not" || out.Child.Type != "text" || out.Child.Text != "foo bar" {
        t.Fatal("unexpected NOT query")
    }

    // Adding some parentheses and spaces.
    out, err = translateStringQuery("NOT ( foobar )")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "not" || out.Child.Type != "text" || out.Child.Text != "foobar" {
        t.Fatal("unexpected NOT query")
    }

    // Fails correctly.
    out, err = translateStringQuery("NOT ")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateStringQuery("foo NOT bar") // ... should be foo AND NOT bar
    if err == nil || strings.Index(err.Error(), "illegal placement") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateStringQuery("(foo) NOT (bar)") // ... should be (foo) AND NOT (bar)
    if err == nil || strings.Index(err.Error(), "illegal placement") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateStringQueryAnd(t *testing.T) {
    out, err := translateStringQuery("foo AND bar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected AND query")
    }

    // Works with multiple words and conditions.
    out, err = translateStringQuery("foo bar AND whee stuff AND other")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 3 || 
        out.Children[0].Type != "text" || out.Children[0].Text != "foo bar" || 
        out.Children[1].Type != "text" || out.Children[1].Text != "whee stuff" ||
        out.Children[2].Type != "text" || out.Children[2].Text != "other" {
        t.Fatal("unexpected AND query")
    }

    // Works with parentheses.
    out, err = translateStringQuery("(foo) AND (bar)")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "and" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected AND query")
    }

    // Fails correctly.
    out, err = translateStringQuery("AND")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateStringQuery("asdasd AND")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateStringQuery("AND asdasd")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateStringQueryOr(t *testing.T) {
    out, err := translateStringQuery("foo OR bar")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected OR query")
    }

    // Works with multiple words and conditions.
    out, err = translateStringQuery("foo bar OR whee stuff OR other")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 3 || 
        out.Children[0].Type != "text" || out.Children[0].Text != "foo bar" || 
        out.Children[1].Type != "text" || out.Children[1].Text != "whee stuff" ||
        out.Children[2].Type != "text" || out.Children[2].Text != "other" {
        t.Fatal("unexpected OR query")
    }

    // Works with parentheses.
    out, err = translateStringQuery("(foo) OR (bar)")
    if err != nil {
        t.Fatal(err)
    }
    if out.Type != "or" || len(out.Children) != 2 || out.Children[0].Type != "text" || out.Children[0].Text != "foo" || out.Children[1].Type != "text" || out.Children[1].Text != "bar" {
        t.Fatal("unexpected OR query")
    }

    // Fails correctly.
    out, err = translateStringQuery("OR")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateStringQuery("asdasd OR")
    if err == nil || strings.Index(err.Error(), "trailing") < 0 {
        t.Fatal("should have failed")
    }

    out, err = translateStringQuery("OR asdasd")
    if err == nil || strings.Index(err.Error(), "no search terms") < 0 {
        t.Fatal("should have failed")
    }
}

func TestTranslateStringQueryComplex(t *testing.T) {
    out, err := translateStringQuery("foo AND bar OR NOT whee")
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

    out, err = translateStringQuery("foo AND (bar OR NOT (whee))")
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
