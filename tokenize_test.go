package main

import (
    "testing"
)

func TestUnicodeTokenizer(t *testing.T) {
    t.Run("basic", func(t *testing.T) {
        tok, err := newUnicodeTokenizer(false)
        if err != nil {
            t.Fatalf(err.Error())
        }

        // Variety of whitespace and cases.
        {
            out, err := tok.Tokenize(" Aaron\thad a little\n lamb ")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !equalStringArrays(out, []string{ "aaron", "had", "a", "little", "lamb" }) {
                t.Fatalf("incorrect tokenization")
            }
        }

        // Dashes and numbers.
        {
            out, err := tok.Tokenize("F-35 Lightning II Joint Strike Fighter")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !equalStringArrays(out, []string{ "f-35", "lightning", "ii", "joint", "strike", "fighter" }) {
                t.Fatalf("incorrect tokenization")
            }
        }

        // Pure numbers.
        {
            out, err := tok.Tokenize("1\n22\n333\n4444\n")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !equalStringArrays(out, []string{ "1", "22", "333", "4444" }) {
                t.Fatalf("incorrect tokenization")
            }
        }

        // Our arch nemesis, diacritics.
        {
            out, err := tok.Tokenize("Amélie,Zoë,Siân,François")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !equalStringArrays(out, []string{ "amelie", "zoe", "sian", "francois" }) {
                t.Fatalf("incorrect tokenization %q", out)
            }
        }

        // Removes duplicates.
        {
            out, err := tok.Tokenize("Aaron and AARON and aaron")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !equalStringArrays(out, []string{ "aaron", "and" }) {
                t.Fatalf("incorrect tokenization")
            }
        }
    })

    t.Run("wildcard", func(t *testing.T) {
        tok, err := newUnicodeTokenizer(true)
        if err != nil {
            t.Fatalf(err.Error())
        }

        {
            out, err := tok.Tokenize(" Aar*\thad a little\n l?mb ")
            if err != nil {
                t.Fatalf(err.Error())
            }
            if !equalStringArrays(out, []string{ "aar*", "had", "a", "little", "l?mb" }) {
                t.Fatalf("incorrect tokenization")
            }
        }
    })
}
