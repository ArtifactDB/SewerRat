package main

import (
    "strings"
    "fmt"
    "regexp"
	"unicode"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/runes"
)

type unicodeTokenizer struct {
    Stripper transform.Transformer
    Splitter *regexp.Regexp
    Converter *strings.Replacer
}

func newUnicodeTokenizer(allow_wildcards bool) (*unicodeTokenizer, error) {
    pattern := ""
    if allow_wildcards {
        pattern = "*?"
    }

    comp, err := regexp.Compile("[^\\p{L}\\p{N}\\p{Co}" + pattern + "-]+")
    if err != nil {
        return nil, fmt.Errorf("failed to compile regex; %w", err)
    }

    var replacer *strings.Replacer
    if allow_wildcards {
        // Convert the usual wildcards to SQLite wildcards.
        replacer = strings.NewReplacer(
            "?", "_",
            "*", "%",
        )
    }

    return &unicodeTokenizer {
	    Stripper: transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC),
        Splitter: comp,
        Converter: replacer,
    }, nil
}

func (u *unicodeTokenizer) Tokenize(x string) ([]string, error) {
    y, _, err := transform.String(u.Stripper, x)
    if err != nil {
        return nil, fmt.Errorf("failed to strip diacritics; %w", err)
    }

    y = strings.ToLower(y)
    output := u.Splitter.Split(y, -1)

    final := []string{}
    present := map[string]bool{}

    for _, t := range output {
        if len(t) > 0 {
            if u.Converter != nil {
                t = u.Converter.Replace(t)
            }
            if _, ok := present[t]; !ok {
                final = append(final, t)
                present[t] = true
            }
        }
    }

    return final, nil
}
