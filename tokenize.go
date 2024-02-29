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
}

func newUnicodeTokenizer(allow_wildcards bool) (*unicodeTokenizer, error) {
    pattern := ""
    if allow_wildcards {
        pattern = "%_"
    }

    comp, err := regexp.Compile("[^\\p{L}\\p{N}\\p{Co}" + pattern + "-]+")
    if err != nil {
        return nil, fmt.Errorf("failed to compile regex; %w", err)
    }

    return &unicodeTokenizer {
	    Stripper: transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC),
        Splitter: comp,
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
    for _, t := range output {
        if len(t) > 0 {
            final = append(final, t)
        }
    }
    return final, nil
}
