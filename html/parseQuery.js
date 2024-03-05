function parseQuery(message, at=0, open_par=false) {
    let word = "";
    let words = [];
    let clauses = [];
    let operations = [];
    let negation = false;

    function add_text_clause(at) {
        let new_component = { type: "text" };

        if (words.length == 0) {
            throw new Error("no search terms at position " + String(at));
        }
        let fi = words[0].indexOf(":");
        if (fi == 0) {
            throw new Error("search field should be non-empty for terms ending at " + String(at));
        } else if (fi > 0) {
            new_component.field = words[0].slice(0, fi);
            let leftover = words[0].slice(fi + 1);
            if (leftover == "") {
                if (words.length == 1) {
                    throw new Error("no search terms at position " + String(at) + " after removing the search field");
                }
                words = words.slice(1);
            } else {
                words[0] = leftover;
            }
        }

        new_component.text = words.join(" ");
        if (new_component.text.match("%")) {
            new_component.partial = true;
        }

        if (negation) {
            new_component = { "type": "not", "child": new_component };
            negation = false;
        }

        clauses.push(new_component);
        words = [];
        return;
    }

    function add_operation(at) {
        if (operations.length == clauses.length) {
            // Operations are binary, so if there wasn't already a preceding
            // clause, then we must try to add a text clause.
            add_text_clause(at);
        } else if (operations.length > clauses.length) {
            throw new Error("multiple AND or OR keywords at position " + String(at));
        }
        operations.push(word);
        word = "";
    }

    // Parsing the query to obtain all of the individual search clauses.
    let i = at;
    let closing_par = false;
    while (i < message.length) {
        const c = message[i];
        const is_whitespace = c.match(/\s/);

        if (c == "(") {
            if (word == "AND" || word == "OR") {
                add_operation(i);
            } else if (word == "NOT") {
                negation = true;
                word = "";
            } else if (word != "" || words.length > 0) {
                throw new Error("search clauses must be separated by AND or OR at position " + String(i));
            }
            let nested = parseQuery(message, i + 1, true);
            i = nested.at;
            clauses.push(nested.metadata);
            negation = false;
            continue;
        }

        if (c == ")") {
            if (!open_par) {
                throw new Error("unmatched closing parenthesis at position " + String(i))
            }
            closing_par = true;
            ++i;
            break;
        }

        if (is_whitespace) {
            if (word == "AND" || word == "OR") {
                add_operation(i);
            } else if (word == "NOT") {
                negation = true;
                word = "";
            } else if (word.length) {
                words.push(word)
                word = "";
            }
        } else {
            word += c;
        }

        ++i;
    }

    if (operations.length == clauses.length) {
        if (word.length) {
            words.push(word);
        }
        add_text_clause(i);
    }

    if (open_par && !closing_par) {
        throw new Error("unmatched openining parenthesis at position " + String(at - 1))
    }

    // Finding the stretches of ANDs first.
    if (operations.length > 0) {
        let tmp_clauses = [];
        let active_clauses = [clauses[0]];
        for (var o = 0; o < operations.length; ++o) {
            if (operations[o] == "AND") {
                active_clauses.push(clauses[o + 1]);
            } else {
                tmp_clauses.push(active_clauses);
                active_clauses = [clauses[o + 1]];
            }
        }
        tmp_clauses.push(active_clauses);

        for (var t = 0; t < tmp_clauses.length; t++) {
            if (tmp_clauses[t].length > 1) {
                tmp_clauses[t] = { type: "and", children: tmp_clauses[t] };
            } else {
                tmp_clauses[t] = tmp_clauses[t][0];
            }
        }

        clauses = tmp_clauses;
    }

    // Finally, resolving the ORs.
    let output;
    if (clauses.length > 1) {
        output = { type: "or", children: clauses };
    } else {
        output = clauses[0];
    }
    return { metadata: output, at: i };
}
