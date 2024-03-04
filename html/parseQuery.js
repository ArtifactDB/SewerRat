function parseQuery(message, at=0, open_par=false) {
    let word = "";
    let words = [];
    let children = [];
    let operations = [];
    let i = at;

    // Function to easily add a text search clause.
    function add_text_child() {
        let new_component = { type: "text" };

        if (words.length == 0) {
            throw new Error("no search terms at position " + String(i));
        }
        let fi = words[0].indexOf(":");
        if (fi == 0) {
            throw new Error("search field should be non-empty for terms ending at " + String(i));
        } else if (fi > 0) {
            new_component.field = words[0].slice(0, fi);
            let leftover = words[0].slice(fi + 1);
            if (leftover == "") {
                if (words.length == 1) {
                    throw new Error("no search terms at position " + String(i) + " after removing the search field");
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
        children.push(new_component);
        words = [];
        return;
    }

    // Parsing the query to obtain all of the (possibly nested) text clauses.
    let closing_par = false;
    while (i < message.length) {
        const c = message[i];
        if (c == "(") {
            if (operations.length != children.length) {
                throw new Error("search clauses must be separated by AND or OR at position " + String(i));
            }
            let nested = parseMetadataQuery(message, i + 1, true);
            i = nested.at;
            children.push(nested.metadata);
            continue;
        } else if (c == ")") {
            if (!open_par) {
                throw new Error("unmatched closing parenthesis at position " + String(i))
            }
            closing_par = true;
            ++i;
            break;
        }

        let is_whitespace = c.match(/\s/);
        if (is_whitespace) {
            if (word == "AND" || word == "OR") {
                add_text_child();
                operations.push(word);
            } else if (word.length) {
                words.push(word)
            }
            word = "";
        } else {
            word += c;
        }

        ++i;
    }

    if (word.length) {
        words.push(word);
    }
    add_text_child();

    if (open_par && !closing_par) {
        throw new Error("unmatched openining parenthesis at position " + String(at - 1))
    }

    // Finding the stretches of ANDs first.
    if (operations.length > 0) {
        let tmp_children = [];
        let active_children = [children[0]];
        for (var o = 0; o < operations.length; ++o) {
            if (operations[o] == "AND") {
                active_children.push(children[o + 1]);
            } else {
                tmp_children.push(active_children);
                active_children = [children[o + 1]];
            }
        }
        tmp_children.push(active_children);

        for (var t = 0; t < tmp_children.length; t++) {
            if (tmp_children[t].length > 1) {
                tmp_children[t] = { type: "and", children: tmp_children[t] };
            } else {
                tmp_children[t] = tmp_children[t][0];
            }
        }

        children = tmp_children;
    }

    // Finally, resolving the ORs.
    let output;
    if (children.length > 1) {
        output = { type: "or", children: children };
    } else {
        output = children[0];
    }
    return { metadata: output, at: i };
}
