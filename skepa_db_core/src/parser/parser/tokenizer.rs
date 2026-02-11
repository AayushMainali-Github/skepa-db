pub(super) fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut just_closed_quote = false;

    let mut it = input.chars().peekable();

    while let Some(ch) = it.next() {
        match ch {
            '"' => {
                if just_closed_quote {
                    return Err(
                        "Unexpected quote after closing quote. Add whitespace between tokens."
                            .to_string(),
                    );
                }

                if !in_quotes {
                    if !current.is_empty() {
                        return Err("Quote (\") cannot start in the middle of a token. Add whitespace before the quote."
                            .to_string());
                    }
                    in_quotes = true;
                } else {
                    in_quotes = false;
                    just_closed_quote = true;
                }
            }

            '\\' if in_quotes => {
                match it.peek().copied() {
                    Some('"') => {
                        it.next();
                        current.push('"');
                    }
                    Some('\\') => {
                        it.next();
                        current.push('\\');
                    }
                    _ => {
                        return Err("Invalid escape sequence in quotes. Use \\\" for a quote or \\\\ for a backslash."
                            .to_string());
                    }
                }
                just_closed_quote = false;
            }

            c if c.is_whitespace() && !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                    continue;
                }

                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }

            ',' | '(' | ')' if !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                } else if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(ch.to_string());
            }

            '>' | '<' | '=' | '!' if !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                } else if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                if let Some('=') = it.peek().copied() {
                    let mut op = String::new();
                    op.push(ch);
                    op.push('=');
                    it.next();
                    tokens.push(op);
                } else {
                    tokens.push(ch.to_string());
                }
            }

            _ => {
                if just_closed_quote {
                    return Err("Characters found immediately after a closing quote. Add whitespace after the quoted string."
                        .to_string());
                }
                current.push(ch);
            }
        }
    }

    if in_quotes {
        return Err("Unclosed quote (\") in input".to_string());
    }

    if !current.is_empty() || just_closed_quote {
        tokens.push(current);
    }

    Ok(tokens)
}
