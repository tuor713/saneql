/// Tokens produced by the SaneQL lexer.
/// Carries slices into the original input for zero-copy identifiers / literals.
#[derive(Debug, Clone, PartialEq)]
pub enum Token<'input> {
    // Literals
    Ident(&'input str),
    Integer(&'input str),
    Float(&'input str),
    String(&'input str),

    // Keywords
    Let,
    Defun,
    Null,
    True,
    False,
    Table,

    // Multi-char operators / punctuation
    And,         // &&
    Or,          // ||
    Not,         // !
    TypeCast,    // ::
    ColonEq,     // :=
    EqGreater,   // =>
    LessEq,      // <=
    GreaterEq,   // >=
    NotEq,       // <> or !=
    DotDot,      // ..

    // Single-char punctuation
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    Less,
    Greater,
    Equals,
    LParen,
    RParen,
    LCurly,
    RCurly,
    LSquare,
    RSquare,
    Comma,
    Dot,
    Colon,
    Semicolon,
}

/// A lexical error with byte offset and description.
#[derive(Debug, Clone, PartialEq)]
pub struct LexError {
    pub offset: usize,
    pub message: String,
}

impl std::fmt::Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lex error at offset {}: {}", self.offset, self.message)
    }
}

/// Iterator-based lexer that yields `(start, Token, end)` triples for LALRPOP.
pub struct Lexer<'input> {
    input: &'input str,
    pos: usize,
}

impl<'input> Lexer<'input> {
    pub fn new(input: &'input str) -> Self {
        Lexer { input, pos: 0 }
    }

    #[allow(dead_code)]
    fn rest(&self) -> &'input str {
        &self.input[self.pos..]
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.pos).copied()
    }

    fn peek2(&self) -> Option<u8> {
        self.input.as_bytes().get(self.pos + 1).copied()
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn slice(&self, start: usize, end: usize) -> &'input str {
        &self.input[start..end]
    }

    fn err(&self, msg: &str) -> LexError {
        LexError { offset: self.pos, message: msg.to_string() }
    }

    /// Skip whitespace (ASCII + common Unicode codepoints) and `--` / `/* */` comments.
    fn skip_ws(&mut self) {
        loop {
            match self.peek() {
                // ASCII whitespace
                Some(b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c) => self.advance(),
                // Single-line comment
                Some(b'-') if self.peek2() == Some(b'-') => {
                    self.pos += 2;
                    while let Some(c) = self.peek() {
                        self.advance();
                        if c == b'\n' || c == b'\r' {
                            break;
                        }
                    }
                }
                // Multi-line comment
                Some(b'/') if self.peek2() == Some(b'*') => {
                    self.pos += 2;
                    loop {
                        match self.peek() {
                            None => break,
                            Some(b'*') if self.peek2() == Some(b'/') => {
                                self.pos += 2;
                                break;
                            }
                            _ => self.advance(),
                        }
                    }
                }
                _ => break,
            }
        }
    }

    fn lex_identifier(&mut self, start: usize) -> Token<'input> {
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.advance();
            } else {
                break;
            }
        }
        let text = self.slice(start, self.pos);
        // Case-insensitive keyword match
        match text.to_ascii_lowercase().as_str() {
            "let"   => Token::Let,
            "defun" => Token::Defun,
            "null"  => Token::Null,
            "true"  => Token::True,
            "false" => Token::False,
            "table" => Token::Table,
            _       => Token::Ident(text),
        }
    }

    /// Lex a quoted identifier: `"foo"` (with `""` as escaped quote).
    fn lex_quoted_ident(&mut self, start: usize) -> Result<Token<'input>, LexError> {
        loop {
            match self.peek() {
                None => return Err(self.err("unterminated quoted identifier")),
                Some(b'"') => {
                    self.advance();
                    // Escaped quote?
                    if self.peek() == Some(b'"') {
                        self.advance();
                        continue;
                    }
                    // Return the full slice including the surrounding quotes so
                    // callers can strip / unescape them.
                    return Ok(Token::Ident(self.slice(start, self.pos)));
                }
                _ => self.advance(),
            }
        }
    }

    /// Lex a numeric literal (integer or float).
    fn lex_number(&mut self, start: usize) -> Token<'input> {
        let mut is_float = self.input.as_bytes().get(start).copied() == Some(b'.');

        // Integer part
        if !is_float {
            while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                self.advance();
            }
            match self.peek() {
                Some(b'.') | Some(b'e') | Some(b'E') => {
                    is_float = true;
                    self.advance();
                }
                _ => return Token::Integer(self.slice(start, self.pos)),
            }
        }

        // Fractional part
        while self.peek().map_or(false, |c| c.is_ascii_digit()) {
            self.advance();
        }

        // Exponent part
        match self.peek() {
            Some(b'e') | Some(b'E') => {
                self.advance();
                if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                    self.advance();
                }
                while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                    self.advance();
                }
            }
            _ => {}
        }

        if is_float {
            Token::Float(self.slice(start, self.pos))
        } else {
            Token::Integer(self.slice(start, self.pos))
        }
    }

    /// Lex a string literal: `'...'` (with `''` as escaped quote, and SQL
    /// adjacent-literal continuation across newlines).
    fn lex_string(&mut self, start: usize) -> Result<Token<'input>, LexError> {
        loop {
            match self.peek() {
                None => return Err(self.err("unterminated string literal")),
                Some(b'\'') => {
                    self.advance();
                    // Escaped quote?
                    if self.peek() == Some(b'\'') {
                        self.advance();
                        continue;
                    }
                    // SQL adjacent-literal: skip whitespace+comments; if there
                    // is a newline and then another `'`, continue the literal.
                    let save = self.pos;
                    self.skip_ws();
                    if self.peek() == Some(b'\'') {
                        // Check that we crossed a newline (skip_ws advanced past it)
                        let gap = &self.input[save..self.pos];
                        if gap.contains('\n') || gap.contains('\r') {
                            self.advance(); // consume the opening `'` of the next fragment
                            continue;
                        }
                    }
                    self.pos = save; // undo the whitespace skip
                    return Ok(Token::String(self.slice(start, self.pos)));
                }
                _ => self.advance(),
            }
        }
    }

    /// Lex an operator sequence (PostgreSQL-style greedy multi-char operators).
    /// Returns the appropriate specific token when the operator is a known one.
    fn lex_operator(&mut self, start: usize) -> Result<Token<'input>, LexError> {
        // Greedy scan: keep consuming operator characters
        loop {
            match self.peek() {
                Some(b'*' | b'+' | b'<' | b'=' | b'>') => self.advance(),
                Some(b'!' | b'#' | b'%' | b'&' | b'?' | b'@' | b'^' | b'`' | b'|' | b'~') => {
                    self.advance();
                }
                Some(b'-') => {
                    // Stop before `--`
                    if self.peek2() == Some(b'-') {
                        break;
                    }
                    self.advance();
                }
                Some(b'/') => {
                    // Stop before `/*`
                    if self.peek2() == Some(b'*') {
                        break;
                    }
                    self.advance();
                }
                _ => break,
            }
        }

        // Trim trailing `+` and `-` unless the operator contains special chars
        let op = self.slice(start, self.pos);
        let has_special = op.bytes().any(|c| {
            matches!(c, b'!' | b'#' | b'%' | b'&' | b'?' | b'@' | b'^' | b'`' | b'|' | b'~')
        });
        if !has_special {
            while self.pos > start + 1
                && matches!(self.input.as_bytes()[self.pos - 1], b'+' | b'-')
            {
                self.pos -= 1;
            }
        }

        let op = self.slice(start, self.pos);
        let tok = match op {
            "+"  => Token::Plus,
            "-"  => Token::Minus,
            "*"  => Token::Star,
            "/"  => Token::Slash,
            "%"  => Token::Percent,
            "^"  => Token::Caret,
            "<"  => Token::Less,
            "="  => Token::Equals,
            ">"  => Token::Greater,
            "!"  => Token::Not,
            "<=" => Token::LessEq,
            ">=" => Token::GreaterEq,
            "<>" | "!=" => Token::NotEq,
            "=>" => Token::EqGreater,
            "&&" => Token::And,
            "||" => Token::Or,
            _    => return Err(LexError {
                offset: start,
                message: format!("unknown operator `{op}`"),
            }),
        };
        Ok(tok)
    }
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Result<(usize, Token<'input>, usize), LexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.skip_ws();

        let start = self.pos;
        let byte = self.peek()?; // None → EOF
        self.advance();

        let result: Result<Token<'input>, LexError> = match byte {
            // Single-char punctuation (never part of an operator sequence)
            b'(' => Ok(Token::LParen),
            b')' => Ok(Token::RParen),
            b'{' => Ok(Token::LCurly),
            b'}' => Ok(Token::RCurly),
            b'[' => Ok(Token::LSquare),
            b']' => Ok(Token::RSquare),
            b',' => Ok(Token::Comma),
            b';' => Ok(Token::Semicolon),

            // `.` or `..` or `.NNN` (float starting with dot)
            b'.' => {
                if self.peek() == Some(b'.') {
                    self.advance();
                    Ok(Token::DotDot)
                } else if self.peek().map_or(false, |c| c.is_ascii_digit()) {
                    Ok(self.lex_number(start))
                } else {
                    Ok(Token::Dot)
                }
            }

            // `:`, `::`, `:=`
            b':' => match self.peek() {
                Some(b':') => { self.advance(); Ok(Token::TypeCast) }
                Some(b'=') => { self.advance(); Ok(Token::ColonEq) }
                _ => Ok(Token::Colon),
            },

            // Numbers
            b'0'..=b'9' => Ok(self.lex_number(start)),

            // Identifiers and keywords
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => Ok(self.lex_identifier(start)),

            // Quoted identifier
            b'"' => self.lex_quoted_ident(start),

            // String literal
            b'\'' => self.lex_string(start),

            // Operator characters
            b'!' | b'#' | b'%' | b'&' | b'*' | b'+' | b'-' | b'/'
            | b'<' | b'=' | b'>' | b'?' | b'@' | b'^' | b'`' | b'|' | b'~' => {
                self.lex_operator(start)
            }

            other => Err(LexError {
                offset: start,
                message: format!("unexpected character `{}`", other as char),
            }),
        };

        let end = self.pos;
        Some(result.map(|tok| (start, tok, end)))
    }
}

/// Convenience: decode a string literal token (strip outer quotes, unescape `''`).
pub fn decode_string(raw: &str) -> String {
    let inner = &raw[1..raw.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' && chars.peek() == Some(&'\'') {
            chars.next();
        }
        out.push(c);
    }
    out
}

/// Convenience: decode a quoted identifier (strip outer `"`, unescape `""`).
pub fn decode_quoted_ident(raw: &str) -> String {
    let inner = &raw[1..raw.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' && chars.peek() == Some(&'"') {
            chars.next();
        }
        out.push(c);
    }
    out
}

/// Normalize an unquoted identifier to lowercase (SQL semantics).
pub fn normalize_ident(raw: &str) -> String {
    raw.to_ascii_lowercase()
}
