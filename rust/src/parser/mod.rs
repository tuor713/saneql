pub mod ast;
pub mod lexer;

use lalrpop_util::ParseError;
use lexer::{LexError, Lexer, Token};

// LALRPOP-generated parser (produced by build.rs from saneql.lalrpop)
#[allow(clippy::all)]
mod saneql {
    include!(concat!(env!("OUT_DIR"), "/parser/saneql.rs"));
}

/// Parse a SaneQL query string and return the AST, or a human-readable error.
pub fn parse(input: &str) -> Result<ast::Ast, String> {
    let lexer = Lexer::new(input);
    saneql::QueryParser::new()
        .parse(lexer)
        .map_err(|e| format_error(input, e))
}

fn format_error(input: &str, e: ParseError<usize, Token<'_>, LexError>) -> String {
    match e {
        ParseError::InvalidToken { location } => {
            format!("invalid token at offset {location}: `{}`", &input[location..])
        }
        ParseError::UnrecognizedEof { location, expected } => {
            format!(
                "unexpected end of input at offset {location}, expected one of: {}",
                expected.join(", ")
            )
        }
        ParseError::UnrecognizedToken { token: (start, tok, _end), expected } => {
            format!(
                "unexpected token `{tok:?}` at offset {start}, expected one of: {}",
                expected.join(", ")
            )
        }
        ParseError::ExtraToken { token: (start, tok, _end) } => {
            format!("extra token `{tok:?}` at offset {start}")
        }
        ParseError::User { error } => error.to_string(),
    }
}
