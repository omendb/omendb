//! Bind complete transaction controls before allowing any session boundary effect.

use sqlparser::ast::Statement;

use crate::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionControl {
    Begin,
    Commit,
    Rollback,
}

pub(super) fn bind(statement: &Statement) -> Result<Option<TransactionControl>> {
    let unsupported = || {
        super::unsupported(
            "transaction control",
            "transaction characteristics, savepoints, chaining and procedural blocks are not supported",
        )
    };
    Ok(Some(match statement {
        Statement::StartTransaction {
            modes,
            modifier,
            statements,
            exception,
            has_end_keyword,
            ..
        } => {
            if !modes.is_empty()
                || modifier.is_some()
                || !statements.is_empty()
                || exception.is_some()
                || *has_end_keyword
            {
                return Err(unsupported());
            }
            TransactionControl::Begin
        }
        Statement::Commit {
            chain, modifier, ..
        } => {
            if *chain || modifier.is_some() {
                return Err(unsupported());
            }
            TransactionControl::Commit
        }
        Statement::Rollback { chain, savepoint } => {
            if *chain || savepoint.is_some() {
                return Err(unsupported());
            }
            TransactionControl::Rollback
        }
        Statement::Savepoint { .. } | Statement::ReleaseSavepoint { .. } => {
            return Err(unsupported());
        }
        _ => return Ok(None),
    }))
}

/// Keep the original SQL, splitting only on actual delimiter tokens, not on
/// semicolons in literals, quoted identifiers or comments. Locations count
/// Unicode characters rather than bytes; walk the source once to recover slices.
#[cfg(feature = "pgwire")]
pub(crate) fn statement_sources(source: &str) -> Result<Vec<&str>> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::tokenizer::{Location, Token, Tokenizer};

    let tokens = Tokenizer::new(&GenericDialect {}, source)
        .tokenize_with_location()
        .map_err(|error| crate::DbError::SqlParse(error.to_string()))?;
    let mut chars = source.char_indices();
    let mut location = Location::new(1, 1);
    let mut start = 0;
    let mut has_statement = false;
    let mut statements = Vec::new();
    for token in tokens {
        if token.token != Token::SemiColon {
            has_statement |= !matches!(token.token, Token::Whitespace(_));
            continue;
        }
        let delimiter = chars.by_ref().find_map(|(offset, ch)| {
            let matches = location == token.span.start;
            if ch == '\n' {
                location.line += 1;
                location.column = 1;
            } else {
                location.column += 1;
            }
            matches.then_some((offset, ch))
        });
        // GenericDialect expands MySQL executable-comment hints using
        // synthetic spans. Such a token must never split off a bare
        // COMMIT/ROLLBACK before the rest of the original control.
        let Some((offset, ';')) = delimiter else {
            return Err(crate::DbError::SqlParse(
                "statement delimiter has no literal source position".to_owned(),
            ));
        };
        if has_statement {
            statements.push(source[start..offset].trim());
        }
        start = offset + 1;
        has_statement = false;
    }
    if has_statement {
        statements.push(source[start..].trim());
    }
    Ok(statements)
}
