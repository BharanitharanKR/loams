//! `CREATE GRAPH … FROM COLLECTION …`, the mapped graph DDL of design §07 §2.1 (GR1 Task 17).
//!
//! ```sql
//! CREATE GRAPH kg
//!   VERTEX entity   KEY (_pk)                                  FROM COLLECTION entities
//!   VERTEX chunk    KEY (_pk)                                  FROM COLLECTION chunks
//!   EDGE   RELATED  SOURCE entity(src) DESTINATION entity(dst) TYPE FROM (rel_type) FROM COLLECTION relations
//!   EDGE   MENTIONS SOURCE chunk(chunk_id) DESTINATION entity(entity_id)          FROM COLLECTION mentions;
//! ```
//!
//! It is a Loams extension that DataFusion's parser does not know, so it is read here, over
//! DataFusion's own SQL tokenizer, into the very `loams.graph.v1.GraphMapping` that
//! `GraphAdminService/CreateGraph` takes for a LINKED graph: SQL and the RPC describe one graph
//! one way. Declarations may be separated by commas. Keywords are case-insensitive; names keep
//! their case, and may be quoted.
//!
//! Only the statement is read here. Loams Graph (`loams-graph`, behind the `graph` feature)
//! checks the mapping and creates the graph.

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use loams_proto::loams::graph::v1 as pb;

/// A parsed `CREATE GRAPH`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateGraph {
    /// The graph's name.
    pub name: String,
    /// Its mapping, as `CreateGraphRequest.mapping` carries it.
    pub mapping: pb::GraphMapping,
}

/// A `CREATE GRAPH` that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("CREATE GRAPH: {0}")]
pub struct GraphDdlError(pub String);

/// Whether `sql` is a `CREATE GRAPH` statement (its first two words), so a caller can route it
/// here rather than to DataFusion.
#[must_use]
pub fn is_create_graph(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    matches!(
        (words.next(), words.next()),
        (Some(a), Some(b)) if a.eq_ignore_ascii_case("CREATE") && b.eq_ignore_ascii_case("GRAPH")
    )
}

/// Parses one `CREATE GRAPH` statement.
///
/// # Errors
///
/// [`GraphDdlError`] naming what was expected where the statement went wrong.
pub fn parse_create_graph(sql: &str) -> Result<CreateGraph, GraphDdlError> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize()
        .map_err(|err| GraphDdlError(err.to_string()))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    Parser { tokens, at: 0 }.statement()
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn fail<T>(&self, expected: &str) -> Result<T, GraphDdlError> {
        let found = self.tokens.get(self.at).map_or_else(
            || "the end of the statement".to_string(),
            |t| format!("`{t}`"),
        );
        Err(GraphDdlError(format!("expected {expected}, found {found}")))
    }

    /// Whether the next token is keyword `word` (unquoted, any case).
    fn at_keyword(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
    }

    fn keyword(&mut self, word: &str) -> Result<(), GraphDdlError> {
        if self.at_keyword(word) {
            self.at += 1;
            Ok(())
        } else {
            self.fail(word)
        }
    }

    fn token(&mut self, wanted: &Token, shown: &str) -> Result<(), GraphDdlError> {
        if self.peek() == Some(wanted) {
            self.at += 1;
            Ok(())
        } else {
            self.fail(shown)
        }
    }

    /// A name: a word (quoted or not), or a single-quoted string.
    fn name(&mut self, what: &str) -> Result<String, GraphDdlError> {
        match self.peek() {
            Some(Token::Word(w)) => {
                let value = w.value.clone();
                self.at += 1;
                Ok(value)
            }
            Some(Token::SingleQuotedString(s)) => {
                let value = s.clone();
                self.at += 1;
                Ok(value)
            }
            _ => self.fail(what),
        }
    }

    /// `( <name> )`.
    fn parenthesized(&mut self, what: &str) -> Result<String, GraphDdlError> {
        self.token(&Token::LParen, "`(`")?;
        let name = self.name(what)?;
        self.token(&Token::RParen, "`)`")?;
        Ok(name)
    }

    /// `FROM COLLECTION <name>`.
    fn collection_clause(&mut self) -> Result<String, GraphDdlError> {
        self.keyword("FROM")?;
        self.keyword("COLLECTION")?;
        self.name("a collection name")
    }

    fn statement(mut self) -> Result<CreateGraph, GraphDdlError> {
        self.keyword("CREATE")?;
        self.keyword("GRAPH")?;
        let name = self.name("the graph's name")?;
        let mut mapping = pb::GraphMapping::default();
        loop {
            if self.at_keyword("VERTEX") {
                self.at += 1;
                let label = self.name("a vertex label")?;
                self.keyword("KEY")?;
                let key_field = self.parenthesized("a key field")?;
                let collection = self.collection_clause()?;
                mapping.vertices.push(pb::VertexMapping {
                    label,
                    collection,
                    key_field,
                    ..Default::default()
                });
            } else if self.at_keyword("EDGE") {
                self.at += 1;
                let r#type = self.name("an edge name")?;
                self.keyword("SOURCE")?;
                let source_label = self.name("the source label")?;
                let source_field = self.parenthesized("the source field")?;
                self.keyword("DESTINATION")?;
                let destination_label = self.name("the destination label")?;
                let destination_field = self.parenthesized("the destination field")?;
                // `TYPE FROM (field)` before `FROM COLLECTION`.
                let type_field = if self.at_keyword("TYPE") {
                    self.at += 1;
                    self.keyword("FROM")?;
                    self.parenthesized("the type field")?
                } else {
                    String::new()
                };
                let collection = self.collection_clause()?;
                mapping.edges.push(pb::EdgeMapping {
                    r#type,
                    collection,
                    source_label,
                    source_field,
                    destination_label,
                    destination_field,
                    type_field,
                    ..Default::default()
                });
            } else if mapping.vertices.is_empty() && mapping.edges.is_empty() {
                return self.fail("VERTEX or EDGE");
            } else {
                break;
            }
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
            }
        }
        if self.peek() == Some(&Token::SemiColon) {
            self.at += 1;
        }
        if self.peek().is_some() {
            return self.fail("VERTEX, EDGE or the end of the statement");
        }
        Ok(CreateGraph { name, mapping })
    }
}
