//! What `loams.auth.v1` ([`super::connect_auth`]) and `loams.admin.v1`
//! ([`super::connect_admin`]) share: the errors they raise over the
//! principal store, slugs, and the audit event of a mutation.

use connectrpc::{ConnectError, ErrorCode};
use loams_proto::loams::admin::v1 as pb;

use super::connect::refuse;
use crate::auth::authorizer::Actor;
use crate::auth::directory::Unavailable;

/// The longest slug.
pub(super) const MAX_SLUG: usize = 63;

impl From<Unavailable> for ConnectError {
    fn from(err: Unavailable) -> Self {
        unavailable(&err.0)
    }
}

/// `unavailable`: the principal store cannot be read or written now.
pub(super) fn unavailable(message: &str) -> ConnectError {
    refuse(
        ErrorCode::Unavailable,
        "unavailable",
        message.to_owned(),
        &[],
    )
}

/// `invalid_argument` naming the field (the console mock's message shape).
pub(super) fn invalid(field: &str, message: &str) -> ConnectError {
    refuse(
        ErrorCode::InvalidArgument,
        "invalid_argument",
        format!("{field}: {message}"),
        &[("field", field.to_owned())],
    )
}

/// A slug from a name: lower case, runs of anything else as one `-`.
pub(super) fn slugify(name: &str) -> String {
    let mut slug = String::new();
    for c in name.trim().chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.ends_with('-') && !slug.is_empty() {
            slug.push('-');
        }
    }
    let slug = slug.trim_end_matches('-');
    slug.chars()
        .take(MAX_SLUG)
        .collect::<String>()
        .trim_end_matches('-')
        .to_owned()
}

/// One audit event of a mutation `actor` made.
pub(super) fn event(
    actor: &Actor,
    action: &str,
    target: (&str, &str, &str),
    project: &str,
    environment: &str,
) -> pb::AuditEvent {
    let (kind, id, name) = target;
    pb::AuditEvent {
        actor: actor.subject().into(),
        action: action.to_owned(),
        target: pb::ResourceRef {
            kind: kind.to_owned(),
            id: id.to_owned(),
            name: name.to_owned(),
            ..Default::default()
        }
        .into(),
        project: project.to_owned(),
        environment: environment.to_owned(),
        outcome: pb::AuditOutcome::AUDIT_OUTCOME_SUCCESS.into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_lower_case_with_inner_hyphens() {
        assert_eq!(slugify("  Docs Search!! v2 "), "docs-search-v2");
        assert_eq!(slugify("***"), "");
    }
}
