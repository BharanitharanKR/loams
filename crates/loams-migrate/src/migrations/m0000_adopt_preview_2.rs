//! `0000-adopt-preview-2` (design §54 §5.3, D926; UP1 Task 5): records an
//! unstamped preview.2 stack as `0.1.0-preview.2` once the fingerprint
//! ([`crate::adopt`]) recognises it. The runner writes the stack record
//! after it; the migration itself writes nothing else, so it is
//! `Reversible`: preview.2 never reads `_system/upgrade/*`.

use async_trait::async_trait;

use crate::adopt::{Fingerprint, PREVIEW_2};
use crate::formats::{FormatId, Store};
use crate::migration::{Class, Cursor, Cx, Estimate, Migration, MigrationError, Step};

/// See the module documentation.
#[derive(Debug)]
pub struct AdoptPreview2;

#[async_trait]
impl Migration for AdoptPreview2 {
    fn id(&self) -> &'static str {
        "0000-adopt-preview-2"
    }

    fn store(&self) -> Store {
        Store::Meta
    }

    fn formats(&self) -> &'static [(FormatId, u16, u16)] {
        &[]
    }

    fn class(&self) -> Class {
        Class::Reversible
    }

    fn adopts(&self) -> Option<&'static str> {
        Some(PREVIEW_2)
    }

    fn estimate(&self, _cx: &Cx) -> Result<Estimate, MigrationError> {
        Ok(Estimate {
            rows: 0,
            bytes: 0,
            seconds: 2,
        })
    }

    async fn apply(&self, cx: &mut Cx, _cursor: Option<Cursor>) -> Result<Step, MigrationError> {
        Fingerprint::check(cx, cx.declared_release.as_deref())
            .await
            .map_err(|m| MigrationError::new("stack_unrecognised", m.to_string()))?;
        Ok(Step::Done)
    }

    async fn verify(&self, _cx: &Cx) -> Result<(), MigrationError> {
        Ok(())
    }

    async fn abandon(&self, _cx: &mut Cx) -> Result<(), MigrationError> {
        Ok(())
    }
}
