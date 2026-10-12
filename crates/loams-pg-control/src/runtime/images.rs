//! The compute image of each Postgres major (PG2 Task 11, R10.8): read from
//! the repository's pin file, `deploy/pins/loams-postgres-images.toml`
//! (written by `scripts/nf1/pins.py`), compiled in, and overridable per
//! major (`loams pg-control --compute-image 17=<repo>@sha256:<digest>`).
//!
//! Every image is pinned by digest ([`is_pinned`]), and never one of the
//! private archive copies under `.../neon-archive/` (NF1 keeps those for
//! the record, not to run).

use std::collections::BTreeMap;

use super::is_pinned;

/// The pin file, as this build was made with it.
pub const PINS: &str = include_str!("../../../../deploy/pins/loams-postgres-images.toml");

/// Why an image or the pin file was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ImageError {
    #[error("the pin file: {0}")]
    Pins(String),
    #[error("{0:?} is not pinned by digest (<repository>@sha256:<64 hex>)")]
    NotPinned(String),
    #[error("{0:?} is a neon-archive copy, which is never run")]
    Archive(String),
    #[error("{0:?}: expected <major>=<repository>@sha256:<digest>")]
    Malformed(String),
}

/// `image`, if computes may run it: pinned by digest, not an archive copy.
///
/// # Errors
///
/// [`ImageError::NotPinned`], [`ImageError::Archive`].
pub fn check_image(image: &str) -> Result<(), ImageError> {
    if !is_pinned(image) {
        return Err(ImageError::NotPinned(image.to_owned()));
    }
    if image.contains("/neon-archive/") {
        return Err(ImageError::Archive(image.to_owned()));
    }
    Ok(())
}

/// The compute image of each supported major in pin file `text`
/// (`[majors] supported`, each `[images.compute-v<major>]`'s `repository`
/// and `digest`).
///
/// # Errors
///
/// [`ImageError::Pins`] for a file that does not parse or misses a
/// supported major's image; the checks of [`check_image`].
pub fn from_pins(text: &str) -> Result<BTreeMap<u32, String>, ImageError> {
    let pins: toml::Value = toml::from_str(text).map_err(|e| ImageError::Pins(e.to_string()))?;
    let supported = pins
        .get("majors")
        .and_then(|m| m.get("supported"))
        .and_then(toml::Value::as_array)
        .ok_or_else(|| ImageError::Pins("no [majors] supported".into()))?;
    let mut out = BTreeMap::new();
    for major in supported {
        let major = major
            .as_integer()
            .and_then(|m| u32::try_from(m).ok())
            .ok_or_else(|| ImageError::Pins("a supported major is not a number".into()))?;
        let entry = pins
            .get("images")
            .and_then(|i| i.get(format!("compute-v{major}")))
            .ok_or_else(|| ImageError::Pins(format!("no [images.compute-v{major}]")))?;
        let field = |name: &str| {
            entry
                .get(name)
                .and_then(toml::Value::as_str)
                .ok_or_else(|| ImageError::Pins(format!("compute-v{major} has no {name}")))
        };
        let image = format!("{}@{}", field("repository")?, field("digest")?);
        check_image(&image)?;
        out.insert(major, image);
    }
    Ok(out)
}

/// The compute images this build was pinned with ([`PINS`]).
///
/// # Errors
///
/// As [`from_pins`].
pub fn pinned() -> Result<BTreeMap<u32, String>, ImageError> {
    from_pins(PINS)
}

/// `<major>=<image>`, checked.
///
/// # Errors
///
/// [`ImageError::Malformed`]; the checks of [`check_image`].
pub fn parse_override(s: &str) -> Result<(u32, String), ImageError> {
    let (major, image) = s
        .split_once('=')
        .ok_or_else(|| ImageError::Malformed(s.to_owned()))?;
    let major = major
        .parse()
        .map_err(|_| ImageError::Malformed(s.to_owned()))?;
    check_image(image)?;
    Ok((major, image.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";

    #[test]
    fn the_pin_file_names_a_pinned_image_per_supported_major() {
        let images = pinned().expect("the pin file parses");
        assert!(images.contains_key(&17), "{images:?}");
        for image in images.values() {
            assert!(is_pinned(image), "{image}");
            assert!(!image.contains("neon-archive"), "{image}");
        }
    }

    #[test]
    fn overrides_are_checked() {
        let image = format!("ghcr.io/ostrium-labs/loams-postgres-compute-v17@{DIGEST}");
        assert_eq!(
            parse_override(&format!("17={image}")).expect("ok"),
            (17, image)
        );
        assert!(matches!(
            parse_override("17=ghcr.io/x/compute:latest"),
            Err(ImageError::NotPinned(_))
        ));
        assert!(matches!(
            parse_override(&format!(
                "17=ghcr.io/ostrium-labs/neon-archive/compute@{DIGEST}"
            )),
            Err(ImageError::Archive(_))
        ));
        assert!(matches!(
            parse_override("seventeen=x"),
            Err(ImageError::Malformed(_))
        ));
    }

    #[test]
    fn a_pin_file_missing_an_image_is_refused() {
        let text = "[majors]\nsupported = [17]\n";
        assert!(matches!(from_pins(text), Err(ImageError::Pins(_))));
    }
}
