//! Corpus loading: the spike's own `-- @id` files, and plain `.sql` files
//! (HS1's corpus, or mined statements), split on `;` at the end of a line.

use std::path::Path;

#[derive(Debug, Clone)]
pub struct Entry {
    pub id: String,
    pub source: String,
    pub features: Vec<String>,
    pub sql: String,
}

/// Loads one file or every `.sql` file under a directory (sorted).
pub fn load(path: &Path) -> std::io::Result<Vec<Entry>> {
    let mut out = Vec::new();
    if path.is_dir() {
        let mut files = Vec::new();
        walk(path, &mut files)?;
        files.sort();
        for f in files {
            out.extend(load_file(&f)?);
        }
    } else {
        out.extend(load_file(path)?);
    }
    Ok(out)
}

fn walk(dir: &Path, files: &mut Vec<std::path::PathBuf>) -> std::io::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        if p.is_dir() {
            walk(&p, files)?;
        } else if p.extension().is_some_and(|x| x == "sql") {
            files.push(p);
        }
    }
    Ok(())
}

fn load_file(path: &Path) -> std::io::Result<Vec<Entry>> {
    let text = std::fs::read_to_string(path)?;
    let source = path.display().to_string();
    if text.contains("-- @id ") {
        Ok(tagged(&text, &source))
    } else {
        Ok(plain(&text, &source, path))
    }
}

fn tagged(text: &str, source: &str) -> Vec<Entry> {
    let mut out = Vec::new();
    let mut cur: Option<Entry> = None;
    for line in text.lines() {
        if let Some(id) = line.strip_prefix("-- @id ") {
            if let Some(e) = cur.take() {
                out.push(finish(e));
            }
            cur = Some(Entry {
                id: id.trim().to_string(),
                source: source.to_string(),
                features: vec![],
                sql: String::new(),
            });
        } else if let Some(f) = line.strip_prefix("-- @features ") {
            if let Some(e) = cur.as_mut() {
                e.features = f.split(',').map(|s| s.trim().to_string()).collect();
            }
        } else if line.trim_start().starts_with("--") {
            continue;
        } else if let Some(e) = cur.as_mut() {
            e.sql.push_str(line);
            e.sql.push('\n');
        }
    }
    if let Some(e) = cur.take() {
        out.push(finish(e));
    }
    out.into_iter().filter(|e| !e.sql.is_empty()).collect()
}

fn finish(mut e: Entry) -> Entry {
    let s = e.sql.trim().trim_end_matches(';').trim().to_string();
    e.sql = s;
    e
}

fn plain(text: &str, source: &str, path: &Path) -> Vec<Entry> {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let dir = path
        .parent()
        .and_then(|p| p.file_name())
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let body: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut out = Vec::new();
    let mut buf = String::new();
    for line in body.lines() {
        buf.push_str(line);
        buf.push('\n');
        if line.trim_end().ends_with(';') {
            push_plain(&mut out, &mut buf, &dir, &stem, source);
        }
    }
    push_plain(&mut out, &mut buf, &dir, &stem, source);
    out
}

fn push_plain(out: &mut Vec<Entry>, buf: &mut String, dir: &str, stem: &str, source: &str) {
    let s = buf.trim().trim_end_matches(';').trim().to_string();
    buf.clear();
    if s.is_empty() {
        return;
    }
    let n = out.len() + 1;
    out.push(Entry {
        id: format!("{dir}/{stem}#{n}"),
        source: source.to_string(),
        features: vec![],
        sql: s,
    });
}
