//! Reading the documents referenced by CSVW metadata.
//!
//! maplib does not fetch anything over the network when processing CSVW. Documents are
//! read from the local file system: `file:` IRIs are read directly, and an IRI that starts
//! with one of the configured IRI prefixes is read from the corresponding local directory.
//! This lets metadata that uses absolute `http(s)` URLs (as published) be processed against
//! a local copy of the files.

use crate::errors::CsvwError;
use oxiri::Iri;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct Loader {
    /// (IRI prefix, local directory) pairs, the longest matching prefix is used.
    pub mappings: Vec<(String, PathBuf)>,
}

impl Loader {
    pub fn new() -> Loader {
        Loader { mappings: vec![] }
    }

    pub fn add_mapping(&mut self, iri_prefix: String, dir: PathBuf) {
        self.mappings.push((iri_prefix, dir));
        self.mappings
            .sort_by_key(|(a, _)| std::cmp::Reverse(a.len()));
    }

    /// The local path of a document, if the IRI can be mapped to the file system.
    pub fn local_path(&self, iri: &str) -> Option<PathBuf> {
        let without_fragment = iri.split('#').next().unwrap();
        // A query string can not be part of a file name, it is ignored.
        let without_query = without_fragment.split('?').next().unwrap();
        for (prefix, dir) in &self.mappings {
            if let Some(rest) = without_query.strip_prefix(prefix.as_str()) {
                let rest = percent_decode(rest);
                return Some(dir.join(rest));
            }
        }
        if let Some(rest) = without_query.strip_prefix("file://") {
            let rest = percent_decode(rest);
            return Some(PathBuf::from(rest));
        }
        None
    }

    /// Reads a document. Returns None if it does not exist.
    pub fn load(&self, iri: &str) -> Result<Option<Vec<u8>>, CsvwError> {
        let Some(p) = self.local_path(iri) else {
            return Err(CsvwError::ReadError(
                iri.to_string(),
                "the IRI is not a file: IRI and is not below any of the base IRIs mapped to a local directory, and maplib does not fetch CSVW documents over the network".to_string(),
            ));
        };
        if !p.is_file() {
            return Ok(None);
        }
        let bytes =
            std::fs::read(&p).map_err(|x| CsvwError::ReadError(iri.to_string(), x.to_string()))?;
        Ok(Some(bytes))
    }

    pub fn load_required(&self, iri: &str) -> Result<Vec<u8>, CsvwError> {
        match self.load(iri)? {
            Some(b) => Ok(b),
            None => Err(CsvwError::ReadError(
                iri.to_string(),
                "file not found".to_string(),
            )),
        }
    }
}

pub fn file_iri(p: &Path) -> Result<String, CsvwError> {
    let abs = std::path::absolute(p)
        .map_err(|x| CsvwError::ReadError(p.to_string_lossy().to_string(), x.to_string()))?;
    let s = abs.to_string_lossy().replace('\\', "/");
    let mut out = String::from("file://");
    if !s.starts_with('/') {
        out.push('/');
    }
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '.' | '_' | '~' | '/' | ':' => out.push(c),
            _ => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
        }
    }
    Ok(out)
}

pub fn file_dir_iri(p: &Path) -> Result<String, CsvwError> {
    let mut s = file_iri(p)?;
    if !s.ends_with('/') {
        s.push('/');
    }
    Ok(s)
}

pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Resolves a (possibly relative) IRI reference against a base IRI.
pub fn resolve(base: &str, reference: &str) -> Result<String, CsvwError> {
    let base_iri = Iri::parse(base.to_string())
        .map_err(|x| CsvwError::IriError(base.to_string(), x.to_string()))?;
    let r = base_iri
        .resolve(reference)
        .map_err(|x| CsvwError::IriError(reference.to_string(), x.to_string()))?;
    Ok(r.into_inner())
}

/// URL normalization used when comparing URLs (scheme and host are case insensitive, default
/// ports are removed).
pub fn normalize_url(u: &str) -> String {
    let mut s = u.to_string();
    if let Some(i) = s.find("://") {
        let scheme = s[..i].to_ascii_lowercase();
        let rest = &s[i + 3..];
        let auth_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let mut auth = rest[..auth_end].to_ascii_lowercase();
        if (scheme == "http" && auth.ends_with(":80"))
            || (scheme == "https" && auth.ends_with(":443"))
        {
            auth = auth.rsplit_once(':').unwrap().0.to_string();
        }
        s = format!("{scheme}://{auth}{}", &rest[auth_end..]);
    }
    s
}
