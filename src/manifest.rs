use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    pub routes: Vec<RouteSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteSpec {
    pub path: String,
    pub source_glob: String,
    pub formatter: FormatterKind,
    pub header_sql: Option<String>,
    pub row_sql: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FormatterKind {
    Vcf,
    Csv,
}

impl Manifest {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("reading manifest {}", path.display()))?;
        let manifest: Manifest = toml::from_str(&text)
            .with_context(|| format!("parsing manifest {}", path.display()))?;
        Ok(manifest)
    }
}
