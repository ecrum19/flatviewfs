use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, anyhow};
use blake3::Hasher;
use glob::glob;

use crate::manifest::{Manifest, RouteSpec};

#[derive(Debug)]
pub struct PreparedSql {
    /// The SQL string with parameters replaced by positional placeholders.
    pub sql: String,
    /// Values bound to each placeholder in order of appearance.
    pub params: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CompiledManifest {
    pub routes: Vec<CompiledRoute>,
    pub static_dirs: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct CompiledRoute {
    pub spec: RouteSpec,
    parts: Vec<TemplatePart>,
}

#[derive(Debug, Clone)]
enum TemplatePart {
    Lit(String),
    Var(String),
}

#[derive(Debug, Clone)]
pub struct MatchedRoute {
    pub route_index: usize,
    pub params: BTreeMap<String, String>,
}

impl CompiledManifest {
    pub fn compile(manifest: Manifest) -> Result<Self> {
        let mut routes = Vec::new();
        let mut static_dirs = BTreeSet::new();
        static_dirs.insert("/".to_string());

        for spec in manifest.routes {
            let compiled = CompiledRoute::new(spec)?;
            for d in compiled.literal_dirs() {
                static_dirs.insert(d);
            }
            routes.push(compiled);
        }

        Ok(Self { routes, static_dirs })
    }

    pub fn match_path(&self, path: &str) -> Option<MatchedRoute> {
        self.routes
            .iter()
            .enumerate()
            .find_map(|(i, r)| {
                r.match_path(path).map(|params| MatchedRoute {
                    route_index: i,
                    params,
                })
            })
    }
}

impl CompiledRoute {
    pub fn new(spec: RouteSpec) -> Result<Self> {
        Ok(Self {
            parts: parse_template(&spec.path)?,
            spec,
        })
    }

    pub fn match_path(&self, path: &str) -> Option<BTreeMap<String, String>> {
        match_template(&self.parts, path)
    }

    pub fn render_path_template(
        &self,
        template: &str,
        params: &BTreeMap<String, String>,
    ) -> Result<String> {
        render_template(template, params, false)
    }

    pub fn render_sql_template(
        &self,
        template: &str,
        params: &BTreeMap<String, String>,
    ) -> Result<PreparedSql> {
        render_template_prepared(template, params)
    }

    pub fn literal_dirs(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut acc = String::new();

        for seg in self.spec.path.trim_start_matches('/').split('/') {
            if seg.contains('{') {
                break;
            }
            acc.push('/');
            acc.push_str(seg);
            out.push(acc.clone());
        }

        out
    }

    pub fn resolve_files(&self, params: &BTreeMap<String, String>) -> Result<Vec<PathBuf>> {
        let pattern = self.render_path_template(&self.spec.source_glob, params)?;
        let mut files = Vec::new();

        for entry in glob(&pattern).with_context(|| format!("bad glob pattern: {pattern}"))? {
            let path = entry?;
            if path.is_file() {
                files.push(path);
            }
        }

        files.sort();
        files.dedup();

        if files.is_empty() {
            return Err(anyhow!("no parquet files matched {}", pattern));
        }

        Ok(files)
    }

    pub fn snapshot_key(
        &self,
        params: &BTreeMap<String, String>,
        files: &[PathBuf],
    ) -> Result<String> {
        let mut hasher = Hasher::new();
        hasher.update(self.spec.path.as_bytes());
        hasher.update(self.spec.source_glob.as_bytes());
        hasher.update(self.spec.row_sql.as_bytes());
        if let Some(h) = &self.spec.header_sql {
            hasher.update(h.as_bytes());
        }

        for (k, v) in params {
            hasher.update(k.as_bytes());
            hasher.update(v.as_bytes());
        }

        for path in files {
            let meta = fs::metadata(path)?;
            hasher.update(path.to_string_lossy().as_bytes());
            hasher.update(&meta.len().to_le_bytes());
            let mtime = meta.modified()?.duration_since(UNIX_EPOCH)?.as_secs();
            hasher.update(&mtime.to_le_bytes());
        }

        Ok(hasher.finalize().to_hex().to_string())
    }
}

fn parse_template(input: &str) -> Result<Vec<TemplatePart>> {
    let mut parts = Vec::new();
    let mut buf = String::new();
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '{' {
            if !buf.is_empty() {
                parts.push(TemplatePart::Lit(std::mem::take(&mut buf)));
            }

            let mut name = String::new();
            while let Some(&c) = chars.peek() {
                chars.next();
                if c == '}' {
                    break;
                }
                name.push(c);
            }

            if name.is_empty() {
                return Err(anyhow!("empty variable in template: {input}"));
            }

            parts.push(TemplatePart::Var(name));
        } else {
            buf.push(ch);
        }
    }

    if !buf.is_empty() {
        parts.push(TemplatePart::Lit(buf));
    }

    Ok(parts)
}

fn match_template(parts: &[TemplatePart], input: &str) -> Option<BTreeMap<String, String>> {
    let mut rest = input;
    let mut out = BTreeMap::new();

    for i in 0..parts.len() {
        match &parts[i] {
            TemplatePart::Lit(lit) => {
                if !rest.starts_with(lit) {
                    return None;
                }
                rest = &rest[lit.len()..];
            }
            TemplatePart::Var(name) => {
                let next_lit = parts[i + 1..].iter().find_map(|p| match p {
                    TemplatePart::Lit(x) => Some(x),
                    TemplatePart::Var(_) => None,
                });

                if let Some(next) = next_lit {
                    let idx = rest.find(next)?;
                    out.insert(name.clone(), rest[..idx].to_string());
                    rest = &rest[idx..];
                } else {
                    out.insert(name.clone(), rest.to_string());
                    rest = "";
                }
            }
        }
    }

    if rest.is_empty() {
        Some(out)
    } else {
        None
    }
}

fn render_template(
    template: &str,
    params: &BTreeMap<String, String>,
    sql_quote: bool,
) -> Result<String> {
    let parts = parse_template(template)?;
    let mut out = String::new();

    for part in parts {
        match part {
            TemplatePart::Lit(x) => out.push_str(&x),
            TemplatePart::Var(name) => {
                let value = params
                    .get(&name)
                    .ok_or_else(|| anyhow!("missing template variable {name}"))?;
                if sql_quote {
                    out.push('\'');
                    out.push_str(&value.replace('\'', "''"));
                    out.push('\'');
                } else {
                    out.push_str(value);
                }
            }
        }
    }

    Ok(out)
}

fn render_template_prepared(
    template: &str,
    params: &BTreeMap<String, String>,
) -> Result<PreparedSql> {
    let parts = parse_template(template)?;
    let mut sql = String::new();
    let mut args = Vec::new();

    for part in parts {
        match part {
            TemplatePart::Lit(x) => sql.push_str(&x),
            TemplatePart::Var(name) => {
                let value = params
                    .get(&name)
                    .ok_or_else(|| anyhow!("missing template variable {name}"))?;
                sql.push('?');
                args.push(value.clone());
            }
        }
    }

    Ok(PreparedSql { sql, params: args })
}

pub fn path_parent(path: &str) -> &str {
    if path == "/" {
        return "/";
    }
    Path::new(path)
        .parent()
        .and_then(|p| p.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("/")
}
