use std::{collections::HashMap, path::Path, sync::Mutex};

use anyhow::{Context, Result, anyhow};
use duckdb::arrow::{
    array::{Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use serde::Deserialize;

use crate::{cache::CacheEntry, formatters::Formatter};

/// VCF formatter that reconstructs VCF text from the canonical split/Parquet package.
///
/// The formatter expects the query to supply:
/// - string columns: `chrom`, `id`, `ref`, `alt_text`, `qual`, `filter_text`, `sample_name`,
///   `info_json`, `format_json`, `package_dir`
/// - integer columns: `pos`, `format_signature_id`, `info_signature_id`
/// The `info_json` and `format_json` columns contain JSON encodings of the row from the
/// canonical info/genotype Parquet tables. Metadata TSVs in the package directory are used
/// to map field IDs back to VCF field names and ordering.
pub struct VcfFormatter {
    pub extra_header_lines: Vec<String>,
    pub sample_name: String,
    meta_cache: Mutex<HashMap<String, PackageMeta>>,
}

impl VcfFormatter {
    pub fn new(extra_header_lines: Vec<String>, sample_name: String) -> Self {
        Self {
            extra_header_lines,
            sample_name,
            meta_cache: Mutex::new(HashMap::new()),
        }
    }
}

impl Formatter for VcfFormatter {
    fn write_header(&self, entry: &CacheEntry) -> Result<()> {
        if self.extra_header_lines.is_empty() {
            let header = format!(
                "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{}\n",
                self.sample_name
            );
            entry.append(header.as_bytes())?;
            return Ok(());
        }

        for line in &self.extra_header_lines {
            entry.append(line.as_bytes())?;
            entry.append(b"\n")?;
        }
        Ok(())
    }

    fn write_batch(&self, batch: &RecordBatch, entry: &CacheEntry) -> Result<()> {
        let record_id = int_col(batch, "record_id")?;
        let chrom = str_col(batch, "chrom")?;
        let pos = int_col(batch, "pos")?;
        let id = str_col(batch, "id")?;
        let r#ref = str_col(batch, "ref")?;
        let alt = str_col(batch, "alt_text")?;
        let qual = str_col(batch, "qual")?;
        let filter = str_col(batch, "filter_text")?;
        let sample_id = int_col(batch, "sample_id")?;
        let format_sig = int_col(batch, "format_signature_id")?;
        let info_sig = int_col(batch, "info_signature_id")?;
        let package_dir = str_col(batch, "package_dir")?;

        for row in 0..batch.num_rows() {
            let package_dir = val_str(package_dir, row, "");
            let meta = self.load_meta(package_dir)?;
            let format_signature_id = val_i64(format_sig, row, 0);
            let info_signature_id = val_i64(info_sig, row, 0);
            let record_id = val_i64(record_id, row, 0);
            let sample_id = val_i64(sample_id, row, 0);
            let info_text = render_info(
                info_signature_id,
                record_id,
                &meta,
            )?;
            let (format_text, sample_text) = render_genotype(
                format_signature_id,
                record_id,
                sample_id,
                &meta,
            )?;

            let line = format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                val_str(chrom, row, "."),
                val_i64(pos, row, 0),
                val_str(id, row, "."),
                val_str(r#ref, row, "N"),
                val_str(alt, row, "."),
                val_str(qual, row, "."),
                val_str(filter, row, "."),
                info_text,
                format_text,
                sample_text,
            );
            entry.append(line.as_bytes())?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FormatField {
    #[serde(rename = "format_signature_id")]
    signature_id: i64,
    #[serde(rename = "field_index")]
    index: i64,
    #[serde(rename = "field_name")]
    name: String,
    #[serde(rename = "source_column")]
    source: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FormatSignature {
    #[serde(rename = "format_signature_id")]
    signature_id: i64,
    #[serde(rename = "format_string")]
    format_string: String,
    #[serde(rename = "genotype_file")]
    genotype_file: String,
}

#[derive(Debug, Clone, Deserialize)]
struct InfoField {
    #[serde(rename = "info_signature_id")]
    signature_id: i64,
    #[serde(rename = "field_index")]
    index: i64,
    #[serde(rename = "info_key")]
    name: String,
    #[serde(rename = "source_column")]
    source: String,
    #[serde(rename = "is_flag")]
    is_flag: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct InfoSignatureRow {
    #[serde(rename = "info_signature_id")]
    _signature_id: i64,
    #[serde(rename = "info_file")]
    info_file: String,
}

#[derive(Debug, Default, Clone)]
struct PackageMeta {
    format_fields: HashMap<i64, Vec<FormatField>>,
    format_strings: HashMap<i64, String>,
    info_fields: HashMap<i64, Vec<InfoField>>,
    info_rows: HashMap<i64, HashMap<String, String>>,
    genotype_rows: HashMap<(i64, i64), HashMap<String, String>>,
}

fn parse_tsv<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    for record in rdr.deserialize() {
        out.push(record?);
    }
    Ok(out)
}

fn read_rows(path: &Path) -> Result<Vec<HashMap<String, String>>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let headers = rdr
        .headers()
        .with_context(|| format!("reading headers from {}", path.display()))?
        .clone();
    let mut rows = Vec::new();
    for record in rdr.records() {
        let record = record?;
        let mut map = HashMap::new();
        for (h, v) in headers.iter().zip(record.iter()) {
            map.insert(h.to_string(), v.to_string());
        }
        rows.push(map);
    }
    Ok(rows)
}

impl VcfFormatter {
    fn load_meta(&self, package_dir: &str) -> Result<PackageMeta> {
        {
            if let Some(cached) = self.meta_cache.lock().unwrap().get(package_dir) {
                return Ok(cached.clone());
            }
        }

        let base = Path::new(package_dir);
        let mut meta = PackageMeta::default();

        let format_signatures: Vec<FormatSignature> =
            parse_tsv(&base.join("format_signatures.tsv"))?;
        for sig in &format_signatures {
            meta.format_strings
                .insert(sig.signature_id, sig.format_string.clone());
        }

        let mut fields: Vec<FormatField> = parse_tsv(&base.join("format_signature_fields.tsv"))?;
        fields.sort_by_key(|f| (f.signature_id, f.index));
        for f in fields {
            meta.format_fields
                .entry(f.signature_id)
                .or_default()
                .push(f);
        }

        let mut info_fields: Vec<InfoField> = parse_tsv(&base.join("info_signature_fields.tsv"))?;
        info_fields.sort_by_key(|f| (f.signature_id, f.index));
        for f in info_fields {
            meta.info_fields
                .entry(f.signature_id)
                .or_default()
                .push(f);
        }

        // Load info rows keyed by record_id.
        let info_sigs: Vec<InfoSignatureRow> =
            parse_tsv(&base.join("info_signatures.tsv"))?;
        for sig in info_sigs {
            let path = base.join(sig.info_file);
            let rows = read_rows(&path)?;
            for mut row in rows {
                let record_id = row
                    .remove("record_id")
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or_default();
                row.remove("vcf_file_id");
                meta.info_rows.insert(record_id, row);
            }
        }

        // Load genotype rows keyed by (record_id, sample_id).
        for sig in format_signatures {
            let path = base.join(sig.genotype_file);
            let rows = read_rows(&path)?;
            for mut row in rows {
                let record_id = row
                    .remove("record_id")
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or_default();
                let sample_id = row
                    .remove("sample_id")
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or_default();
                row.remove("vcf_file_id");
                meta.genotype_rows.insert((record_id, sample_id), row);
            }
        }

        self.meta_cache
            .lock()
            .unwrap()
            .insert(package_dir.to_string(), meta.clone());
        Ok(meta)
    }
}

fn render_info(
    sig_id: i64,
    record_id: i64,
    meta: &PackageMeta,
) -> Result<String> {
    if sig_id == 0 {
        return Ok(".".to_string());
    }
    let fields = match meta.info_fields.get(&sig_id) {
        Some(f) if !f.is_empty() => f,
        _ => return Ok(".".to_string()),
    };
    let data = meta.info_rows.get(&record_id);
    let mut parts = Vec::new();
    for field in fields {
        let val = data.and_then(|m| m.get(&field.source));
        if field.is_flag != 0 && matches!(val, Some(v) if v != "0" && !v.is_empty() && v != ".") {
            parts.push(field.name.clone());
        } else if let Some(rendered) = val {
            if !rendered.is_empty() && rendered != "." {
                parts.push(format!("{}={}", field.name, rendered));
            }
        }
    }
    if parts.is_empty() {
        Ok(".".to_string())
    } else {
        Ok(parts.join(";"))
    }
}

fn render_genotype(
    sig_id: i64,
    record_id: i64,
    sample_id: i64,
    meta: &PackageMeta,
) -> Result<(String, String)> {
    if sig_id == 0 {
        return Ok((".".to_string(), ".".to_string()));
    }
    let fields = match meta.format_fields.get(&sig_id) {
        Some(f) if !f.is_empty() => f,
        _ => return Ok((".".to_string(), ".".to_string())),
    };
    let empty = HashMap::new();
    let data = meta
        .genotype_rows
        .get(&(record_id, sample_id))
        .unwrap_or(&empty);

    let mut values = Vec::new();
    for field in fields {
        let val = data.get(&field.source);
        values.push(
            val.map(|s| s.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(".")
                .to_string(),
        );
    }

    let format_text = meta
        .format_strings
        .get(&sig_id)
        .cloned()
        .unwrap_or_else(|| fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>().join(":"));

    let sample_text = values.join(":");
    Ok((format_text, sample_text))
}

fn str_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = batch
        .schema()
        .column_with_name(name)
        .map(|(i, _)| i)
        .ok_or_else(|| anyhow!("missing column {name}"))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column {name} is not StringArray"))
}

fn int_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let idx = batch
        .schema()
        .column_with_name(name)
        .map(|(i, _)| i)
        .ok_or_else(|| anyhow!("missing column {name}"))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("column {name} is not Int64Array"))
}

fn val_str<'a>(col: &'a StringArray, row: usize, default: &'a str) -> &'a str {
    if col.is_null(row) {
        default
    } else {
        col.value(row)
    }
}

fn val_i64(col: &Int64Array, row: usize, default: i64) -> i64 {
    if col.is_null(row) {
        default
    } else {
        col.value(row)
    }
}
