use anyhow::{Result, anyhow};
use duckdb::arrow::{
    array::{Array, Float64Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};

use crate::{cache::CacheEntry, formatters::Formatter};

pub struct VcfFormatter {
    pub extra_header_lines: Vec<String>,
    pub sample_name: String,
}

impl Formatter for VcfFormatter {
    fn write_header(&self, entry: &CacheEntry) -> Result<()> {
        entry.append(b"##fileformat=VCFv4.3\n")?;

        for line in &self.extra_header_lines {
            entry.append(line.as_bytes())?;
            entry.append(b"\n")?;
        }

        let header = format!(
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{}\n",
            self.sample_name
        );
        entry.append(header.as_bytes())?;
        Ok(())
    }

    fn write_batch(&self, batch: &RecordBatch, entry: &CacheEntry) -> Result<()> {
        let chrom = str_col(batch, 0, "chrom")?;
        let pos = int_col(batch, 1, "pos")?;
        let id = str_col(batch, 2, "id")?;
        let r#ref = str_col(batch, 3, "ref")?;
        let alt = str_col(batch, 4, "alt")?;
        let qual = maybe_str_or_float_col(batch, 5)?;
        let filter = str_col(batch, 6, "filter")?;
        let info = str_col(batch, 7, "info")?;
        let fmt = str_col(batch, 8, "fmt")?;
        let sample_value = str_col(batch, 9, "sample_value")?;

        for row in 0..batch.num_rows() {
            let line = format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                val_str(chrom, row, "."),
                val_i64(pos, row, 0),
                val_str(id, row, "."),
                val_str(r#ref, row, "N"),
                val_str(alt, row, "."),
                val_mixed(&qual, row, "."),
                val_str(filter, row, "."),
                val_str(info, row, "."),
                val_str(fmt, row, "."),
                val_str(sample_value, row, "."),
            );
            entry.append(line.as_bytes())?;
        }

        Ok(())
    }
}

enum MixedCol<'a> {
    Str(&'a StringArray),
    F64(&'a Float64Array),
}

fn str_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column {name} at index {idx} is not StringArray"))
}

fn int_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("column {name} at index {idx} is not Int64Array"))
}

fn maybe_str_or_float_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<MixedCol<'a>> {
    if let Some(x) = batch.column(idx).as_any().downcast_ref::<StringArray>() {
        return Ok(MixedCol::Str(x));
    }
    if let Some(x) = batch.column(idx).as_any().downcast_ref::<Float64Array>() {
        return Ok(MixedCol::F64(x));
    }
    Err(anyhow!(
        "column {idx} is neither StringArray nor Float64Array"
    ))
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

fn val_mixed(col: &MixedCol<'_>, row: usize, default: &str) -> String {
    match col {
        MixedCol::Str(x) => {
            if x.is_null(row) {
                default.to_string()
            } else {
                x.value(row).to_string()
            }
        }
        MixedCol::F64(x) => {
            if x.is_null(row) {
                default.to_string()
            } else {
                x.value(row).to_string()
            }
        }
    }
}
