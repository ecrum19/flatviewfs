use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use duckdb::arrow::{
    array::Array, record_batch::RecordBatch, util::display::array_value_to_string,
};

use crate::{cache::CacheEntry, formatters::Formatter};

/// Minimal CSV formatter: writes a header row once, then comma-separated values per batch.
pub struct CsvFormatter {
    write_header_row: bool,
    header_written: AtomicBool,
}

impl CsvFormatter {
    pub fn new(write_header_row: bool) -> Self {
        Self {
            write_header_row,
            header_written: AtomicBool::new(false),
        }
    }
}

impl Formatter for CsvFormatter {
    fn write_header(&self, _entry: &CacheEntry) -> Result<()> {
        // header is emitted on first batch because we need the schema.
        Ok(())
    }

    fn write_batch(&self, batch: &RecordBatch, entry: &CacheEntry) -> Result<()> {
        let mut buffer = String::new();
        // Emit header once when the first batch arrives.
        if self.write_header_row && !self.header_written.swap(true, Ordering::SeqCst) {
            let header = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .map(csv_escape)
                .collect::<Vec<_>>()
                .join(",");
            buffer.push_str(&header);
            buffer.push('\n');
        }

        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for col in 0..batch.num_columns() {
                let array = batch.column(col);
                if array.is_null(row) {
                    cells.push(String::new());
                } else {
                    cells.push(csv_escape(array_value_to_string(array.as_ref(), row)?));
                }
            }
            let line = cells.join(",");
            buffer.push_str(&line);
            buffer.push('\n');
        }

        if !buffer.is_empty() {
            entry.append(buffer.as_bytes())?;
        }
        Ok(())
    }
}

fn csv_escape(val: String) -> String {
    if val.contains(',') || val.contains('"') || val.contains('\n') {
        let escaped = val.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        val
    }
}
