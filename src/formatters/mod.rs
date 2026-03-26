pub mod csv;
pub mod vcf;

use anyhow::Result;
use duckdb::arrow::record_batch::RecordBatch;

use crate::cache::CacheEntry;

pub trait Formatter {
    fn write_header(&self, entry: &CacheEntry) -> Result<()>;
    fn write_batch(&self, batch: &RecordBatch, entry: &CacheEntry) -> Result<()>;
}
