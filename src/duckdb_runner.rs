use anyhow::{Context, Result};
use duckdb::Connection;

use crate::{
    formatters::{Formatter, csv::CsvFormatter, vcf::VcfFormatter},
    manifest::FormatterKind,
    materialize::MaterializeJob,
};

pub fn configure_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        SET threads = 4;
        SET memory_limit = '4GB';
        "#,
    )?;
    Ok(())
}

pub fn run_job(conn: &Connection, job: MaterializeJob) -> Result<()> {
    let scan_sql = parquet_scan_sql(&job.files);

    let header_lines = if let Some(sql) = &job.route.spec.header_sql {
        let sql = job.route.render_sql_template(sql, &job.params)?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        out
    } else {
        Vec::new()
    };

    let formatter: Box<dyn Formatter> = match job.route.spec.formatter {
        FormatterKind::Vcf => Box::new(VcfFormatter {
            extra_header_lines: header_lines,
            sample_name: job
                .params
                .get("sample")
                .cloned()
                .unwrap_or_else(|| "SAMPLE".to_string()),
        }),
        FormatterKind::Csv => Box::new(CsvFormatter::new(true)),
    };

    formatter.write_header(&job.entry)?;

    let sql = job.route.spec.row_sql.replace("__PARQUET_SCAN__", &scan_sql);
    let sql = job.route.render_sql_template(&sql, &job.params)?;

    let mut stmt = conn.prepare(&sql).context("prepare row_sql")?;
    let batches = stmt.query_arrow([]).context("execute row_sql")?;

    for batch in batches {
        formatter.write_batch(&batch, &job.entry)?;
    }

    job.entry.finish();
    Ok(())
}

fn parquet_scan_sql(files: &[std::path::PathBuf]) -> String {
    let list = files
        .iter()
        .map(|p| format!("'{}'", p.to_string_lossy().replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "read_parquet([{}], hive_partitioning = true, union_by_name = true)",
        list
    )
}
