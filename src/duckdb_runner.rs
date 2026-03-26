use anyhow::{Context, Result};
use duckdb::{Connection, params_from_iter};

use crate::{
    formatters::{Formatter, csv::CsvFormatter, vcf::VcfFormatter},
    manifest::FormatterKind,
    materialize::MaterializeJob,
    route::PreparedSql,
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

fn render_prepared(sql: &str, job: &MaterializeJob, scan_sql: &str) -> Result<PreparedSql> {
    let params = job.params.clone();
    let package_dir = job
        .files
        .first()
        .and_then(|f| f.parent())
        .and_then(|p| p.parent())
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    let mut template = sql.to_string();
    if !package_dir.is_empty() {
        let escaped = package_dir.replace('\'', "''");
        template = template.replace("{__package_dir}", &format!("'{escaped}'"));
    }

    let mut rendered = job.route.render_sql_template(&template, &params)?;
    rendered.sql = rendered.sql.replace("__PARQUET_SCAN__", scan_sql);
    Ok(rendered)
}

pub fn run_job(conn: &Connection, job: MaterializeJob) -> Result<()> {
    let scan_sql = parquet_scan_sql(&job.files);

    let header_lines = if let Some(sql) = &job.route.spec.header_sql {
        let rendered = render_prepared(sql, &job, &scan_sql)?;
        let mut stmt = conn.prepare(&rendered.sql)?;
        let rows = stmt.query_map(params_from_iter(rendered.params.iter()), |row| {
            row.get::<_, String>(0)
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        out
    } else {
        Vec::new()
    };

    let formatter: Box<dyn Formatter> = match job.route.spec.formatter {
        FormatterKind::Vcf => Box::new(VcfFormatter::new(
            header_lines,
            job.params
                .get("sample")
                .cloned()
                .unwrap_or_else(|| "SAMPLE".to_string()),
        )),
        FormatterKind::Csv => Box::new(CsvFormatter::new(true)),
    };

    formatter.write_header(&job.entry)?;

    let rendered = render_prepared(&job.route.spec.row_sql, &job, &scan_sql)?;

    let mut stmt = conn.prepare(&rendered.sql).context("prepare row_sql")?;
    let batches = stmt
        .query_arrow(params_from_iter(rendered.params.iter()))
        .context("execute row_sql")?;

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
