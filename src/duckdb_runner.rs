use anyhow::{Context, Result};
use duckdb::{params_from_iter, Connection};

use crate::{
    formatters::{csv::CsvFormatter, vcf::VcfFormatter, Formatter},
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
    let mut params = job.params.clone();
    if let Some(root) = job.route.package_root(&params) {
        params.insert("package_root".to_string(), root.clone());
        let escaped = root.replace('\'', "''");
        let mut template = sql.to_string();
        template = template.replace("{__package_dir}", &format!("'{escaped}'"));
        let mut rendered = job.route.render_sql_template(&template, &params)?;
        rendered.sql = rendered.sql.replace("__PARQUET_SCAN__", scan_sql);
        return Ok(rendered);
    }

    let mut rendered = job.route.render_sql_template(sql, &params)?;
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
    let row_sql = rendered
        .sql
        .trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    let schema_sql = format!("SELECT * FROM ({row_sql}) LIMIT 0");
    let schema = {
        let mut stmt = conn.prepare(&schema_sql)?;
        let arrow = stmt.query_arrow(params_from_iter(rendered.params.iter()))?;
        arrow.get_schema()
    };

    if job.entry.is_failed() {
        return Ok(());
    }

    let mut stmt = conn.prepare(row_sql).context("prepare row_sql")?;
    let mut stream = stmt
        .stream_arrow(params_from_iter(rendered.params.iter()), schema)
        .context("execute row_sql")?;

    for batch in &mut stream {
        if job.entry.is_failed() {
            break;
        }
        formatter.write_batch(&batch, &job.entry)?;
        let produced = job.entry.size_hint();
        job.entry.wait_for_need(produced)?;
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
