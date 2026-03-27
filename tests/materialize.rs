use std::collections::BTreeMap;

use duckdb::Connection;
use flatviewfs::{
    cache::CacheManager,
    duckdb_runner::{configure_connection, run_job},
    manifest::{FormatterKind, Manifest, RouteSpec},
    materialize::MaterializeJob,
    route::CompiledManifest,
};

fn bmap(k: &str, v: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert(k.to_string(), v.to_string());
    map
}

fn make_parquet(conn: &Connection, sql: &str, path: &std::path::Path) {
    let copy_sql = format!("COPY ({sql}) TO '{}' (FORMAT PARQUET);", path.display());
    conn.execute_batch(&copy_sql).expect("copy parquet");
}

#[test]
fn materializes_csv_small_example() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().join("tiny.parquet");

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE t AS
        SELECT 1 AS id, 'alpha' AS name
        UNION ALL
        SELECT 2 AS id, 'beta' AS name;
        "#,
    )
    .unwrap();
    make_parquet(&conn, "SELECT * FROM t ORDER BY id", &data_path);

    let manifest = Manifest {
        routes: vec![RouteSpec {
            path: "/tables/{table}.csv".into(),
            source_glob: format!("{}/{{table}}.parquet", tmp.path().display()),
            extra_inputs: vec![],
            package_root: None,
            formatter: FormatterKind::Csv,
            header_sql: None,
            row_sql: "SELECT id, name FROM __PARQUET_SCAN__ ORDER BY id".into(),
        }],
    };

    let compiled = CompiledManifest::compile(manifest).unwrap();
    let route = compiled.routes[0].clone();
    let params = bmap("table", "tiny");
    let files = route.resolve_files(&params).unwrap();
    let key = route.snapshot_key(&params, &files).unwrap();

    let cache = CacheManager::new(tmp.path().join("cache")).unwrap();
    let (entry, _) = cache.get_or_create(&key).unwrap();

    let job = MaterializeJob {
        route,
        params,
        files,
        entry: entry.clone(),
    };

    let conn = Connection::open_in_memory().unwrap();
    configure_connection(&conn).unwrap();
    run_job(&conn, job).unwrap();

    let text = std::fs::read_to_string(entry.data_path.clone()).unwrap();
    assert_eq!(text, "id,name\n1,alpha\n2,beta\n");
}

#[test]
fn materializes_vcf_small_example() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().join("7.parquet");

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE variants AS
        SELECT
            CAST(1 AS BIGINT) AS record_id,
            '1' AS chrom,
            CAST(42 AS BIGINT)  AS pos,
            'rs1' AS id,
            'A' AS ref,
            'C' AS alt_text,
            '50' AS qual_text,
            'PASS' AS filter_text,
            7 AS sample_id,
            CAST(1 AS BIGINT) AS format_signature_id,
            CAST(1 AS BIGINT) AS info_signature_id;
        "#,
    )
    .unwrap();
    make_parquet(&conn, "SELECT * FROM variants", &data_path);

    let pkg_dir = tmp.path().join("pkg");
    std::fs::create_dir_all(&pkg_dir).unwrap();
    std::fs::write(
        pkg_dir.join("format_signatures.tsv"),
        "vcf_file_id\tformat_signature_id\tformat_string\tfield_count\tgenotype_file\tgenotype_parquet_path\n1\t1\tGT\t1\tgenotype_sig_1.tsv\tgenotype_sig_1.parquet\n",
    )
    .unwrap();
    std::fs::write(
        pkg_dir.join("format_signature_fields.tsv"),
        "vcf_file_id\tformat_signature_id\tfield_index\tformat_header_definition_id\tfield_name\tsource_column\tnumber\tvalue_type\n1\t1\t1\t1\tGT\tgt\t1\tString\n",
    )
    .unwrap();
    std::fs::write(
        pkg_dir.join("genotype_sig_1.tsv"),
        "vcf_file_id\trecord_id\tsample_id\tgt\n1\t1\t7\t0/1\n",
    )
    .unwrap();
    std::fs::write(
        pkg_dir.join("info_signatures.tsv"),
        "vcf_file_id\tinfo_signature_id\tinfo_string\tfield_count\tinfo_file\tinfo_parquet_path\n1\t1\tDP\t1\tinfo_sig_1.tsv\tinfo_sig_1.parquet\n",
    )
    .unwrap();
    std::fs::write(
        pkg_dir.join("info_signature_fields.tsv"),
        "vcf_file_id\tinfo_signature_id\tfield_index\theader_definition_id\tinfo_key\tsource_column\tnumber\tvalue_type\tis_flag\n1\t1\t1\t1\tDP\tdp\t1\tInteger\t0\n",
    )
    .unwrap();
    std::fs::write(
        pkg_dir.join("info_sig_1.tsv"),
        "vcf_file_id\trecord_id\tdp\n1\t1\t10\n",
    )
    .unwrap();

    let manifest = Manifest {
        routes: vec![RouteSpec {
            path: "/samples/{sample}.vcf".into(),
            source_glob: format!("{}/{{sample}}.parquet", tmp.path().display()),
            extra_inputs: vec![],
            package_root: None,
            formatter: FormatterKind::Vcf,
            header_sql: None,
            row_sql: format!(
                "SELECT record_id, chrom, pos, id, ref, alt_text, qual_text AS qual, filter_text, format_signature_id, info_signature_id, CAST(sample_id AS BIGINT) AS sample_id, 'SAMPLE' AS sample_name, '{}' AS package_dir FROM __PARQUET_SCAN__ WHERE sample_id = {{sample}} ORDER BY pos",
                pkg_dir.display()
            ),
        }],
    };

    let compiled = CompiledManifest::compile(manifest).unwrap();
    let route = compiled.routes[0].clone();
    let params = bmap("sample", "7");
    let files = route.resolve_files(&params).unwrap();
    let key = route.snapshot_key(&params, &files).unwrap();

    let cache = CacheManager::new(tmp.path().join("cache")).unwrap();
    let (entry, _) = cache.get_or_create(&key).unwrap();

    let job = MaterializeJob {
        route,
        params,
        files,
        entry: entry.clone(),
    };

    let conn = Connection::open_in_memory().unwrap();
    configure_connection(&conn).unwrap();
    run_job(&conn, job).unwrap();

    let text = std::fs::read_to_string(entry.data_path.clone()).unwrap();
    let expected = "\
##fileformat=VCFv4.3
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t7
1\t42\trs1\tA\tC\t50\tPASS\tDP=10\tGT\t0/1
";
    assert_eq!(text, expected);
}
