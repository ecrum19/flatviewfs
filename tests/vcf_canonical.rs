use std::{collections::BTreeMap, fs, path::Path};

use duckdb::Connection;
use flatviewfs::{
    cache::CacheManager,
    duckdb_runner::{configure_connection, run_job},
    manifest::Manifest,
    materialize::MaterializeJob,
    route::CompiledManifest,
};

fn params(package: &str, sample: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert("package".into(), package.into());
    map.insert("sample".into(), sample.into());
    map
}

#[test]
fn canonical_subset_reconstructs_vcf() {
    let manifest = Manifest::load(Path::new("examples/vcf-canonical.toml")).unwrap();
    let compiled = CompiledManifest::compile(manifest).unwrap();

    let package = "0GOOR_HG002_subset";
    let sample_name = "HG002_PacBio_Clc_OTS_PASS_Hg38_no_alt";
    let path = format!("/vcf/{package}/{sample_name}.vcf");

    let matched = compiled.match_path(&path).expect("route match");
    let route = compiled.routes[matched.route_index].clone();
    let params = params(package, sample_name);
    let files = route.resolve_files(&params).unwrap();
    let key = route.snapshot_key(&params, &files).unwrap();

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = CacheManager::new(cache_dir.path()).unwrap();
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

    let rendered = fs::read_to_string(entry.data_path.clone()).unwrap();
    let expected = fs::read_to_string("vcf/tests/test-files/0GOOR_HG002_subset.vcf").unwrap();

    assert_eq!(rendered, expected);
}
