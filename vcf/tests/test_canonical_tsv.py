import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from vcf.canonical_tsv import SCHEMA_VERSION, canonicalize_vcf, reconstruct_vcf, split_vcf_to_package


ROOT = Path("/Users/eliascrum/PhD_Things/flatviewfs")
FIXTURES = ROOT / "vcf" / "test-files"


def read_tsv(path: Path):
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


class CanonicalTsvTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="canonical-vcf-tests-"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_curated_subset_fixture_shape(self):
        canonical = canonicalize_vcf(FIXTURES / "0GOOR_HG002.subset.vcf")
        self.assertEqual(len(canonical["records"]), 5)
        self.assertEqual(canonical["samples"], ["HG002_PacBio_Clc_OTS_PASS_Hg38_no_alt"])
        self.assertEqual(sum(1 for row in canonical["records"] if row["pos"] == "10177"), 2)
        self.assertIn("C,CC", [row["alt"] for row in canonical["records"]])

    def test_head1k_fixture_supported(self):
        canonical = canonicalize_vcf(FIXTURES / "head1k.vcf")
        self.assertEqual(len(canonical["records"]), 747)
        self.assertEqual(len(canonical["samples"]), 2504)
        self.assertEqual({row["format"] for row in canonical["records"]}, {"GT"})

    def test_split_writes_numeric_ids_and_parquet_defaults(self):
        package = split_vcf_to_package(FIXTURES / "NG131FQA1I.mixed_formats.subset.vcf", self.tmpdir)
        manifest = json.loads((package / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema_version"], SCHEMA_VERSION)

        records = read_tsv(package / "records.tsv")
        self.assertEqual(records[0]["record_id"], "1")
        self.assertEqual(records[0]["vcf_file_id"], "1")
        self.assertNotIn("record_index", records[0])
        self.assertTrue((package / "parquet" / "records.parquet").exists())
        self.assertTrue((package / "parquet" / "record_alt.parquet").exists())
        self.assertFalse((package / "record_info.tsv").exists())
        self.assertFalse((package / "record_info_values.tsv").exists())

        format_signature_fields = read_tsv(package / "format_signature_fields.tsv")
        self.assertTrue(all(row["format_signature_id"].isdigit() for row in format_signature_fields))
        self.assertTrue(any(row["field_name"] == "AD" and int(row["format_header_definition_id"]) > 0 for row in format_signature_fields))

        info_signature_fields = read_tsv(package / "info_signature_fields.tsv")
        self.assertTrue(any(row["field_name"] == "AC" and int(row["info_header_definition_id"]) > 0 for row in info_signature_fields))

    def test_header_linked_columns_are_used_for_signature_tables(self):
        package = split_vcf_to_package(FIXTURES / "NG131FQA1I.mixed_formats.subset.vcf", self.tmpdir)
        genotype_sig_1 = read_tsv(package / "genotype_sig_1.tsv")
        genotype_sig_2 = read_tsv(package / "genotype_sig_2.tsv")
        info_sig_1 = read_tsv(package / "info_sig_1.tsv")
        alt_rows = read_tsv(package / "record_alt.tsv")
        samples = read_tsv(package / "samples.tsv")
        format_signatures = read_tsv(package / "format_signatures.tsv")
        info_signatures = read_tsv(package / "info_signatures.tsv")

        self.assertIn("field_3_ad", genotype_sig_1[0])
        self.assertIn("field_4_dp", genotype_sig_1[0])
        self.assertIn("field_5_gq", genotype_sig_1[0])
        self.assertIn("field_6_gt", genotype_sig_1[0])
        self.assertIn("field_11_ac", info_sig_1[0])
        self.assertIn("field_13_an", info_sig_1[0])
        self.assertNotIn("sample_index", genotype_sig_1[0])
        self.assertNotIn("sample_index", samples[0])
        self.assertNotIn("format_index", format_signatures[0])
        self.assertNotIn("info_index", info_signatures[0])
        self.assertEqual(list(alt_rows[0].keys()), ["vcf_file_id", "record_id", "alt_index", "alt_value"])

    def test_round_trip_semantics_preserved(self):
        for fixture_name in ["0GOOR_HG002.subset.vcf", "NG131FQA1I.mixed_formats.subset.vcf", "head1k.vcf"]:
            source = FIXTURES / fixture_name
            package = split_vcf_to_package(source, self.tmpdir / source.stem)
            reconstructed = self.tmpdir / f"{source.stem}.reconstructed.vcf"
            reconstruct_vcf(package, reconstructed)
            self.assertEqual(canonicalize_vcf(source), canonicalize_vcf(reconstructed))

    def test_duckdb_parquet_layer_uses_lists(self):
        import duckdb

        package = split_vcf_to_package(FIXTURES / "NG131FQA1I.mixed_formats.subset.vcf", self.tmpdir)
        connection = duckdb.connect(database=":memory:")
        row = connection.execute(
            f"SELECT * FROM read_parquet('{package / 'parquet' / 'genotype_sig_1.parquet'}') LIMIT 1"
        ).fetchone()
        description = connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{package / 'parquet' / 'genotype_sig_1.parquet'}')"
        ).fetchall()
        type_by_name = {name: dtype for name, dtype, *_ in description}
        self.assertEqual(type_by_name["field_3_ad"], "BIGINT[]")
        self.assertEqual(type_by_name["field_4_dp"], "BIGINT")
        self.assertIsInstance(row[3], str)
        self.assertIsInstance(row[4], list)

        info_description = connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{package / 'parquet' / 'info_sig_1.parquet'}')"
        ).fetchall()
        info_types = {name: dtype for name, dtype, *_ in info_description}
        self.assertEqual(info_types["field_11_ac"], "BIGINT[]")
        self.assertEqual(info_types["field_12_af"], "DOUBLE[]")
        self.assertEqual(info_types["field_13_an"], "BIGINT")


if __name__ == "__main__":
    unittest.main()
