#!/usr/bin/env python3
"""Canonical VCF package writer with default typed Parquet exports."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = "2.0.0"
MISSING_NUMERIC_ID = 0
VENDOR_DIR = Path(__file__).with_name("_vendor")
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

import duckdb  # type: ignore  # noqa: E402


@dataclass(frozen=True)
class HeaderDefinition:
    header_definition_id: int
    header_key: str
    header_category: str
    structured_id: str
    number: str
    value_type: str
    description: str
    value: str
    extra_attributes_json: str
    raw_line: str


@dataclass(frozen=True)
class SampleDefinition:
    sample_id: int
    sample_index: int
    sample_name: str


@dataclass(frozen=True)
class FormatSignature:
    format_signature_id: int
    format_string: str
    genotype_file: str
    genotype_parquet_file: str


@dataclass(frozen=True)
class InfoSignature:
    info_signature_id: int
    info_string: str
    info_file: str
    info_parquet_file: str


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def safe_column_name(header_definition_id: int, field_name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", field_name).strip("_").lower()
    if not cleaned:
        cleaned = "value"
    return f"field_{header_definition_id}_{cleaned}"


def split_quoted(text: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    escape = False
    for char in text:
        if escape:
            current.append(char)
            escape = False
            continue
        if char == "\\":
            current.append(char)
            escape = True
            continue
        if char == '"':
            in_quotes = not in_quotes
            current.append(char)
            continue
        if char == delimiter and not in_quotes:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    parts.append("".join(current))
    return parts


def parse_angle_bracket_attributes(value: str) -> dict[str, str]:
    inner = value[1:-1]
    attributes: dict[str, str] = {}
    for part in split_quoted(inner, ","):
        if "=" in part:
            key, raw_value = part.split("=", 1)
            attributes[key] = raw_value
        elif part:
            attributes[part] = ""
    return attributes


def parse_meta_header_line(header_definition_id: int, raw_line: str) -> HeaderDefinition:
    content = raw_line[2:]
    if "=" not in content:
        return HeaderDefinition(
            header_definition_id=header_definition_id,
            header_key=content,
            header_category="scalar",
            structured_id="",
            number="",
            value_type="",
            description="",
            value="",
            extra_attributes_json="{}",
            raw_line=raw_line,
        )

    key, value = content.split("=", 1)
    if value.startswith("<") and value.endswith(">"):
        attributes = parse_angle_bracket_attributes(value)
        extra = {
            attr_key: attr_value
            for attr_key, attr_value in attributes.items()
            if attr_key not in {"ID", "Number", "Type", "Description"}
        }
        return HeaderDefinition(
            header_definition_id=header_definition_id,
            header_key=key,
            header_category="structured",
            structured_id=attributes.get("ID", ""),
            number=attributes.get("Number", ""),
            value_type=attributes.get("Type", ""),
            description=attributes.get("Description", ""),
            value="",
            extra_attributes_json=json.dumps(extra, sort_keys=True),
            raw_line=raw_line,
        )

    return HeaderDefinition(
        header_definition_id=header_definition_id,
        header_key=key,
        header_category="scalar",
        structured_id="",
        number="",
        value_type="",
        description="",
        value=value,
        extra_attributes_json="{}",
        raw_line=raw_line,
    )


def normalize_record(record: list[str], sample_count: int) -> list[str]:
    fields = list(record)
    while len(fields) < 9 + sample_count:
        fields.append(".")
    return fields


def parse_info_entries(info_text: str) -> tuple[list[tuple[int, str, bool, list[str]]], str]:
    if info_text in {"", "."}:
        return [], "."

    entries: list[tuple[int, str, bool, list[str]]] = []
    for info_index, token in enumerate(info_text.split(";"), start=1):
        if "=" in token:
            key, raw_values = token.split("=", 1)
            values = raw_values.split(",")
            entries.append((info_index, key, False, values))
        else:
            entries.append((info_index, token, True, []))
    return entries, info_text


def parse_filter_entries(filter_text: str) -> tuple[str, list[str]]:
    if filter_text in {"", "."}:
        return "MISSING", []
    if filter_text == "PASS":
        return "PASS", []
    return "VALUES", filter_text.split(";")


def parse_alt_entries(alt_text: str) -> tuple[str, list[str]]:
    if alt_text in {"", "."}:
        return "MISSING", []
    return "VALUES", alt_text.split(",")


def genotype_tokens(sample_text: str, field_names: list[str]) -> list[str]:
    if not field_names:
        return []
    if sample_text == "":
        sample_text = "."
    tokens = sample_text.split(":")
    if len(tokens) < len(field_names):
        tokens.extend(["."] * (len(field_names) - len(tokens)))
    return tokens[: len(field_names)]


def write_tsv(path: Path, header: list[str], rows: Iterable[Iterable[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def header_maps(header_definitions: list[HeaderDefinition]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    info_map: dict[str, int] = {}
    format_map: dict[str, int] = {}
    filter_map: dict[str, int] = {}
    for item in header_definitions:
        if item.header_key == "INFO" and item.structured_id:
            info_map[item.structured_id] = item.header_definition_id
        elif item.header_key == "FORMAT" and item.structured_id:
            format_map[item.structured_id] = item.header_definition_id
        elif item.header_key == "FILTER" and item.structured_id:
            filter_map[item.structured_id] = item.header_definition_id
    return info_map, format_map, filter_map


def build_info_signature(entries: list[tuple[int, str, bool, list[str]]], info_map: dict[str, int]) -> tuple[tuple[tuple[int, bool], ...], list[tuple[int, int, str, bool, list[str]]]]:
    signature_parts: list[tuple[int, bool]] = []
    resolved: list[tuple[int, int, str, bool, list[str]]] = []
    for info_index, info_key, is_flag, values in entries:
        header_definition_id = info_map.get(info_key, MISSING_NUMERIC_ID)
        signature_parts.append((header_definition_id, is_flag))
        resolved.append((info_index, header_definition_id, info_key, is_flag, values))
    return tuple(signature_parts), resolved


def build_format_signature(format_text: str, format_map: dict[str, int]) -> tuple[tuple[int, ...], list[tuple[int, str]]]:
    if format_text in {"", "."}:
        return tuple(), []
    resolved = [(format_map.get(field_name, MISSING_NUMERIC_ID), field_name) for field_name in format_text.split(":")]
    return tuple(item[0] for item in resolved), resolved


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def infer_duckdb_type(number: str, value_type: str, is_flag: bool) -> tuple[str, bool]:
    upper_type = value_type.upper()
    if is_flag or number == "0" or upper_type == "FLAG":
        return "BOOLEAN", False
    is_list = number not in {"", "1"}
    if upper_type == "INTEGER":
        return ("BIGINT[]", True) if is_list else ("BIGINT", False)
    if upper_type == "FLOAT":
        return ("DOUBLE[]", True) if is_list else ("DOUBLE", False)
    return ("VARCHAR[]", True) if is_list else ("VARCHAR", False)


def typed_expression(source_column: str, duckdb_type: str, is_list: bool) -> str:
    source = quote_identifier(source_column)
    if duckdb_type == "BOOLEAN":
        return f"CASE WHEN {source} IN ('1','true','TRUE') THEN TRUE WHEN {source} IN ('0','false','FALSE','.','') THEN FALSE ELSE NULL END AS {quote_identifier(source_column)}"
    if is_list:
        base_type = duckdb_type[:-2]
        if base_type == "VARCHAR":
            expr = f"CASE WHEN {source} IN ('.','') THEN NULL ELSE string_split({source}, ',') END"
        else:
            expr = (
                f"CASE WHEN {source} IN ('.','') THEN NULL "
                f"ELSE list_transform(string_split({source}, ','), x -> TRY_CAST(x AS {base_type})) END"
            )
        return f"{expr} AS {quote_identifier(source_column)}"
    cast_expr = f"NULLIF({source}, '.')"
    if duckdb_type == "VARCHAR":
        return f"{cast_expr} AS {quote_identifier(source_column)}"
    return f"TRY_CAST({cast_expr} AS {duckdb_type}) AS {quote_identifier(source_column)}"


def emit_typed_parquet(tsv_path: Path, parquet_path: Path, typed_columns: list[tuple[str, str, bool]]) -> None:
    connection = duckdb.connect(database=":memory:")
    try:
        select_parts = []
        for column_name, duckdb_type, is_list in typed_columns:
            if duckdb_type == "BIGINT" and not is_list:
                select_parts.append(
                    f"TRY_CAST(NULLIF({quote_identifier(column_name)}, '') AS BIGINT) AS {quote_identifier(column_name)}"
                )
            else:
                select_parts.append(typed_expression(column_name, duckdb_type, is_list))

        select_sql = ", ".join(select_parts)
        query = (
            f"COPY (SELECT {select_sql} "
            f"FROM read_csv_auto({sql_string(str(tsv_path))}, delim='\\t', header=true, all_varchar=true)) "
            f"TO {sql_string(str(parquet_path))} (FORMAT PARQUET)"
        )
        connection.execute(query)
    finally:
        connection.close()


def split_vcf_to_package(input_path: Path, output_root: Path) -> Path:
    package_name = input_path.name.replace(".vcf.gz", "").replace(".vcf", "")
    package_dir = output_root / package_name
    package_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir = package_dir / "parquet"
    parquet_dir.mkdir(exist_ok=True)

    vcf_file_id = 1
    header_lines: list[list[object]] = []
    header_definitions: list[HeaderDefinition] = []
    samples: list[SampleDefinition] = []
    records_rows: list[list[object]] = []
    record_alt_rows: list[list[object]] = []
    record_filter_rows: list[list[object]] = []
    genotype_rows_by_signature: dict[int, list[list[object]]] = defaultdict(list)
    info_rows_by_signature: dict[int, list[list[object]]] = defaultdict(list)
    format_signature_columns: dict[int, list[tuple[int, str, str]]] = {}
    info_signature_columns: dict[int, list[tuple[int, str, str, bool]]] = {}
    signatures_by_format: dict[tuple[int, ...], FormatSignature] = {}
    signatures_by_info: dict[tuple[tuple[int, bool], ...], InfoSignature] = {}
    format_signature_fields_rows: list[list[object]] = []
    info_signature_fields_rows: list[list[object]] = []

    sample_names: list[str] = []
    header_count = 0
    record_count = 0
    with open_text(input_path) as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n").rstrip("\r")
            if line.startswith("##"):
                header_count += 1
                header_lines.append([vcf_file_id, header_count, "META", line])
                header_definitions.append(parse_meta_header_line(header_count, line))
                continue

            if line.startswith("#CHROM\t"):
                header_count += 1
                header_lines.append([vcf_file_id, header_count, "COLUMNS", line])
                columns = line.split("\t")
                sample_names = columns[9:]
                for sample_index, sample_name in enumerate(sample_names, start=1):
                    samples.append(SampleDefinition(sample_id=sample_index, sample_index=sample_index, sample_name=sample_name))
                continue

            if not line:
                continue

            info_map, format_map, filter_map = header_maps(header_definitions)
            fields = normalize_record(line.split("\t"), len(sample_names))
            record_count += 1
            record_id = record_count
            chrom, pos, record_name, ref, alt_text, qual, filter_text, info_text, format_text = fields[:9]
            sample_values = fields[9:]

            alt_state, alt_values = parse_alt_entries(alt_text)
            filter_state, filter_values = parse_filter_entries(filter_text)
            info_entries, info_raw_text = parse_info_entries(info_text)
            info_signature_key, resolved_info_entries = build_info_signature(info_entries, info_map)
            format_signature_key, resolved_format_fields = build_format_signature(format_text, format_map)

            info_signature_id = MISSING_NUMERIC_ID
            if info_signature_key:
                if info_signature_key not in signatures_by_info:
                    new_id = len(signatures_by_info) + 1
                    info_file = f"info_sig_{new_id}.tsv"
                    parquet_file = f"info_sig_{new_id}.parquet"
                    signatures_by_info[info_signature_key] = InfoSignature(
                        info_signature_id=new_id,
                        info_string=info_raw_text,
                        info_file=info_file,
                        info_parquet_file=parquet_file,
                    )
                    columns = []
                    for field_index, (_, header_definition_id, info_key, is_flag, _) in enumerate(resolved_info_entries, start=1):
                        source_column = safe_column_name(
                            header_definition_id if header_definition_id else field_index,
                            info_key + ("_flag" if is_flag else ""),
                        )
                        header_def = next(
                            (item for item in header_definitions if item.header_definition_id == header_definition_id),
                            None,
                        )
                        number = header_def.number if header_def else ("0" if is_flag else ".")
                        value_type = header_def.value_type if header_def else ("Flag" if is_flag else "String")
                        columns.append((header_definition_id, info_key, source_column, is_flag))
                        info_signature_fields_rows.append(
                            [vcf_file_id, new_id, field_index, header_definition_id, info_key, source_column, number, value_type, 1 if is_flag else 0]
                        )
                    info_signature_columns[new_id] = columns
                info_signature_id = signatures_by_info[info_signature_key].info_signature_id

            format_signature_id = MISSING_NUMERIC_ID
            if format_signature_key:
                if format_signature_key not in signatures_by_format:
                    new_id = len(signatures_by_format) + 1
                    genotype_file = f"genotype_sig_{new_id}.tsv"
                    parquet_file = f"genotype_sig_{new_id}.parquet"
                    signatures_by_format[format_signature_key] = FormatSignature(
                        format_signature_id=new_id,
                        format_string=format_text,
                        genotype_file=genotype_file,
                        genotype_parquet_file=parquet_file,
                    )
                    columns = []
                    for field_index, (header_definition_id, field_name) in enumerate(resolved_format_fields, start=1):
                        source_column = safe_column_name(
                            header_definition_id if header_definition_id else field_index,
                            field_name,
                        )
                        header_def = next(
                            (item for item in header_definitions if item.header_definition_id == header_definition_id),
                            None,
                        )
                        number = header_def.number if header_def else "."
                        value_type = header_def.value_type if header_def else "String"
                        columns.append((header_definition_id, field_name, source_column))
                        format_signature_fields_rows.append(
                            [vcf_file_id, new_id, field_index, header_definition_id, field_name, source_column, number, value_type]
                        )
                    format_signature_columns[new_id] = columns
                format_signature_id = signatures_by_format[format_signature_key].format_signature_id

            records_rows.append(
                [
                    vcf_file_id,
                    record_id,
                    chrom,
                    pos,
                    record_name,
                    ref,
                    qual,
                    alt_state,
                    filter_state,
                    info_signature_id,
                    format_signature_id,
                ]
            )

            for alt_index, alt_value in enumerate(alt_values, start=1):
                record_alt_rows.append([vcf_file_id, record_id, alt_index, alt_value])

            for filter_index, filter_value in enumerate(filter_values, start=1):
                record_filter_rows.append(
                    [vcf_file_id, record_id, filter_index, filter_value, filter_map.get(filter_value, MISSING_NUMERIC_ID)]
                )

            if info_signature_id:
                info_row: list[object] = [vcf_file_id, record_id]
                for _, _, source_column, is_flag in info_signature_columns[info_signature_id]:
                    match = next(item for item in resolved_info_entries if safe_column_name(item[1] if item[1] else item[0], item[2] + ("_flag" if item[3] else "")) == source_column)
                    info_row.append("1" if is_flag else ",".join(match[4]))
                info_rows_by_signature[info_signature_id].append(info_row)

            if format_signature_id:
                source_columns = [item[2] for item in format_signature_columns[format_signature_id]]
                format_field_names = [item[1] for item in format_signature_columns[format_signature_id]]
                for sample_index, sample_value in enumerate(sample_values, start=1):
                    genotype_row: list[object] = [vcf_file_id, record_id, sample_index]
                    genotype_row.extend(genotype_tokens(sample_value, format_field_names))
                    genotype_rows_by_signature[format_signature_id].append(genotype_row)

    files_rows = [[vcf_file_id, input_path.name, str(input_path.resolve()), 1 if input_path.suffix == ".gz" else 0, header_count, len(sample_names), record_count]]
    write_tsv(package_dir / "files.tsv", ["vcf_file_id", "source_name", "source_path", "is_gzipped", "header_count", "sample_count", "record_count"], files_rows)
    write_tsv(package_dir / "header_lines.tsv", ["vcf_file_id", "header_index", "line_type", "raw_line"], header_lines)
    write_tsv(
        package_dir / "header_definitions.tsv",
        ["vcf_file_id", "header_definition_id", "header_key", "header_category", "structured_id", "number", "value_type", "description", "value", "extra_attributes_json", "raw_line"],
        [[vcf_file_id, item.header_definition_id, item.header_key, item.header_category, item.structured_id, item.number, item.value_type, item.description, item.value, item.extra_attributes_json, item.raw_line] for item in header_definitions],
    )
    write_tsv(package_dir / "samples.tsv", ["vcf_file_id", "sample_id", "sample_name"], [[vcf_file_id, item.sample_id, item.sample_name] for item in samples])
    write_tsv(
        package_dir / "records.tsv",
        ["vcf_file_id", "record_id", "chrom", "pos", "record_name", "ref", "qual", "alt_state", "filter_state", "info_signature_id", "format_signature_id"],
        records_rows,
    )
    write_tsv(package_dir / "record_alt.tsv", ["vcf_file_id", "record_id", "alt_index", "alt_value"], record_alt_rows)
    write_tsv(package_dir / "record_filter.tsv", ["vcf_file_id", "record_id", "filter_index", "filter_value", "filter_header_definition_id"], record_filter_rows)

    format_signature_rows = [
        [vcf_file_id, sig.format_signature_id, sig.format_string, len(format_signature_columns[sig.format_signature_id]), sig.genotype_file, str(parquet_dir / sig.genotype_parquet_file)]
        for sig in sorted(signatures_by_format.values(), key=lambda item: item.format_signature_id)
    ]
    info_signature_rows = [
        [vcf_file_id, sig.info_signature_id, sig.info_string, len(info_signature_columns[sig.info_signature_id]), sig.info_file, str(parquet_dir / sig.info_parquet_file)]
        for sig in sorted(signatures_by_info.values(), key=lambda item: item.info_signature_id)
    ]
    write_tsv(package_dir / "format_signatures.tsv", ["vcf_file_id", "format_signature_id", "format_string", "field_count", "genotype_file", "genotype_parquet_path"], format_signature_rows)
    write_tsv(package_dir / "format_signature_fields.tsv", ["vcf_file_id", "format_signature_id", "field_index", "format_header_definition_id", "field_name", "source_column", "number", "value_type"], format_signature_fields_rows)
    write_tsv(package_dir / "info_signatures.tsv", ["vcf_file_id", "info_signature_id", "info_string", "field_count", "info_file", "info_parquet_path"], info_signature_rows)
    write_tsv(package_dir / "info_signature_fields.tsv", ["vcf_file_id", "info_signature_id", "field_index", "info_header_definition_id", "field_name", "source_column", "number", "value_type", "is_flag"], info_signature_fields_rows)

    table_counts: dict[str, int] = {
        "files.tsv": len(files_rows),
        "header_lines.tsv": len(header_lines),
        "header_definitions.tsv": len(header_definitions),
        "samples.tsv": len(samples),
        "records.tsv": len(records_rows),
        "record_alt.tsv": len(record_alt_rows),
        "record_filter.tsv": len(record_filter_rows),
        "format_signatures.tsv": len(format_signature_rows),
        "format_signature_fields.tsv": len(format_signature_fields_rows),
        "info_signatures.tsv": len(info_signature_rows),
        "info_signature_fields.tsv": len(info_signature_fields_rows),
    }

    emit_typed_parquet(
        package_dir / "records.tsv",
        parquet_dir / "records.parquet",
        [
            ("vcf_file_id", "BIGINT", False),
            ("record_id", "BIGINT", False),
            ("chrom", "VARCHAR", False),
            ("pos", "BIGINT", False),
            ("record_name", "VARCHAR", False),
            ("ref", "VARCHAR", False),
            ("qual", "DOUBLE", False),
            ("alt_state", "VARCHAR", False),
            ("filter_state", "VARCHAR", False),
            ("info_signature_id", "BIGINT", False),
            ("format_signature_id", "BIGINT", False),
        ],
    )
    emit_typed_parquet(
        package_dir / "record_alt.tsv",
        parquet_dir / "record_alt.parquet",
        [
            ("vcf_file_id", "BIGINT", False),
            ("record_id", "BIGINT", False),
            ("alt_index", "BIGINT", False),
            ("alt_value", "VARCHAR", False),
        ],
    )

    genotype_tables = []
    for signature in sorted(signatures_by_format.values(), key=lambda item: item.format_signature_id):
        columns = format_signature_columns[signature.format_signature_id]
        header = ["vcf_file_id", "record_id", "sample_id"] + [item[2] for item in columns]
        tsv_path = package_dir / signature.genotype_file
        write_tsv(tsv_path, header, genotype_rows_by_signature[signature.format_signature_id])
        typed_columns = [("vcf_file_id", "BIGINT", False), ("record_id", "BIGINT", False), ("sample_id", "BIGINT", False)]
        for header_definition_id, field_name, source_column in columns:
            header_def = next((item for item in header_definitions if item.header_definition_id == header_definition_id), None)
            number = header_def.number if header_def else "."
            value_type = header_def.value_type if header_def else "String"
            duckdb_type, is_list = infer_duckdb_type(number, value_type, False)
            typed_columns.append((source_column, duckdb_type, is_list))
        parquet_path = parquet_dir / signature.genotype_parquet_file
        emit_typed_parquet(tsv_path, parquet_path, typed_columns)
        table_counts[signature.genotype_file] = len(genotype_rows_by_signature[signature.format_signature_id])
        genotype_tables.append({"format_signature_id": signature.format_signature_id, "tsv_file": signature.genotype_file, "parquet_file": str(parquet_path), "row_count": len(genotype_rows_by_signature[signature.format_signature_id])})

    info_tables = []
    for signature in sorted(signatures_by_info.values(), key=lambda item: item.info_signature_id):
        columns = info_signature_columns[signature.info_signature_id]
        header = ["vcf_file_id", "record_id"] + [item[2] for item in columns]
        tsv_path = package_dir / signature.info_file
        write_tsv(tsv_path, header, info_rows_by_signature[signature.info_signature_id])
        typed_columns = [("vcf_file_id", "BIGINT", False), ("record_id", "BIGINT", False)]
        for header_definition_id, field_name, source_column, is_flag in columns:
            header_def = next((item for item in header_definitions if item.header_definition_id == header_definition_id), None)
            number = header_def.number if header_def else ("0" if is_flag else ".")
            value_type = header_def.value_type if header_def else ("Flag" if is_flag else "String")
            duckdb_type, is_list = infer_duckdb_type(number, value_type, is_flag)
            typed_columns.append((source_column, duckdb_type, is_list))
        parquet_path = parquet_dir / signature.info_parquet_file
        emit_typed_parquet(tsv_path, parquet_path, typed_columns)
        table_counts[signature.info_file] = len(info_rows_by_signature[signature.info_signature_id])
        info_tables.append({"info_signature_id": signature.info_signature_id, "tsv_file": signature.info_file, "parquet_file": str(parquet_path), "row_count": len(info_rows_by_signature[signature.info_signature_id])})

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "vcf_file_id": vcf_file_id,
        "source_name": input_path.name,
        "package_dir": str(package_dir.resolve()),
        "parquet_dir": str(parquet_dir.resolve()),
        "table_counts": table_counts,
        "genotype_tables": genotype_tables,
        "info_tables": info_tables,
    }
    (package_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return package_dir


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def reconstruct_vcf(package_dir: Path, output_path: Path) -> Path:
    header_lines = read_tsv(package_dir / "header_lines.tsv")
    samples = sorted(read_tsv(package_dir / "samples.tsv"), key=lambda row: int(row["sample_id"]))
    records = sorted(read_tsv(package_dir / "records.tsv"), key=lambda row: int(row["record_id"]))
    alt_rows = sorted(read_tsv(package_dir / "record_alt.tsv"), key=lambda row: (int(row["record_id"]), int(row["alt_index"])))
    filter_rows = sorted(read_tsv(package_dir / "record_filter.tsv"), key=lambda row: (int(row["record_id"]), int(row["filter_index"])))
    format_signature_fields = read_tsv(package_dir / "format_signature_fields.tsv")
    info_signature_fields = read_tsv(package_dir / "info_signature_fields.tsv")
    format_signatures = {int(row["format_signature_id"]): row for row in read_tsv(package_dir / "format_signatures.tsv")}
    info_signatures = {int(row["info_signature_id"]): row for row in read_tsv(package_dir / "info_signatures.tsv")}

    alts_by_record: dict[int, list[str]] = defaultdict(list)
    for row in alt_rows:
        alts_by_record[int(row["record_id"])].append(row["alt_value"])

    filters_by_record: dict[int, list[str]] = defaultdict(list)
    for row in filter_rows:
        filters_by_record[int(row["record_id"])].append(row["filter_value"])

    genotype_rows_by_signature: dict[int, dict[tuple[int, int], dict[str, str]]] = {}
    for signature_id, row in format_signatures.items():
        genotype_rows = read_tsv(package_dir / row["genotype_file"])
        genotype_rows_by_signature[signature_id] = {(int(item["record_id"]), int(item["sample_id"])): item for item in genotype_rows}

    info_rows_by_signature: dict[int, dict[int, dict[str, str]]] = {}
    for signature_id, row in info_signatures.items():
        info_rows = read_tsv(package_dir / row["info_file"])
        info_rows_by_signature[signature_id] = {int(item["record_id"]): item for item in info_rows}

    format_fields_by_signature: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in sorted(format_signature_fields, key=lambda item: (int(item["format_signature_id"]), int(item["field_index"]))):
        format_fields_by_signature[int(row["format_signature_id"])].append(row)

    info_fields_by_signature: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in sorted(info_signature_fields, key=lambda item: (int(item["info_signature_id"]), int(item["field_index"]))):
        info_fields_by_signature[int(row["info_signature_id"])].append(row)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        for row in sorted(header_lines, key=lambda item: int(item["header_index"])):
            handle.write(row["raw_line"] + "\n")

        for record in records:
            record_id = int(record["record_id"])
            alt = "." if record["alt_state"] != "VALUES" else ",".join(alts_by_record[record_id])
            if record["filter_state"] == "PASS":
                filter_text = "PASS"
            elif record["filter_state"] == "VALUES":
                filter_text = ";".join(filters_by_record[record_id])
            else:
                filter_text = "."

            info_signature_id = int(record["info_signature_id"])
            info_text = "."
            if info_signature_id:
                info_row = info_rows_by_signature[info_signature_id][record_id]
                tokens = []
                for field in info_fields_by_signature[info_signature_id]:
                    source_column = field["source_column"]
                    if field["is_flag"] == "1":
                        if info_row[source_column] == "1":
                            tokens.append(field["field_name"])
                    else:
                        tokens.append(f"{field['field_name']}={info_row[source_column]}")
                info_text = ";".join(tokens) if tokens else "."

            format_signature_id = int(record["format_signature_id"])
            format_text = "."
            sample_texts: list[str] = []
            if format_signature_id:
                fields = format_fields_by_signature[format_signature_id]
                format_text = ":".join(field["field_name"] for field in fields)
                for sample in samples:
                    genotype_row = genotype_rows_by_signature[format_signature_id][(record_id, int(sample["sample_id"]))]
                    sample_texts.append(":".join(genotype_row[field["source_column"]] for field in fields))

            row_values = [
                record["chrom"],
                record["pos"],
                record["record_name"],
                record["ref"],
                alt,
                record["qual"],
                filter_text,
                info_text,
                format_text,
            ]
            row_values.extend(sample_texts)
            handle.write("\t".join(row_values) + "\n")
    return output_path


def canonicalize_vcf(path: Path) -> dict[str, object]:
    header_lines: list[str] = []
    samples: list[str] = []
    records: list[dict[str, object]] = []
    with open_text(path) as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n").rstrip("\r")
            if line.startswith("#"):
                header_lines.append(line)
                if line.startswith("#CHROM\t"):
                    samples = line.split("\t")[9:]
                continue

            fields = normalize_record(line.split("\t"), len(samples))
            info_entries, info_raw = parse_info_entries(fields[7])
            format_text = fields[8]
            format_fields = [] if format_text in {"", "."} else format_text.split(":")
            sample_entries = [genotype_tokens(value, format_fields) for value in fields[9:]]
            records.append(
                {
                    "chrom": fields[0],
                    "pos": fields[1],
                    "record_name": fields[2],
                    "ref": fields[3],
                    "alt": fields[4],
                    "qual": fields[5],
                    "filter": fields[6],
                    "info": info_raw if info_raw else ".",
                    "format": format_text,
                    "samples": sample_entries,
                    "info_key_count": len(info_entries),
                }
            )
    return {"headers": header_lines, "samples": samples, "records": records}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Canonical VCF TSV/Parquet packager")
    subparsers = parser.add_subparsers(dest="command", required=True)
    split_parser = subparsers.add_parser("split", help="Split a VCF into canonical TSV tables and typed Parquet")
    split_parser.add_argument("input_path", type=Path)
    split_parser.add_argument("output_dir", type=Path)
    reconstruct_parser = subparsers.add_parser("reconstruct", help="Rebuild a VCF from a canonical TSV package")
    reconstruct_parser.add_argument("package_dir", type=Path)
    reconstruct_parser.add_argument("output_path", type=Path)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "split":
        split_vcf_to_package(args.input_path, args.output_dir)
        return 0
    if args.command == "reconstruct":
        reconstruct_vcf(args.package_dir, args.output_path)
        return 0
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
