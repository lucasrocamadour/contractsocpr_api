from __future__ import annotations

import json
from pathlib import Path

import duckdb

# Input, output, and fix specification locations
INPUT_DIR = Path("download/2c_contracts_merged_names")
OUTPUT_DIR = Path("download/3b_contracts_fixed")
FIX_FILE = Path("fix.json")


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main_fix(year) -> None:
    input_path = INPUT_DIR / f"contracts_{year}_names_merged.parquet"
    output_path = OUTPUT_DIR / f"contracts_{year}_fixed.parquet"

    con = duckdb.connect()
    try:
        con.execute(f"CREATE TEMP TABLE t AS SELECT * FROM read_parquet('{input_path.as_posix()}')")

        with FIX_FILE.open("r", encoding="utf-8") as handle:
            spec = json.load(handle)

        fix_rules = spec.get("Fix", [])
        drop_rules = spec.get("Drop", [])
        if isinstance(fix_rules, dict):
            fix_rules = [fix_rules]
        if isinstance(drop_rules, dict):
            drop_rules = [drop_rules]

        for rule in drop_rules:
            if not rule:
                continue
            where_cols = list(rule.keys())
            where_vals = [rule[col] for col in where_cols]
            where_sql = " AND ".join([f"{_quote_identifier(col)} = ?" for col in where_cols])
            con.execute(f"DELETE FROM t WHERE {where_sql}", where_vals)

        for rule in fix_rules:
            if not rule:
                continue
            assignments = {key[:-4]: value for key, value in rule.items() if key.endswith("_new")}
            conditions = {key: value for key, value in rule.items() if not key.endswith("_new")}
            if not assignments:
                continue
            where_cols = list(conditions.keys())
            where_vals = [conditions[col] for col in where_cols]
            where_sql = (
                " AND ".join([f"{_quote_identifier(col)} = ?" for col in where_cols]) if where_cols else "TRUE"
            )
            for column, new_value in assignments.items():
                con.execute(
                    f"UPDATE t SET {_quote_identifier(column)} = ? WHERE {where_sql}",
                    [new_value, *where_vals],
                )

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY t TO '{output_path.as_posix()}' (FORMAT PARQUET)")
        print(f"\nWrote {output_path}")
    finally:
        con.close()
