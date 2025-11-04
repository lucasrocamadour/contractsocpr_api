#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import duckdb

# Input and output locations
INPUT_DIR = Path("download/2b_contracts_normalized_names")
OUTPUT_DIR = Path("download/2c_contracts_merged_names")

# Columns we expect to have both raw and normalized counterparts
COLUMN_PAIRS: tuple[tuple[str, str], ...] = (
    ("EntityName", "normalized_EntityName"),
    ("Contractors", "normalized_Contractors"),
    ("Service", "normalized_Service"),
    ("ServiceGroup", "normalized_ServiceGroup"),
)


def _sanitize_temp_name(column_name: str) -> str:
    return "canonical_" + "".join(ch.lower() if ch.isalnum() else "_" for ch in column_name)


def analyze_column(con: duckdb.DuckDBPyConnection, raw_col: str, norm_col: str) -> None:
    existing_columns = {row[1] for row in con.execute("PRAGMA table_info('contracts')").fetchall()}
    if raw_col not in existing_columns or norm_col not in existing_columns:
        print(f"[WARN] Skipping column pair ({raw_col}, {norm_col}) – missing from table.")
        return

    stats_row = con.execute(
        f"""
        WITH counts AS (
            SELECT {norm_col} AS norm_value,
                   {raw_col} AS raw_value,
                   COUNT(*) AS freq
            FROM contracts
            WHERE {norm_col} IS NOT NULL AND {norm_col} <> ''
            GROUP BY 1, 2
        ),
        per_norm AS (
            SELECT norm_value,
                   SUM(freq) AS total_rows,
                   COUNT(*) AS variants
            FROM counts
            GROUP BY 1
        )
        SELECT
            COALESCE(COUNT(*), 0) AS total_norm_groups,
            COALESCE(SUM(CASE WHEN variants > 1 THEN 1 ELSE 0 END), 0) AS multi_form_groups
        FROM per_norm
        """
    ).fetchone()

    total_norm_groups = stats_row[0] or 0
    multi_form_groups = stats_row[1] or 0

    temp_table = _sanitize_temp_name(norm_col)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        WITH counts AS (
            SELECT {norm_col} AS norm_value,
                   {raw_col} AS raw_value,
                   COUNT(*) AS freq
            FROM contracts
            WHERE {norm_col} IS NOT NULL AND {norm_col} <> ''
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY norm_value ORDER BY freq DESC, raw_value) AS rn
            FROM counts
        )
        SELECT norm_value, raw_value
        FROM ranked
        WHERE rn = 1
        """
    )

    updates = con.execute(
        f"""
        SELECT COUNT(*)
        FROM contracts AS c
        JOIN {temp_table} AS canon
          ON c.{norm_col} = canon.norm_value
        WHERE c.{raw_col} IS DISTINCT FROM canon.raw_value
        """
    ).fetchone()[0]

    if updates:
        con.execute(
            f"""
            UPDATE contracts AS c
            SET {raw_col} = canon.raw_value
            FROM {temp_table} AS canon
            WHERE c.{norm_col} = canon.norm_value
              AND c.{raw_col} IS DISTINCT FROM canon.raw_value
            """
        )

    print(f"\n=== Column: {raw_col} ===")
    print(f"Normalized groups: {total_norm_groups:,}")
    print(f"Groups with multiple raw forms: {multi_form_groups:,}")
    print(f"Rows updated: {updates:,}")

    if multi_form_groups:
        canonical_map = {
            row[0]: row[1]
            for row in con.execute(f"SELECT norm_value, raw_value FROM {temp_table}").fetchall()
        }
        conflict_rows = con.execute(
            f"""
            WITH counts AS (
                SELECT {norm_col} AS norm_value,
                       {raw_col} AS raw_value,
                       COUNT(*) AS freq
                FROM contracts
                WHERE {norm_col} IS NOT NULL AND {norm_col} <> ''
                GROUP BY 1, 2
            ),
            per_norm AS (
                SELECT norm_value,
                       SUM(freq) AS total_rows,
                       COUNT(*) AS variants
                FROM counts
                GROUP BY 1
                HAVING COUNT(*) > 1
            ),
            ranked_groups AS (
                SELECT norm_value,
                       total_rows,
                       variants,
                       ROW_NUMBER() OVER (ORDER BY total_rows DESC, norm_value) AS group_rank
                FROM per_norm
            ),
            ranked_forms AS (
                SELECT c.norm_value,
                       c.raw_value,
                       c.freq,
                       g.total_rows,
                       g.variants,
                       ROW_NUMBER() OVER (PARTITION BY c.norm_value ORDER BY c.freq DESC, c.raw_value) AS form_rank,
                       g.group_rank
                FROM counts c
                JOIN ranked_groups g USING (norm_value)
                WHERE g.group_rank <= 5
            )
            SELECT norm_value, raw_value, freq, total_rows, variants, form_rank, group_rank
            FROM ranked_forms
            WHERE form_rank <= 5
            ORDER BY group_rank, form_rank
            """
        ).fetchall()

        print("Top conflicting normalized values:")
        last_group_rank = None
        for norm_value, raw_value, freq, total_rows, variants, form_rank, group_rank in conflict_rows:
            if group_rank != last_group_rank:
                print(f"  {norm_value!r} -> total {total_rows:,} rows, {variants} variants")
                last_group_rank = group_rank
            suffix = " (canonical)" if canonical_map.get(norm_value) == raw_value else ""
            print(f"    {raw_value!r}: {freq:,}{suffix}")
            if form_rank == 5 and variants > 5:
                print("    ...")


def merge_names(year: int) -> None:
    input_path = INPUT_DIR / f"contracts_{year}_dates_normalized_with_norm.parquet"
    output_path = OUTPUT_DIR / f"contracts_{year}_names_merged.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_path}")

    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TEMP TABLE contracts AS SELECT * FROM read_parquet(?)",
            [str(input_path)],
        )
    except Exception:
        con.close()
        raise

    total_rows = con.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
    print(f"Loaded {total_rows:,} rows from {input_path}")

    for raw_col, norm_col in COLUMN_PAIRS:
        analyze_column(con, raw_col, norm_col)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        con.execute(f"COPY (SELECT * FROM contracts) TO '{output_path.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.close()
    print(f"\nWrote merged names to {output_path}")


if __name__ == "__main__":
    merge_names(2025)
