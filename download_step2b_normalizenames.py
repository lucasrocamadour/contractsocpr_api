from __future__ import annotations

from pathlib import Path

import duckdb

# Input and output locations
INPUT_DIR = Path("download/2a_contracts_normalized_dates")
OUTPUT_DIR = Path("download/2b_contracts_normalized_names")

# Map of original column -> new normalized column name with normalized_ prefix
COLUMNS_TO_NORMALIZE: dict[str, str] = {
    "EntityName": "normalized_EntityName",
    "Contractors": "normalized_Contractors",
    "Service": "normalized_Service",
    "ServiceGroup": "normalized_ServiceGroup",
}


def normalizenames(year) -> None:
    input_path: Path = INPUT_DIR / f"contracts_{year}_dates_normalized.parquet"
    output_path: Path = OUTPUT_DIR / f"contracts_{year}_dates_normalized_with_norm.parquet"

    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TEMP TABLE data AS SELECT * FROM read_parquet(?)",
            [input_path.as_posix()],
        )
    except Exception:
        con.close()
        raise

    existing_columns = {row[1] for row in con.execute("PRAGMA table_info('data')").fetchall()}
    missing_columns = [col for col in COLUMNS_TO_NORMALIZE if col not in existing_columns]
    if missing_columns:
        con.close()
        raise KeyError(f"Missing expected columns: {', '.join(missing_columns)}")

    select_parts = ["data.*"]
    for src_col, dest_col in COLUMNS_TO_NORMALIZE.items():
        select_parts.append(
            f"regexp_replace(lower(coalesce(CAST({src_col} AS VARCHAR), '')), '[^0-9a-z]+', '') AS {dest_col}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    query = "SELECT " + ", ".join(select_parts) + " FROM data"
    try:
        con.execute(f"COPY ({query}) TO '{output_path.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.close()

    print(f"Wrote {output_path}")
