from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, List

import duckdb

# Input and output locations
INPUT_DIR = Path("download/1a_contracts_raw")
OUTPUT_DIR = Path("download/2a_contracts_normalized_dates")

# Fields targeted for normalization
DATE_FIELDS: List[str] = [
    "DateOfGrant",
    "EffectiveDateFrom",
    "EffectiveDateTo",
    "CancellationDate",
]
CONTRACTORS_FIELD = "Contractors"


def normalize_date(val: Any) -> Any:
    """
    Normalize a single date-like value to an ISO8601 string.
    Rules:
      - If val is None -> return as-is
      - If val is a string starting with '/Date(' -> extract integer, detect ms vs s, convert to ISO
      - If val is an int/float -> treat as epoch (ms if > 1e12 else s), convert to ISO
      - Else if val is a parseable string -> try python-dateutil, otherwise return original
      - Otherwise return the original value unchanged
    """
    if val is None:
        return val

    # 1) ASP.NET /Date(1234567890)/ pattern
    try:
        if isinstance(val, str) and val.startswith("/Date(") and val.endswith(")/"):
            ts_raw = val[len("/Date("):-len(")/")]
            match = re.match(r"^-?\d+", ts_raw)
            if not match:
                return val
            ts_int = int(match.group(0))
            ts_secs = ts_int / 1000.0 if abs(ts_int) > 1_000_000_000_000 else float(ts_int)
            from datetime import datetime, timezone

            return datetime.fromtimestamp(ts_secs, tz=timezone.utc).replace(tzinfo=None).isoformat()
    except Exception:
        return val

    # 2) Numeric epoch (seconds or milliseconds)
    if isinstance(val, (int, float)):
        try:
            ts_num = int(val)
            ts_secs = ts_num / 1000.0 if abs(ts_num) > 1_000_000_000_000 else ts_num
            from datetime import datetime, timezone

            return datetime.fromtimestamp(ts_secs, tz=timezone.utc).replace(tzinfo=None).isoformat()
        except Exception:
            return val

    # 3) Parseable string -> ISO via dateutil if available
    if isinstance(val, str):
        try:
            from dateutil import parser as dateparser

            dt = dateparser.parse(val)
            return dt.isoformat()
        except Exception:
            return val

    return val


def normalize_contractors(val: Any) -> Any:
    """
    Normalize the Contractors column value:
      - If JSON string representing list of objects -> extract each object's 'Name'
      - If a dict -> extract 'Name'
      - Return a plain semicolon-separated string of names (e.g. "A; B; C")
      - If no names found -> return None
      - If parsing fails -> return the original value unchanged
    """
    if val is None:
        return val

    data = val
    if isinstance(val, str):
        text = val.strip()
        try:
            data = json.loads(text)
        except Exception:
            return val

    names: List[str] = []

    try:
        if isinstance(data, dict):
            name = data.get("Name") or data.get("name")
            if name is not None:
                names.append(str(name))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    name = item.get("Name") or item.get("name")
                    if name is not None:
                        names.append(str(name))
                else:
                    names.append(str(item))
        else:
            return str(data)
    except Exception:
        return val

    if not names:
        return None

    return "; ".join(names)


def normalizedates(year: int | str) -> None:
    year_label = "*" if year == "ALL" else year
    input_path = INPUT_DIR / f"contracts_{year_label}.parquet"
    output_path = OUTPUT_DIR / f"contracts_{year_label}_dates_normalized.parquet"

    print(f"Reading Parquet: {input_path}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.create_function(
            "py_normalize_date",
            normalize_date,
            return_type=duckdb.sqltypes.VARCHAR,
            null_handling="special",
        )
        con.create_function(
            "py_normalize_contractors",
            normalize_contractors,
            return_type=duckdb.sqltypes.VARCHAR,
            null_handling="special",
        )

        con.execute(f"CREATE TEMP TABLE t AS SELECT * FROM read_parquet('{input_path.as_posix()}')")

        total_rows = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        if total_rows == 0:
            print("Input file is empty. Writing an identical empty Parquet.")
            con.execute(f"COPY (SELECT * FROM t) TO '{output_path.as_posix()}' (FORMAT PARQUET)")
            print(f"Wrote: {output_path}")
            return

        def column_exists(name: str) -> bool:
            return bool(
                con.execute(
                    "SELECT 1 FROM information_schema.columns WHERE table_name = 't' AND column_name = ?",
                    [name],
                ).fetchone()
            )

        changes: dict[str, int] = {}

        for col in DATE_FIELDS:
            if not column_exists(col):
                print(f"Field '{col}' not present. Skipping.")
                continue

            changed = con.execute(
                f"""
                SELECT COUNT(*)
                FROM t
                WHERE COALESCE(CAST(py_normalize_date({col}) AS VARCHAR), '__NA__')
                      <> COALESCE(CAST({col} AS VARCHAR), '__NA__')
                """
            ).fetchone()[0]

            con.execute(f"UPDATE t SET {col} = py_normalize_date({col})")
            changes[col] = int(changed)
            print(f"Normalized field '{col}': changed {changed} / {total_rows} rows")

        if column_exists(CONTRACTORS_FIELD):
            changed = con.execute(
                f"""
                SELECT COUNT(*)
                FROM t
                WHERE COALESCE(CAST(py_normalize_contractors({CONTRACTORS_FIELD}) AS VARCHAR), '__NA__')
                      <> COALESCE(CAST({CONTRACTORS_FIELD} AS VARCHAR), '__NA__')
                """
            ).fetchone()[0]

            con.execute(f"UPDATE t SET {CONTRACTORS_FIELD} = py_normalize_contractors({CONTRACTORS_FIELD})")
            changes[CONTRACTORS_FIELD] = int(changed)
            print(f"Normalized field '{CONTRACTORS_FIELD}': changed {changed} / {total_rows} rows")
        else:
            print(f"Field '{CONTRACTORS_FIELD}' not present. Skipping contractors normalization.")

        print(f"Writing normalized Parquet to: {output_path}")
        con.execute(f"COPY (SELECT * FROM t) TO '{output_path.as_posix()}' (FORMAT PARQUET)")
        print("Done. Summary of changes:", json.dumps(changes, indent=2))
    finally:
        con.close()
