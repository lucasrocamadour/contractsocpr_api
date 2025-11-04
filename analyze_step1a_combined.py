#!/usr/bin/env python3
from pathlib import Path

import duckdb

# Input and output locations
PARQUET_GLOB = "download/3b_contracts_fixed/contracts_*.parquet"
OUTPUT_ROOT = Path("analyzed")

# Column identifiers
DATE_COL_NAME = "DateOfGrant"
CANCELLATION_COL_NAME = "CancellationDate"
AMOUNT_COL_NAME = "AmountToPay"
CONTRACT_ID_COL = "ContractId"

def main_combined(DATE_FROM: str, DATE_TO: str, GOVERNOR: str, Metric: str):
    # Display labels
    if Metric == "Contractors":
        display_label = "Contratista"
    elif Metric == "Service":
        display_label = "Servicio"
    elif Metric == "EntityName":
        display_label = "Entidad"
    else:
        raise ValueError("Metric must be one of: Contractors, Service, EntityName")

    # Column labels
    col_all_contract_ids = "ContractIds de Este Grupo"
    col_total_cuantia_all_unique = "All_CuantíaEnContratos"
    col_all_unique_cancelled = "All_UniqueCancelledContracts"
    col_total_unique_contracts = "All_UniqueContracts"

    col_x_total_cuantia = "Cuantía Total"
    share_col1 = "% Cuantía Total"
    col_x_unique_cancelled = "Contratos Cancelados"
    share_col2 = "% Contratos Cancelados"
    cancelled_ids_col = "ContractIds de Todos Cancelados"
    col_x_unique_contracts = "Contratos Unicos"
    share_col3 = f"% Contratos Total"

    query = f"""
WITH src AS (
  SELECT
    CAST("{DATE_COL_NAME}" AS DATE) AS grant_date,
    TRY_CAST("{CANCELLATION_COL_NAME}" AS DATE) AS cancellation_date,
    COALESCE(TRY_CAST("{AMOUNT_COL_NAME}" AS DOUBLE), 0.0) AS amount,
    CAST("{Metric}" AS VARCHAR) AS category_value,
    CAST("{CONTRACT_ID_COL}" AS VARCHAR) AS contract_id
  FROM read_parquet('{PARQUET_GLOB}')
),
filtered AS (
  SELECT *
  FROM src
  WHERE grant_date IS NOT NULL
    AND grant_date >= CAST('{DATE_FROM}' AS DATE)
    AND grant_date <= CAST('{DATE_TO}' AS DATE)
),

-- global unique per contract
unique_per_contract_all AS (
  SELECT
    contract_id,
    SUM(amount) AS amount_per_contract_all,
    MAX(CASE WHEN cancellation_date IS NOT NULL THEN 1 ELSE 0 END) AS is_cancelled_int
  FROM filtered
  WHERE contract_id IS NOT NULL
  GROUP BY contract_id
),
global_unique AS (
  SELECT
    SUM(amount_per_contract_all) AS total_unique_cuantia,
    SUM(is_cancelled_int) AS all_unique_cancelled_contracts,
    COUNT(*) AS total_unique_contracts_count
  FROM unique_per_contract_all
),

-- per-category unique per contract
unique_per_contract AS (
  SELECT
    category_value,
    contract_id,
    SUM(amount) AS amount_per_contract,
    MAX(CASE WHEN cancellation_date IS NOT NULL THEN 1 ELSE 0 END) AS is_cancelled_int
  FROM filtered
  WHERE category_value IS NOT NULL AND contract_id IS NOT NULL
  GROUP BY category_value, contract_id
),
unique_sum AS (
  SELECT
    category_value,
    SUM(amount_per_contract) AS category_unique_sum_amount,
    COUNT(*) AS category_unique_count,
    SUM(is_cancelled_int) AS category_unique_cancelled_count
  FROM unique_per_contract
  GROUP BY category_value
),

cuantia AS (
  SELECT
    category_value,
    SUM(amount) AS cuantia_sum,
    LIST(DISTINCT contract_id) FILTER (WHERE contract_id IS NOT NULL) AS category_all_contract_ids
  FROM filtered
  WHERE category_value IS NOT NULL
  GROUP BY category_value
),

cancelled AS (
  SELECT
    category_value,
    SUM(amount) AS cancelled_total_amount,
    LIST(DISTINCT contract_id) FILTER (WHERE contract_id IS NOT NULL) AS cancelled_contract_ids,
    COUNT(DISTINCT contract_id) AS cancelled_unique_contracts_by_rows
  FROM filtered
  WHERE category_value IS NOT NULL AND cancellation_date IS NOT NULL
  GROUP BY category_value
)

SELECT
  c.category_value AS "{display_label}",

  COALESCE(gu.total_unique_cuantia, 0.0) AS "{col_total_cuantia_all_unique}",
  COALESCE(gu.all_unique_cancelled_contracts, 0) AS "{col_all_unique_cancelled}",
  COALESCE(gu.total_unique_contracts_count, 0) AS "{col_total_unique_contracts}",

  COALESCE(u.category_unique_sum_amount, 0.0) AS "{col_x_total_cuantia}",
  CASE
    WHEN gu.total_unique_cuantia IS NULL OR gu.total_unique_cuantia = 0 THEN 0.0
    ELSE (COALESCE(u.category_unique_sum_amount, 0.0) / gu.total_unique_cuantia) * 100.0
  END AS "{share_col1}",

  COALESCE(u.category_unique_cancelled_count, 0) AS "{col_x_unique_cancelled}",
  CASE
    WHEN gu.all_unique_cancelled_contracts IS NULL OR gu.all_unique_cancelled_contracts = 0 THEN 0.0
    ELSE (COALESCE(u.category_unique_cancelled_count, 0)::DOUBLE / gu.all_unique_cancelled_contracts) * 100.0
  END AS "{share_col2}",

  COALESCE(k.cancelled_contract_ids, []::VARCHAR[]) AS "{cancelled_ids_col}",
  COALESCE(u.category_unique_count, 0) AS "{col_x_unique_contracts}",
  CASE
    WHEN gu.total_unique_contracts_count IS NULL OR gu.total_unique_contracts_count = 0 THEN 0.0
    ELSE (COALESCE(u.category_unique_count, 0)::DOUBLE / gu.total_unique_contracts_count) * 100.0
  END AS "{share_col3}",

  c.category_all_contract_ids AS "{col_all_contract_ids}"
FROM cuantia c
CROSS JOIN global_unique gu
LEFT JOIN unique_sum u ON u.category_value = c.category_value
LEFT JOIN cancelled k ON k.category_value = c.category_value
ORDER BY "{share_col2}" DESC
"""

    output_path = OUTPUT_ROOT / GOVERNOR / f"Combined_by_{Metric}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({query}) TO '{output_path.as_posix()}' (FORMAT 'parquet')")
    print(f"File written to: {output_path}")
    return output_path.as_posix()

# Example:
# if __name__ == "__main__":
#     main_combined("2013-01-02", "2017-01-02", "Padilla", "Contractors")
