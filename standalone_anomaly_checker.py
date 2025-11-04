#!/usr/bin/env python3
# Robust per-contractor anomaly detector using Median / MAD z-scores (DuckDB only, no CLI args)

import os

import duckdb

# ====================== USER CONFIG ======================
PARQUET_GLOB = "download/2c_contracts_merged_names/contracts_*.parquet"
CONTRACTOR_COL = "normalized_Contractors"
AMOUNT_COL = "AmountToPay"
CONTRACT_ID_COL = "ContractId"

MIN_OBS_PER_VENDOR = 3  # require at least this many rows for stable stats
MIN_ANOMALY_AMOUNT = 10_000  # minimum amount threshold to include in stats or flag anomaly
Z_SCORE_THRESHOLD = 3.5  # mark anomalies above this robust z-score
MAD_SCALE_FACTOR = 1.4826  # consistent scaling factor so MAD approximates std dev

OUT_DIR = "download/3a_contracts_anomalies"
OUT_ANOMALIES = f"{OUT_DIR}/contractor_amount_anomalies.parquet"
OUT_STATS = f"{OUT_DIR}/contractor_amount_stats.parquet"
# =========================================================
def main_anomaly():
  os.makedirs(OUT_DIR, exist_ok=True)
  con = duckdb.connect()

  source = f"read_parquet('{PARQUET_GLOB}')"

  sql_stats = f"""
  WITH src AS (
    SELECT
      CAST("{AMOUNT_COL}" AS DOUBLE) AS amount,
      CAST("{CONTRACTOR_COL}" AS VARCHAR) AS contractor
    FROM {source}
    WHERE "{AMOUNT_COL}" IS NOT NULL
      AND "{CONTRACTOR_COL}" IS NOT NULL
      AND CAST("{AMOUNT_COL}" AS DOUBLE) >= {float(MIN_ANOMALY_AMOUNT)}
  ),
  medians AS (
    SELECT
      contractor,
      MEDIAN(amount) AS median_amount,
      COUNT(*) AS n_obs
    FROM src
    GROUP BY contractor
  ),
  deviations AS (
    SELECT
      s.contractor,
      ABS(s.amount - m.median_amount) AS abs_dev
    FROM src s
    JOIN medians m USING (contractor)
  ),
  mad AS (
    SELECT
      contractor,
      MEDIAN(abs_dev) AS mad_value
    FROM deviations
    GROUP BY contractor
  )
  SELECT
    m.contractor,
    m.n_obs,
    m.median_amount,
    mad.mad_value AS mad,
    {MAD_SCALE_FACTOR} * mad.mad_value AS mad_scaled
  FROM medians m
  JOIN mad USING (contractor)
  WHERE m.n_obs >= {int(MIN_OBS_PER_VENDOR)}
  """

  sql_anoms = f"""
  WITH src AS (
    SELECT
      CAST("{AMOUNT_COL}" AS DOUBLE) AS amount,
      CAST("{CONTRACTOR_COL}" AS VARCHAR) AS contractor,
      CAST("{CONTRACT_ID_COL}" AS VARCHAR) AS contract_id
    FROM {source}
    WHERE "{AMOUNT_COL}" IS NOT NULL
      AND "{CONTRACTOR_COL}" IS NOT NULL
      AND CAST("{AMOUNT_COL}" AS DOUBLE) >= {float(MIN_ANOMALY_AMOUNT)}
  ),
  medians AS (
    SELECT
      contractor,
      MEDIAN(amount) AS median_amount,
      COUNT(*) AS n_obs
    FROM src
    GROUP BY contractor
  ),
  deviations AS (
    SELECT
      s.contractor,
      s.contract_id,
      s.amount,
      m.n_obs,
      m.median_amount,
      ABS(s.amount - m.median_amount) AS abs_dev
    FROM src s
    JOIN medians m USING (contractor)
  ),
  mad AS (
    SELECT
      contractor,
      MEDIAN(abs_dev) AS mad_value
    FROM deviations
    GROUP BY contractor
  ),
  scored AS (
    SELECT
      d.contractor,
      d.contract_id,
      d.amount,
      d.n_obs,
      d.median_amount,
      mad.mad_value AS mad,
      {MAD_SCALE_FACTOR} * mad.mad_value AS mad_scaled,
      d.abs_dev,
      CASE
        WHEN mad.mad_value <= 0 THEN NULL
        ELSE d.abs_dev / ({MAD_SCALE_FACTOR} * mad.mad_value)
      END AS robust_z
    FROM deviations d
    JOIN mad USING (contractor)
  )
  SELECT
    contractor,
    contract_id,
    amount,
    median_amount,
    mad,
    mad_scaled,
    abs_dev,
    robust_z,
    n_obs
  FROM scored
  WHERE n_obs >= {int(MIN_OBS_PER_VENDOR)}
    AND robust_z IS NOT NULL
    AND robust_z > {float(Z_SCORE_THRESHOLD)}
    AND amount >= {float(MIN_ANOMALY_AMOUNT)}
  ORDER BY robust_z DESC, amount DESC
  """

  # Write stats and anomalies
  con.execute(f"COPY ({sql_stats}) TO '{OUT_STATS}' (FORMAT PARQUET)")
  con.execute(f"COPY ({sql_anoms}) TO '{OUT_ANOMALIES}' (FORMAT PARQUET)")

  # Quick verification prints
  n_all = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
  n_stats = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT_STATS}')").fetchone()[0]
  n_anoms = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT_ANOMALIES}')").fetchone()[0]

  print(f"Scanned rows: {n_all:,}")
  print(f"Vendors with stats: {n_stats:,}")
  print(f"Anomalous rows flagged: {n_anoms:,}")
  print(f"Wrote: {OUT_STATS}")
  print(f"Wrote: {OUT_ANOMALIES}")
