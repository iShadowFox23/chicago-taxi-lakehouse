"""
src/ingest.py
-------------
Stage 0 – Ingesta desde BigQuery.
Extrae datos de bigquery-public-data.chicago_taxi_trips.taxi_trips
filtrando por año (2019, 2020, 2021) con pandas y google-cloud-bigquery.
Cada año se guarda como Parquet comprimido en data/raw/.

Requisitos:
  - GOOGLE_APPLICATION_CREDENTIALS apuntando al JSON de la service account.

Autor: Equipo Chicago Taxi Lakehouse – ITY1101 DuocUC 2025
"""

import logging
import os
import time

import pandas as pd
from google.cloud import bigquery

log = logging.getLogger(__name__)

BQ_TABLE = "bigquery-public-data.chicago_taxi_trips.taxi_trips"
YEARS    = [2019, 2020, 2021]
RAW_DIR  = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

COLUMNS = [
    "unique_key", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "trip_miles", "pickup_community_area", "dropoff_community_area",
    "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
]


def _build_query(year: int) -> str:
    cols = ", ".join(COLUMNS)
    return f"""
        SELECT {cols}
        FROM `{BQ_TABLE}`
        WHERE EXTRACT(YEAR FROM trip_start_timestamp) = {year}
    """


def _extract_year(client: bigquery.Client, year: int) -> int:
    out = os.path.join(RAW_DIR, f"taxi_trips_{year}.parquet")

    if os.path.exists(out):
        log.info(f"  [{year}] Archivo ya existe, omitiendo extraccion.")
        return len(pd.read_parquet(out, columns=["trip_id"]))

    log.info(f"  [{year}] Ejecutando query en BigQuery...")
    t0 = time.perf_counter()
    df = client.query(_build_query(year)).to_dataframe(progress_bar_type=None)
    df = df.rename(columns={"unique_key": "trip_id"})

    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_parquet(out, index=False, compression="snappy")
    elapsed = time.perf_counter() - t0
    size_mb = os.path.getsize(out) / 1024 ** 2
    log.info(f"  [{year}] {len(df):,} filas — {elapsed:.1f}s — {size_mb:.1f} MB → {out}")
    return len(df)


def ingest() -> None:
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds or not os.path.exists(creds):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS no configurado.")

    client = bigquery.Client()
    total = sum(_extract_year(client, y) for y in YEARS)
    log.info(f"Ingesta completada. Total: {total:,} filas.")
