"""
src/clean.py
------------
Stage 1 – Limpieza y transformacion.
Lee los Parquet raw (uno por anio) y aplica reglas de limpieza
identificadas en el EDA. Guarda el resultado en data/processed/.

Autor: Equipo Chicago Taxi Lakehouse – ITY1101 DuocUC 2025
"""

import logging
import os
from glob import glob

import pandas as pd

log = logging.getLogger(__name__)

BASE      = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE, "data", "raw")
PROC_DIR  = os.path.join(BASE, "data", "processed")


def _clean_df(df: pd.DataFrame, year: int) -> pd.DataFrame:
    # Renombrar columnas si vienen directo del Parquet original (sin pasar por ingest)
    rename_map = {}
    if "unique_key" in df.columns and "trip_id" not in df.columns:
        rename_map["unique_key"] = "trip_id"
    if "pickup_latitude" in df.columns:
        rename_map["pickup_latitude"]  = "pickup_centroid_latitude"
        rename_map["pickup_longitude"] = "pickup_centroid_longitude"
    if "dropoff_latitude" in df.columns:
        rename_map["dropoff_latitude"]  = "dropoff_centroid_latitude"
        rename_map["dropoff_longitude"] = "dropoff_centroid_longitude"
    if rename_map:
        df = df.rename(columns=rename_map)

    initial = len(df)
    log.info(f"  [{year}] Registros iniciales: {initial:,}")

    # 1. Deduplicacion por trip_id
    df = df.drop_duplicates(subset=["trip_id"])
    log.info(f"  [{year}] Tras dedup: {len(df):,} ({initial - len(df):,} eliminados)")

    # 2. Eliminar viajes con duracion = 0
    before = len(df)
    df = df[df["trip_seconds"] > 0]
    log.info(f"  [{year}] Tras filtrar trip_seconds=0: {len(df):,} ({before - len(df):,} eliminados)")

    # 3. Marcar anomalias (distancia=0 con costo>0)
    df = df.copy()
    df["anomalia"] = (df["trip_miles"] == 0) & (df["trip_total"] > 0)
    log.info(f"  [{year}] Anomalias marcadas: {df['anomalia'].sum():,}")

    # 4. Normalizar timestamps
    for col in ["trip_start_timestamp", "trip_end_timestamp"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # 5. Normalizar company
    df["company"] = df["company"].fillna("UNKNOWN").str.strip().str.upper()

    # 6. Imputar payment_type nulo
    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].fillna("UNKNOWN")

    # 7. Columnas temporales derivadas
    ts = df["trip_start_timestamp"]
    df["trip_year"]        = ts.dt.year
    df["trip_month"]       = ts.dt.month
    df["trip_hour"]        = ts.dt.hour
    df["trip_day_of_week"] = ts.dt.dayofweek  # 0=lunes

    log.info(f"  [{year}] Registros finales: {len(df):,}")
    return df


def clean() -> None:
    files = sorted(glob(os.path.join(RAW_DIR, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No hay Parquet en {RAW_DIR}. Ejecuta primero la etapa ingest.")

    os.makedirs(PROC_DIR, exist_ok=True)

    for filepath in files:
        fname = os.path.basename(filepath)
        year  = int("".join(filter(str.isdigit, fname))[:4])
        log.info(f"Limpiando: {fname}")
        df       = pd.read_parquet(filepath)
        df_clean = _clean_df(df, year)
        out      = os.path.join(PROC_DIR, f"taxi_trips_{year}_clean.parquet")
        df_clean.to_parquet(out, index=False, compression="snappy")
        log.info(f"  Guardado: {out}")
