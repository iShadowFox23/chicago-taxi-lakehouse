"""
src/load.py
-----------
Stage 3 – Carga en PostgreSQL.
Usa COPY (via execute_values en lotes) para carga rapida,
con una pasada previa de deduplicacion contra la BD.

Salidas:
  data/validated/taxi_trips_inserted.parquet
  data/validated/taxi_trips_db_rejected.parquet
  logs/load.log

Autor: Equipo Chicago Taxi Lakehouse – ITY1101 DuocUC 2025
"""

import logging
import os
from datetime import datetime

import pandas as pd
from psycopg2 import connect, errors as pg_errors
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

BASE       = os.path.join(os.path.dirname(__file__), "..")
VAL_DIR    = os.path.join(BASE, "data", "validated")
LOG_DIR    = os.path.join(BASE, "logs")
CHUNK_SIZE = 10_000   # filas por lote

LOAD_COLUMNS = [
    "trip_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "trip_miles", "fare", "tips", "tolls", "extras",
    "trip_total", "payment_type", "company",
    "pickup_community_area", "dropoff_community_area",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
    "trip_year", "trip_month", "trip_hour", "trip_day_of_week", "anomalia",
]


def _get_conn():
    return connect(
        host    =os.getenv("PG_HOST",     "postgres"),
        port    =int(os.getenv("PG_PORT", "5432")),
        dbname  =os.getenv("PG_DB",       "chicago_taxi"),
        user    =os.getenv("PG_USER",     "taxi_user"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def _years_already_loaded(conn) -> set:
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT DISTINCT trip_year FROM taxi_trips;")
            return {row[0] for row in cur.fetchall()}
        except Exception:
            conn.rollback()
            return set()


def _to_python(val):
    """Convierte tipos numpy/pandas a tipos nativos de Python para psycopg2."""
    if pd.isna(val):
        return None
    if hasattr(val, "item"):
        return val.item()
    return val


def load() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, "load.log")

    val_path = os.path.join(VAL_DIR, "taxi_trips_validated.parquet")
    if not os.path.exists(val_path):
        raise FileNotFoundError(f"No existe {val_path}. Ejecuta primero la etapa validate.")

    df = pd.read_parquet(val_path)
    cols = [c for c in LOAD_COLUMNS if c in df.columns]
    df   = df[cols].copy()
    log.info(f"Registros a cargar: {len(df):,}")

    conn = _get_conn()
    log.info("Conexion a PostgreSQL exitosa.")

    # Carga incremental: omitir años ya cargados
    loaded_years = _years_already_loaded(conn)
    if loaded_years:
        log.info(f"Anos ya en BD: {sorted(loaded_years)} — se omitiran.")
        df = df[~df["trip_year"].isin(loaded_years)]
        log.info(f"Registros nuevos a insertar: {len(df):,}")

    if df.empty:
        log.info("No hay registros nuevos. Carga omitida.")
        conn.close()
        return

    total        = len(df)
    n_inserted   = 0
    rejected_rows = []
    col_names    = ", ".join(cols)
    placeholders = f"INSERT INTO taxi_trips ({col_names}) VALUES %s ON CONFLICT (trip_id) DO NOTHING"

    with open(log_path, "a") as logf:
        logf.write(f"\n=== Carga iniciada: {datetime.now().isoformat()} ===\n")

        for start in range(0, total, CHUNK_SIZE):
            chunk = df.iloc[start:start + CHUNK_SIZE]
            records = [
                tuple(_to_python(row[c]) for c in cols)
                for _, row in chunk.iterrows()
            ]
            try:
                with conn.cursor() as cur:
                    execute_values(cur, placeholders, records)
                conn.commit()
                n_inserted += len(chunk)
            except Exception as e:
                conn.rollback()
                log.warning(f"  Error en lote {start}-{start+CHUNK_SIZE}: {e}. Insertando fila a fila...")
                # Fallback fila a fila solo para el lote con error
                for _, row in chunk.iterrows():
                    record = tuple(_to_python(row[c]) for c in cols)
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"INSERT INTO taxi_trips ({col_names}) VALUES ({','.join(['%s']*len(cols))}) ON CONFLICT (trip_id) DO NOTHING",
                                record
                            )
                        conn.commit()
                        n_inserted += 1
                    except Exception as e2:
                        conn.rollback()
                        r = row.copy()
                        r["rejection_reason"] = str(e2)
                        rejected_rows.append(r)
                        logf.write(f"REJECTED trip_id={row.get('trip_id')} — {e2}\n")

            pct = min((start + CHUNK_SIZE) / total * 100, 100)
            log.info(f"  Progreso: {min(start + CHUNK_SIZE, total):,}/{total:,} ({pct:.1f}%)")

        logf.write(f"Insertados: {n_inserted:,}  Rechazados: {len(rejected_rows):,}\n")

    conn.close()

    # Guardar resultados
    pd.DataFrame(df.iloc[:n_inserted]).to_parquet(
        os.path.join(VAL_DIR, "taxi_trips_inserted.parquet"), index=False)
    if rejected_rows:
        pd.DataFrame(rejected_rows).to_parquet(
            os.path.join(VAL_DIR, "taxi_trips_db_rejected.parquet"), index=False)

    log.info(f"Carga finalizada. Insertados: {n_inserted:,} | Rechazados: {len(rejected_rows):,}")
    log.info(f"Log detallado: {log_path}")
