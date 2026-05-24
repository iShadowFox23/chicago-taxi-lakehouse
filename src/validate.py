"""
src/validate.py
---------------
Stage 2 – Validacion estructural y semantica.
Lee los Parquet procesados y aplica reglas de calidad.
Separa registros validos de rechazados y genera un reporte.

Salidas:
  data/validated/taxi_trips_validated.parquet
  data/validated/taxi_trips_rejected.parquet  (con columna failed_rules)
  data/reports/validation_report.txt

Si la tasa de registros validos < 95% el pipeline se detiene.

Autor: Equipo Chicago Taxi Lakehouse – ITY1101 DuocUC 2025
"""

import json
import logging
import os
from datetime import datetime
from glob import glob

import pandas as pd

log = logging.getLogger(__name__)

BASE       = os.path.join(os.path.dirname(__file__), "..")
PROC_DIR   = os.path.join(BASE, "data", "processed")
VAL_DIR    = os.path.join(BASE, "data", "validated")
REP_DIR    = os.path.join(BASE, "data", "reports")
LOG_DIR    = os.path.join(BASE, "logs")

QUALITY_THRESHOLD = 0.95

REQUIRED_COLUMNS = [
    "trip_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "trip_miles", "fare", "trip_total",
    "payment_type", "company",
]

# Bounding box de Chicago
CHI_LAT = (41.6, 42.1)
CHI_LON = (-87.95, -87.5)


def _check_rules(df: pd.DataFrame) -> pd.Series:
    """Devuelve una Series con la lista de reglas fallidas por fila (string)."""
    failed = pd.Series([""] * len(df), index=df.index)

    def flag(mask, rule):
        failed[mask] = failed[mask].apply(
            lambda x: f"{x},{rule}" if x else rule
        )

    # Reglas semanticas
    flag(df["trip_total"] < 0,                                 "trip_total_negativo")
    flag((df["trip_seconds"] < 1) | (df["trip_seconds"] > 86400), "trip_seconds_fuera_rango")
    flag(df["trip_id"].isna(),                                 "trip_id_nulo")
    flag(df["fare"] < 0,                                       "fare_negativo")

    if "pickup_centroid_latitude" in df.columns:
        lat = df["pickup_centroid_latitude"]
        flag(lat.notna() & ((lat < CHI_LAT[0]) | (lat > CHI_LAT[1])), "latitud_fuera_chicago")

    if "pickup_centroid_longitude" in df.columns:
        lon = df["pickup_centroid_longitude"]
        flag(lon.notna() & ((lon < CHI_LON[0]) | (lon > CHI_LON[1])), "longitud_fuera_chicago")

    return failed


def validate() -> None:
    files = sorted(glob(os.path.join(PROC_DIR, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No hay Parquet en {PROC_DIR}. Ejecuta primero la etapa clean.")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    total = len(df)
    log.info(f"Total registros a validar: {total:,}")

    # Validacion estructural
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes: {missing}")
    log.info("Validacion estructural: OK")

    # Validacion semantica
    df["failed_rules"] = _check_rules(df)
    valid_mask   = df["failed_rules"] == ""
    df_valid     = df[valid_mask].drop(columns=["failed_rules"])
    df_rejected  = df[~valid_mask]

    n_valid    = len(df_valid)
    n_rejected = len(df_rejected)
    quality    = n_valid / total

    log.info(f"Validos  : {n_valid:,} ({quality:.2%})")
    log.info(f"Rechazados: {n_rejected:,}")

    # Conteo por regla
    rule_counts: dict = {}
    for rules in df_rejected["failed_rules"]:
        for r in rules.split(","):
            rule_counts[r] = rule_counts.get(r, 0) + 1
    for rule, cnt in sorted(rule_counts.items(), key=lambda x: -x[1]):
        log.info(f"  [{rule}] {cnt:,} registros")

    # Guardar salidas
    os.makedirs(VAL_DIR, exist_ok=True)
    os.makedirs(REP_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    df_valid.to_parquet(os.path.join(VAL_DIR, "taxi_trips_validated.parquet"),
                        index=False, compression="snappy")
    df_rejected.to_parquet(os.path.join(VAL_DIR, "taxi_trips_rejected.parquet"),
                           index=False, compression="snappy")

    # Reporte de texto
    report_path = os.path.join(REP_DIR, "validation_report.txt")
    with open(report_path, "w") as f:
        f.write(f"REPORTE DE VALIDACION\n")
        f.write(f"Generado : {datetime.now().isoformat()}\n")
        f.write(f"{'='*50}\n")
        f.write(f"Total registros : {total:,}\n")
        f.write(f"Validos         : {n_valid:,} ({quality:.2%})\n")
        f.write(f"Rechazados      : {n_rejected:,}\n")
        f.write(f"\nDetalle por regla:\n")
        for rule, cnt in sorted(rule_counts.items(), key=lambda x: -x[1]):
            f.write(f"  {rule}: {cnt:,}\n")
    log.info(f"Reporte guardado: {report_path}")

    # metrics.json para monitoreo
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "total": total, "valid": n_valid, "rejected": n_rejected,
        "quality_rate": round(quality, 4),
        "threshold": QUALITY_THRESHOLD,
        "rule_counts": rule_counts,
    }
    with open(os.path.join(LOG_DIR, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    if quality < QUALITY_THRESHOLD:
        raise ValueError(
            f"Calidad insuficiente: {quality:.2%} < {QUALITY_THRESHOLD:.0%}. "
            "Revisa data/reports/validation_report.txt"
        )
    log.info("Dataset aprobado para carga en PostgreSQL.")
