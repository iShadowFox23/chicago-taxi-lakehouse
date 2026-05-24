#!/bin/bash
# run_pipeline.sh – Orquestador del pipeline DataOps
set -e

echo "============================================================"
echo " PIPELINE CHICAGO TAXI LAKEHOUSE"
echo " Fecha: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

echo ""
echo ">>> ETAPA 1: Ingesta desde BigQuery (Raw Layer)"
python3 scripts/01_ingestion.py
echo "✓ Etapa 1 completada"

echo ""
echo ">>> ETAPA 2: Limpieza y Transformación (Silver Layer)"
python3 scripts/02_transform.py
echo "✓ Etapa 2 completada"

echo ""
echo ">>> ETAPA 3: Validación Estructural y Semántica"
python3 scripts/03_validate.py
echo "✓ Etapa 3 completada"

echo ""
echo ">>> ETAPA 4: Carga en PostgreSQL (Gold Layer)"
python3 scripts/04_load_postgres.py
echo "✓ Etapa 4 completada"

echo ""
echo "============================================================"
echo " PIPELINE FINALIZADO EXITOSAMENTE"
echo " Fecha: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
