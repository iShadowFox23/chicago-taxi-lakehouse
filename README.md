# Chicago Taxi Lakehouse – Pipeline DataOps

Pipeline de datos reproducible que extrae, limpia, valida y carga el dataset público **Chicago Taxi Trips** de Google BigQuery en una base de datos PostgreSQL.

Proyecto desarrollado para el curso **Gestión de Datos para IA (ITY1101) – DuocUC 2025**.

**Integrantes:** Agustin Bahamondes · Diego Bahamondez · Agustin Morales · Joaquín Fernandez

---

## Dataset

**Fuente:** `bigquery-public-data.chicago_taxi_trips.taxi_trips` (Google BigQuery Public Data)  
**Años extraídos:** 2019, 2020, 2021  
**Registros originales:** ~8.1 millones (tras limpieza: ~7.7 millones)

| Año  | Registros originales | Tras limpieza | Observación |
|------|---------------------|---------------|-------------|
| 2019 | 7,283,804           | 6,909,733     | Año base    |
| 2020 | 815,519             | 791,195       | Caída por pandemia COVID-19 (-89%) |
| 2021 | 79,220              | 76,870        | Recuperación parcial |

---

## Arquitectura del pipeline

```
BigQuery (fuente pública)
      │
      ▼  notebooks/chicago_taxi_trips.ipynb  (extracción en Google Colab)
data/raw/
  ├── chicago_taxi_trips2019.parquet
  ├── chicago_taxi_trips2020.parquet
  └── chicago_taxi_trips2021.parquet
      │
      ▼  src/clean.py  (Stage 1)
data/processed/
  └── taxi_trips_{año}_clean.parquet
      │
      ▼  src/validate.py  (Stage 2)
data/validated/
  ├── taxi_trips_validated.parquet
  ├── taxi_trips_rejected.parquet
data/reports/
  └── validation_report.txt
      │
      ▼  src/load.py  (Stage 3)
PostgreSQL  (contenedor Docker)
data/validated/
  ├── taxi_trips_inserted.parquet
  └── taxi_trips_db_rejected.parquet
logs/
  └── load.log
```

### Infraestructura Docker

```
pipeline_net (bridge)
  ├── postgres  — PostgreSQL 16-alpine, puerto 5432
  ├── pgadmin   — pgAdmin 4, puerto 8080
  └── pipeline  — contenedor Python que ejecuta main.py
```

---

## Obtención de los datos

Los archivos Parquet **no están incluidos en el repositorio** por su tamaño. Se generan ejecutando el notebook de extracción en Google Colab:

1. Abre `notebooks/chicago_taxi_trips.ipynb` en [Google Colab](https://colab.research.google.com/)
2. Ejecuta todas las celdas (requiere cuenta de Google con acceso a BigQuery)
3. Descarga los 3 archivos `.parquet` generados
4. Colócalos en `data/raw/`

---

## Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Los 3 archivos Parquet en `data/raw/` (ver sección anterior)

---

## Puesta en marcha

### 1. Clonar el repositorio

```bash
git clone https://github.com/iShadowFox23/chicago-taxi-lakehouse.git
cd chicago-taxi-lakehouse
```

### 2. Agregar los archivos Parquet

```
data/raw/
  ├── chicago_taxi_trips2019.parquet
  ├── chicago_taxi_trips2020.parquet
  └── chicago_taxi_trips2021.parquet
```

### 3. Levantar la base de datos

```bash
docker compose up postgres pgadmin -d
```

Espera que postgres muestre estado `healthy`:

```bash
docker compose ps
```

### 4. Ejecutar el pipeline completo

```bash
docker compose build --no-cache
docker compose run --rm pipeline python main.py
```

O por etapas individuales:

```bash
docker compose run --rm pipeline python main.py --stage clean
docker compose run --rm pipeline python main.py --stage validate
docker compose run --rm pipeline python main.py --stage load
```

---

## Etapas del pipeline

### Stage 1 — Limpieza (`src/clean.py`)

| Transformación | Detalle |
|---|---|
| Renombrar columnas | `unique_key → trip_id`, coordenadas a formato estándar |
| Eliminar duplicados | Por `trip_id` |
| Filtrar duración cero | `trip_seconds > 0` |
| Marcar anomalías | Flag `anomalia=True` cuando `trip_miles=0` y `trip_total>0` |
| Normalizar timestamps | Conversión a UTC |
| Normalizar company | Strip, uppercase, NULL → 'UNKNOWN' |
| Columnas derivadas | `trip_year`, `trip_month`, `trip_hour`, `trip_day_of_week` |

### Stage 2 — Validación (`src/validate.py`)

Reglas semánticas aplicadas por fila:

| Regla | Descripción |
|---|---|
| `trip_total_non_negative` | trip_total >= 0 |
| `trip_seconds_range` | trip_seconds entre 1 y 86.400 |
| `fare_non_negative` | fare >= 0 |
| `trip_id_not_null` | trip_id no puede ser nulo |
| `pickup_lat_range` | Latitud dentro del bounding box de Chicago (41.6°–42.1°N) |
| `pickup_lon_range` | Longitud dentro del bounding box de Chicago (-87.95° a -87.5°W) |

Umbral mínimo de calidad: **95%**. Si no se alcanza, el pipeline se detiene antes de la carga.

**Salidas:**
- `data/validated/taxi_trips_validated.parquet` — registros aprobados
- `data/validated/taxi_trips_rejected.parquet` — registros rechazados con columna `failed_rules`
- `data/reports/validation_report.txt` — reporte con conteo por regla
- `logs/metrics.json` — métricas de calidad

### Stage 3 — Carga (`src/load.py`)

- Conexión a PostgreSQL via psycopg2
- Carga incremental por año (omite años ya cargados)
- Inserción en lotes de 10.000 filas con `execute_values`
- Manejo de `UniqueViolation` y `CheckViolation` con fallback fila a fila
- Verificación post-carga con log detallado

**Salidas:**
- `data/validated/taxi_trips_inserted.parquet`
- `data/validated/taxi_trips_db_rejected.parquet`
- `logs/load.log`

---

## Tabla en PostgreSQL

```sql
CREATE TABLE IF NOT EXISTS taxi_trips (
    trip_id                    TEXT          PRIMARY KEY,
    trip_start_timestamp       TIMESTAMPTZ,
    trip_end_timestamp         TIMESTAMPTZ,
    trip_seconds               INTEGER,
    trip_miles                 DOUBLE PRECISION,
    fare                       DOUBLE PRECISION,
    tips                       DOUBLE PRECISION,
    tolls                      DOUBLE PRECISION,
    extras                     DOUBLE PRECISION,
    trip_total                 DOUBLE PRECISION,
    payment_type               VARCHAR(50),
    company                    VARCHAR(100),
    pickup_community_area      SMALLINT,
    dropoff_community_area     SMALLINT,
    pickup_centroid_latitude   DOUBLE PRECISION,
    pickup_centroid_longitude  DOUBLE PRECISION,
    dropoff_centroid_latitude  DOUBLE PRECISION,
    dropoff_centroid_longitude DOUBLE PRECISION,
    trip_year                  SMALLINT,
    trip_month                 SMALLINT,
    trip_hour                  SMALLINT,
    trip_day_of_week           SMALLINT,
    anomalia                   BOOLEAN       DEFAULT FALSE,
    loaded_at                  TIMESTAMPTZ   DEFAULT NOW()
);
```

---

## pgAdmin

Acceder en [http://localhost:8080](http://localhost:8080) con las credenciales del `.env`.

Agregar servidor desde pgAdmin:
- **Host:** `postgres`
- **Port:** `5432`
- **Database:** `chicago_taxi`
- **Username:** `taxi_user`
- **Password:** `taxi_password`

---

## Estructura de archivos

```
chicago-taxi-lakehouse/
├── notebooks/
│   └── chicago_taxi_trips.ipynb   # Extracción desde BigQuery en Google Colab
├── src/
│   ├── clean.py                   # Stage 1 – Limpieza y transformación
│   ├── validate.py                # Stage 2 – Validación estructural y semántica
│   └── load.py                    # Stage 3 – Carga en PostgreSQL
├── sql/
│   └── create_table.sql           # DDL con constraints e índices
├── data/
│   ├── raw/                       # Parquet originales (no en repo)
│   ├── processed/                 # Parquet limpios
│   ├── validated/                 # Parquet validados/rechazados
│   └── reports/                   # Reportes de validación
├── logs/                          # Logs de ejecución
├── main.py                        # Orquestador con --stage
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env                           # Variables de entorno (no en repo)
```
