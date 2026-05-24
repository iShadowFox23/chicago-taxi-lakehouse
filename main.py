"""
main.py – Orquestador del pipeline Chicago Taxi Lakehouse.
Ejecuta las 4 etapas en orden. Permite correr una etapa individual.

Uso:
  python main.py                  # todas las etapas
  python main.py --stage ingest
  python main.py --stage clean
  python main.py --stage validate
  python main.py --stage load

Autor: Equipo Chicago Taxi Lakehouse – ITY1101 DuocUC 2025
"""

import argparse
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run_ingest() -> None:
    from src.ingest import ingest
    ingest()

def run_clean() -> None:
    from src.clean import clean
    clean()

def run_validate() -> None:
    from src.validate import validate
    validate()

def run_load() -> None:
    from src.load import load
    load()


STAGES = {
    "ingest":   run_ingest,
    "clean":    run_clean,
    "validate": run_validate,
    "load":     run_load,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Chicago Taxi Lakehouse pipeline")
    parser.add_argument(
        "--stage",
        choices=list(STAGES),
        default=None,
        help="Ejecutar solo una etapa en lugar del pipeline completo.",
    )
    args = parser.parse_args(argv)

    stages = [args.stage] if args.stage else list(STAGES)
    t0 = time.perf_counter()

    for stage in stages:
        log.info(f"{'='*55}")
        log.info(f"  Iniciando etapa: {stage.upper()}")
        log.info(f"{'='*55}")
        t1 = time.perf_counter()
        try:
            STAGES[stage]()
        except Exception:
            log.exception(f"Etapa '{stage}' fallida.")
            sys.exit(1)
        log.info(f"  Etapa '{stage}' completada en {time.perf_counter() - t1:.1f}s")

    log.info(f"Pipeline finalizado en {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
