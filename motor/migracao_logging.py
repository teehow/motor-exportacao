import argparse
import logging
import os
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path


def format_parquet_summary(arguments):
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--out", type=Path)
    args, _ = parser.parse_known_args(arguments)
    if args.out is None:
        return ""

    prefix = f" | Parquet: {args.out.name} | Registros: "
    try:
        import pyarrow.parquet as pq

        with pq.ParquetFile(args.out) as parquet_file:
            total = parquet_file.metadata.num_rows
    except Exception as exc:
        return f"{prefix}indisponivel ({type(exc).__name__})"
    return prefix + f"{total:,}".replace(",", ".")


def run_script(main, script_path):
    if "--help" in sys.argv[1:] or "-h" in sys.argv[1:]:
        return main()
    script = Path(script_path)
    configured_path = os.getenv("MIGRACAO_LOG_FILE")
    if configured_path:
        log_path = Path(configured_path)
    else:
        log_dir = Path(os.getenv("MIGRACAO_LOG_DIR", str(script.parent / "logs")))
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        log_path = log_dir / f"{script.stem}_{stamp}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    console = sys.stdout
    started = time.monotonic()
    print(f"INICIO | {datetime.now():%d/%m/%Y %H:%M:%S} | {script.name}", file=console, flush=True)
    code = 0
    with log_path.open("a", encoding="utf-8") as detail:
        with redirect_stdout(detail), redirect_stderr(detail):
            logging.basicConfig(
                level=logging.INFO, stream=detail, force=True,
                format="%(asctime)s | %(levelname)s | %(message)s",
            )
            try:
                code = main() or 0
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else int(exc.code is not None)
                if isinstance(exc.code, str):
                    logging.error("%s", exc.code)
            except KeyboardInterrupt:
                logging.error("Execucao interrompida pelo usuario.")
                code = 130
            except Exception:
                logging.exception("Falha na execucao de %s", script.name)
                code = 1
            finally:
                logging.shutdown()
    status = "OK" if code == 0 else f"ERRO (codigo {code})"
    parquet_summary = format_parquet_summary(sys.argv[1:]) if code == 0 else ""
    print(
        f"FIM | {script.name} | {status} | {time.monotonic() - started:.1f}s{parquet_summary} | Log: {log_path}",
        file=console, flush=True,
    )
    return code
