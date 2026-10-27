import argparse
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser()
parser.add_argument('--out', required=True)
parser.add_argument('--fail', action='store_true')
parser.add_argument('--slow', action='store_true')
args = parser.parse_args()
with Path(os.environ['MIGRACAO_LOG_FILE']).open('w', encoding='utf-8', buffering=1) as log:
    for count in range(1, 4):
        log.write(f'Registros exportados: {count}\n')
        time.sleep(1 if args.slow else 0.1)
    if args.fail:
        log.write('Erro simulado.\n')
        raise SystemExit(7)
pq.write_table(pa.table({'id': [1, 2, 3]}), args.out)
