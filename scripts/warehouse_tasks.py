"""
BrightCart: load validated rows into the DuckDB warehouse.

This is the "Load raw.orders" step of the Airflow + dbt pipeline. It runs
after run_quality_check, so only rows that passed every check reach the
warehouse. dbt then builds its models on top of these raw tables.

Idempotent: the rows of the day are deleted first, then inserted again,
so rerunning the same day never duplicates data.

Manual run, from the project folder:
    python scripts/warehouse_tasks.py 20261105
"""

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
WAREHOUSE = DATA_DIR / "warehouse" / "brightcart.duckdb"

TABLES = {
    "orders": "orders_valid.csv",
    "customers": "customers_valid.csv",
}


def load_raw_to_warehouse(run_date):
    """Copy the day's valid customers and orders into raw.customers and raw.orders."""
    cleaned = DATA_DIR / "cleaned" / run_date
    for file_name in TABLES.values():
        if not (cleaned / file_name).exists():
            raise FileNotFoundError(f"[{run_date}] Run the pipeline first, missing {cleaned / file_name}")

    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    loaded = {}
    with duckdb.connect(str(WAREHOUSE)) as con:
        con.execute("create schema if not exists raw")
        for table, file_name in TABLES.items():
            csv_path = str(cleaned / file_name)
            select_day = f"select *, '{run_date}' as run_date from read_csv_auto(?, all_varchar = true)"
            # Create the table the first time, with the same columns as the file
            con.execute(f"create table if not exists raw.{table} as {select_day} limit 0", [csv_path])
            # Replace the rows of this day only (idempotent)
            con.execute(f"delete from raw.{table} where run_date = ?", [run_date])
            con.execute(f"insert into raw.{table} by name {select_day}", [csv_path])
            loaded[table] = con.execute(f"select count(*) from raw.{table} where run_date = ?", [run_date]).fetchone()[0]

    print(f"[{run_date}] Loaded into {WAREHOUSE.relative_to(PROJECT_ROOT)}: {loaded}")
    return loaded


if __name__ == "__main__":
    for run_date in sys.argv[1:] or ["20261105"]:
        load_raw_to_warehouse(run_date)
