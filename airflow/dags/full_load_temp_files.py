from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from pathlib import Path

PROJECT_DIR = Path("/opt/airflow/project")
DATA_DIR = PROJECT_DIR / "data"
OUT_DIR = PROJECT_DIR / "output"

FILES = {
    "coldest": ("top_5_coldest_days.csv", "public.top_5_coldest_days", "full_coldest.csv"),
    "hottest": ("top_5_hottest_days.csv", "public.top_5_hottest_days", "full_hottest.csv"),
}

def _full_load_one(src_name: str, table: str, out_name: str):
    src = DATA_DIR / src_name
    df = pd.read_csv(src)

    df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.date

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / out_name
    df.to_csv(out_path, index=False)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    noted_date DATE,
                    avg_temp DOUBLE PRECISION
                );
            """)
            cur.execute(f"TRUNCATE TABLE {table};")
        conn.commit()

    hook.copy_expert(
        sql=f"COPY {table} (noted_date, avg_temp) FROM STDIN WITH CSV HEADER",
        filename=str(out_path),
    )

def full_load_all():
    for _, (src_name, table, out_name) in FILES.items():
        _full_load_one(src_name, table, out_name)

with DAG(
    dag_id="full_load_temp_to_db_and_file",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["homework", "full_load"],
) as dag:
    PythonOperator(
        task_id="full_load_all",
        python_callable=full_load_all,
    )