from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

PROJECT_DIR = Path("/opt/airflow/project")
DATA_DIR = PROJECT_DIR / "data"
OUT_DIR = PROJECT_DIR / "output"

FILES = {
    "coldest": ("top_5_coldest_days.csv", "public.top_5_coldest_days", "inc_coldest.csv"),
    "hottest": ("top_5_hottest_days.csv", "public.top_5_hottest_days", "inc_hottest.csv"),
}

def _increment_one(src_name: str, table: str, out_name: str, days: int):
    src = DATA_DIR / src_name
    df = pd.read_csv(src)
    df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.date

    boundary = (datetime.utcnow() - timedelta(days=days)).date()
    inc = df[df["noted_date"] >= boundary].copy()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / out_name
    inc.to_csv(out_path, index=False)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    noted_date DATE,
                    avg_temp DOUBLE PRECISION
                );
            """)
            # “перезапись партиции”: удаляем даты, которые будем догружать
            cur.execute(f"DELETE FROM {table} WHERE noted_date >= %s;", (boundary,))
        conn.commit()

    if len(inc) > 0:
        hook.copy_expert(
            sql=f"COPY {table} (noted_date, avg_temp) FROM STDIN WITH CSV HEADER",
            filename=str(out_path),
        )

def incremental_load_all(days: int = 14):
    for _, (src_name, table, out_name) in FILES.items():
        _increment_one(src_name, table, out_name, days)

with DAG(
    dag_id="incremental_load_temp_last_days_to_db_and_file",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["homework", "incremental"],
) as dag:
    PythonOperator(
        task_id="incremental_load_all",
        python_callable=incremental_load_all,
        op_kwargs={"days": 14},  # “последние несколько дней” -> поставила 14 как в примере вебинара
    )