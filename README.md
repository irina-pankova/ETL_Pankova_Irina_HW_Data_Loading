# ETL_Pankova_Irina_HW_Data_Loading

## Описание

Проект реализует ETL-пайплайн на Apache Airflow:

- загрузка данных о температуре из CSV
- сохранение результатов в PostgreSQL
- выгрузка результатов в CSV-файлы
- поддержка full load и incremental load

## Стек

- Apache Airflow
- PostgreSQL
- Docker / Docker Compose
- Python (pandas, psycopg2)

## Структура проекта

```text
airflow_etl/
├── airflow/
│   └── dags/
│       ├── full_load_temp_files.py
│       └── incremental_load_temp_files.py
├── data/
│   ├── top_5_coldest_days.csv
│   └── top_5_hottest_days.csv
├── output/
│   ├── full_coldest.csv
│   ├── full_hottest.csv
│   ├── inc_coldest.csv
│   └── inc_hottest.csv
├── docker-compose.yml
├── .gitignore
└── README.md



