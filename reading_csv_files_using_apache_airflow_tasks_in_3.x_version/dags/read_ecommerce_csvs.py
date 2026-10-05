"""Read and print records from the ecommerce CSV files."""

import csv
import os
from pathlib import Path

from airflow.sdk import dag, task
from pendulum import datetime


CSV_DIRECTORY = Path(os.environ.get("ECOMMERCE_CSV_DIR", "/mnt/ecommerce_data"))


@dag(
    dag_id="read_ecommerce_csvs",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["ecommerce", "csv"],
)
def read_ecommerce_csvs():
    @task
    def read_customers():
        file_path = CSV_DIRECTORY / "customers_1MB.csv"
        with file_path.open("r", newline="", encoding="utf-8") as csv_file:
            for record in csv.DictReader(csv_file):
                print(record)

    @task
    def read_items():
        file_path = CSV_DIRECTORY / "items_1MB.csv"
        with file_path.open("r", newline="", encoding="utf-8") as csv_file:
            for record in csv.DictReader(csv_file):
                print(record)

    @task
    def read_orders():
        file_path = CSV_DIRECTORY / "orders_1MB.csv"
        with file_path.open("r", newline="", encoding="utf-8") as csv_file:
            for record in csv.DictReader(csv_file):
                print(record)

    @task
    def read_payments():
        file_path = CSV_DIRECTORY / "payments_1MB.csv"
        with file_path.open("r", newline="", encoding="utf-8") as csv_file:
            for record in csv.DictReader(csv_file):
                print(record)

    @task
    def read_shippings():
        file_path = CSV_DIRECTORY / "shippings_1MB.csv"
        with file_path.open("r", newline="", encoding="utf-8") as csv_file:
            for record in csv.DictReader(csv_file):
                print(record)

    read_customers() >> read_items() >> read_orders() >> read_payments() >> read_shippings()


read_ecommerce_csvs()