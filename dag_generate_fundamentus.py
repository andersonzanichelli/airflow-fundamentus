import pandas

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pandas import DataFrame

from fundamentus.scraper import scraping
from fundamentus.dataframer import generate_dataframe
from fundamentus.cleaner import clean_data
from fundamentus.transformer import apply_transformations
from fundamentus.filter import apply_filters
from fundamentus.sorter import sort_by
from fundamentus.config import FILE_OUTPUT, FILE_OUTPUT_SECTOR, SHEET, SECTOR_PATH
from ticker.extractor import xlsx_extractor


def dataframe_to_json(df: DataFrame):
    return df.to_json()


def get_dataframe(ti, task_id) -> DataFrame:
    json = ti.xcom_pull(task_ids=task_id)
    return pandas.read_json(json)


def get_fundamentus_data():
    thead, tbody = scraping()
    df = generate_dataframe(thead, tbody)
    return dataframe_to_json(df)

def clean_fundamentus_data(ti):
    df = get_dataframe(ti, "get_fundamentus_data")
    df = clean_data(df)
    return dataframe_to_json(df)


def transform_fundamentus_data(ti):
    df = get_dataframe(ti, "clean_fundamentus_data")
    df = apply_transformations(df)
    return dataframe_to_json(df)


def filter_fundamentus_data(ti):
    df = get_dataframe(ti, "transform_fundamentus_data")
    df = apply_filters(df)
    return dataframe_to_json(df)


def get_sector_data():
    df = xlsx_extractor.extract(SHEET, SECTOR_PATH)
    return dataframe_to_json(df)


def sort_fundamentus_data(ti):
    df = get_dataframe(ti, "merge_data")
    df = sort_by(df)
    return dataframe_to_json(df)


def write_fundamentus_data(ti):
    df = get_dataframe(ti, "transform_fundamentus_data")
    df.to_csv(FILE_OUTPUT)


def write_sector_data(ti):
    df = get_dataframe(ti, "get_sector_data")
    df.to_csv(FILE_OUTPUT_SECTOR)


with DAG("fundamentus_table", start_date=datetime(2023, 1, 3), schedule_interval="30 * * * *", catchup=False) as dag:
    get_fundamentus_data = PythonOperator(task_id="get_fundamentus_data", python_callable=get_fundamentus_data)
    clean_fundamentus_data = PythonOperator(task_id="clean_fundamentus_data", python_callable=clean_fundamentus_data)
    transform_fundamentus_data = PythonOperator(task_id="transform_fundamentus_data", python_callable=transform_fundamentus_data)
    #filter_fundamentus_data = PythonOperator(task_id="filter_fundamentus_data", python_callable=filter_fundamentus_data)
    get_sector_data = PythonOperator(task_id="get_sector_data", python_callable=get_sector_data)
    #sort_fundamentus_data = PythonOperator(task_id="sort_fundamentus_data", python_callable=sort_fundamentus_data)
    write_fundamentus_data = PythonOperator(task_id="write_fundamentus_data", python_callable=write_fundamentus_data)
    write_sector_data = PythonOperator(task_id="write_sector_data", python_callable=write_sector_data)

    get_fundamentus_data >> clean_fundamentus_data >> transform_fundamentus_data >> get_sector_data >> [write_fundamentus_data, write_sector_data]