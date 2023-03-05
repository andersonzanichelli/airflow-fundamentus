import requests
import pandas

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from pandas import DataFrame


BASE_URL = "http://www.fundamentus.com.br/resultado.php"
FILE_OUTPUT = "/root/fundamentus.csv"

CLEANER = [
    {
        "column": "Cotação",
        "operation": "replace",
        "value": [
            {
                "from": ".", 
                "to": ""
            }, 
            {
                "from":",",
                "to": "."
            }
        ]
    },
    {
        "column": "Div.Yield",
        "operation": "replace",
        "value": [
            {
                "from":"%",
                "to":""
            }
        ]
    },
    {
        "column": "ROIC",
        "operation": "replace",
        "value": [
            {
                "from":"%",
                "to":""
            }, 
            {
                "from":",",
                "to": "."
            }
        ]
    },
    {
        "column": "Div.Yield",
        "operation": "replace",
        "value": [
            {
                "from": ".", 
                "to": ""
            }, 
            {
                "from":",",
                "to": "."
            }
        ]
    },
    {
        "column": "P/L",
        "operation": "replace",
        "value": [
            {
                "from": ".", 
                "to": ""
            }, 
            {
                "from":",",
                "to": "."
            }
        ]
    },
    {
        "column": "Liq. Corr.",
        "operation": "replace",
        "value": [
            {
                "from":",",
                "to":"."
            }
        ]
    }
]

TRANSFORMATIONS = [
    {
        "column": "Cotação",
        "type": float
    },
    {
        "column": "Div.Yield",
        "type": float
    },
    {
        "column": "P/L",
        "type": float
    },
    {
        "column": "Liq. Corr.",
        "type": float
    }
    ,
    {
        "column": "ROIC",
        "type": float
    }
]

FILTERS = [
    {
        "column": "P/L",
        "operation": ">",
        "value": 0.00
    },
    {
        "column": "Div.Yield",
        "operation": "between",
        "value": (5.00, 20.00)
    },
    {
        "column": "Liq. Corr.",
        "operation": ">",
        "value": 2.00
    },
    {
        "column": "ROIC",
        "operation": ">",
        "value": 10.00
    }
]

SORT = [
    {
        "column": "Div.Yield",
        "asc": False
    }
]

SUBSET = ["Papel", "Cotação", "P/L", "P/VP", "PSR", "Div.Yield", "ROE", "Patrim.Liq", "Liq. Corr.", "Cresc. Rec.5a"]

def get_dataframe(ti, task_id) -> DataFrame:
    json = ti.xcom_pull(task_ids=task_id)
    return pandas.read_json(json)


def dataframe_to_json(df: DataFrame):
    return df.to_json()


def extract_tbody(table):
    tbody = table.find("tbody")
    trs = tbody.findAll("tr")

    rows = []
    for row in trs:
        rows.append([td.text for td in row.findAll("td")])

    return rows


def extract_thead(table):
    return [th.text for th in table.findAll("th")]


def get_fundamentus_data():
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    page = requests.get(BASE_URL, headers=agent)
    soup = BeautifulSoup(page.text, features="html.parser")
    table = soup.find('table')
    thead = extract_thead(table)
    tbody = extract_tbody(table)
    df = DataFrame(data=tbody, columns=thead)

    return dataframe_to_json(df)


def clean_fundamentus_data(ti):
    df = get_dataframe(ti, "get_fundamentus_data")
    for clean in CLEANER:
        if "replace" == clean["operation"]:
            for value in clean["value"]:
                df[clean["column"]] = df[clean["column"]].apply(lambda x: x.replace(value["from"], value["to"]))
    
    return dataframe_to_json(df)


def transform_fundamentus_data(ti):
    df = get_dataframe(ti, "clean_fundamentus_data")
    for transform in TRANSFORMATIONS:
        df[transform["column"]] = df[transform["column"]].astype(transform["type"])
    
    return dataframe_to_json(df)


def filter_fundamentus_data(ti):
    df = get_dataframe(ti, "transform_fundamentus_data")
    for filter in FILTERS:
        if ">" == filter["operation"]:
            df = df[ df[filter["column"]] > filter["value"] ]
        
        if "between" == filter["operation"]:
            df = df[ df[filter["column"]].between(filter["value"][0], filter["value"][1]) ]
    
    return dataframe_to_json(df)


def sort_fundamentus_data(ti):
    df = get_dataframe(ti, "filter_fundamentus_data")
    columns = []
    ascending = []

    for sort in SORT:
        columns.append(sort["column"])
        ascending.append(sort["asc"])

    df = df.sort_values(by=columns, ascending=ascending)
    return dataframe_to_json(df)

def write_fundamentus_data(ti):
    df = get_dataframe(ti, "sort_fundamentus_data")
    df.to_csv(FILE_OUTPUT)


with DAG("fundamentus", start_date=datetime(2023, 1, 3), schedule_interval="30 * * * *", catchup=False) as dag:
    get_fundamentus_data = PythonOperator(task_id="get_fundamentus_data", python_callable=get_fundamentus_data)
    clean_fundamentus_data = PythonOperator(task_id="clean_fundamentus_data", python_callable=clean_fundamentus_data)
    transform_fundamentus_data = PythonOperator(task_id="transform_fundamentus_data", python_callable=transform_fundamentus_data)
    filter_fundamentus_data = PythonOperator(task_id="filter_fundamentus_data", python_callable=filter_fundamentus_data)
    sort_fundamentus_data = PythonOperator(task_id="sort_fundamentus_data", python_callable=sort_fundamentus_data)
    write_fundamentus_data = PythonOperator(task_id="write_fundamentus_data", python_callable=write_fundamentus_data)

    get_fundamentus_data >> clean_fundamentus_data >> transform_fundamentus_data >> filter_fundamentus_data >> sort_fundamentus_data >> write_fundamentus_data