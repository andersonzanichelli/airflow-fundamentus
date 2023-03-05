BASE_URL = "http://www.fundamentus.com.br/resultado.php"
FILE_OUTPUT = "/root/fundamentus.csv"
FILE_OUTPUT_SECTOR = "/root/sector.csv"
SHEET = "Plan3"
SECTOR_PATH = "/root/airflow/dags/resources/SetorialB3.xlsx"


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