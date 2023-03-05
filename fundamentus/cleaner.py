from pandas import DataFrame

from fundamentus.config import CLEANER

def clean_data(df: DataFrame) -> DataFrame:
    for clean in CLEANER:
        if "replace" == clean["operation"]:
            for value in clean["value"]:
                df[clean["column"]] = df[clean["column"]].apply(lambda x: x.replace(value["from"], value["to"]))
    
    return df