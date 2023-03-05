from pandas import DataFrame

from fundamentus.config import FILTERS

def apply_filters(df: DataFrame) -> DataFrame:
    for filter in FILTERS:
        if ">" == filter["operation"]:
            df = df[ df[filter["column"]] > filter["value"] ]
        
        if "between" == filter["operation"]:
            df = df[ df[filter["column"]].between(filter["value"][0], filter["value"][1]) ]
    
    return df