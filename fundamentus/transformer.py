from pandas import DataFrame

from fundamentus.config import TRANSFORMATIONS

def apply_transformations(df: DataFrame) -> DataFrame:
    for transform in TRANSFORMATIONS:
        df[transform["column"]] = df[transform["column"]].astype(transform["type"])
    
    return df