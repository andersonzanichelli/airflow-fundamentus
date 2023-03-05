from pandas import DataFrame
from fundamentus.config import SUBSET

def apply_subset(df: DataFrame) -> DataFrame:
    df = df[SUBSET]
    return df