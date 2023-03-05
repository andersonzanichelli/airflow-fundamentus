from pandas import DataFrame

from fundamentus.config import FILE_OUTPUT

def apply_write(df: DataFrame):
    df.to_csv(FILE_OUTPUT)