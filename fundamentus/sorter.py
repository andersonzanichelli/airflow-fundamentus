from pandas import DataFrame

from fundamentus.config import SORT


def sort_by(df: DataFrame) -> DataFrame:
    columns = []
    ascending = []

    for sort in SORT:
        columns.append(sort["column"])
        ascending.append(sort["asc"])

    df = df.sort_values(by=columns, ascending=ascending)
    return df