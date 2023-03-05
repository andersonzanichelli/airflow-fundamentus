from pandas import DataFrame

def generate_dataframe(thead, tbody) -> DataFrame:
    df = DataFrame(data=tbody, columns=thead)
    return df