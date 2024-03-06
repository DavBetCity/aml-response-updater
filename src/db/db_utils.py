from .bq import BQ
import pandas as pd
from datetime import date


############################################################################################
###### CSV UTILS
############################################################################################
def read_csv(file_path: str, delimiter: bool = None) -> pd.DataFrame:
    return pd.read_csv(file_path, low_memory=False, delimiter=delimiter)


def save_csv(df, file_path):
    df.to_csv(file_path)
    print(f"file saved to {file_path}")


############################################################################################
###### BQ UTILS
############################################################################################
def fetch_bq_data(query: str) -> pd.DataFrame:
    client = BQ()
    df = client.get_table_bq(query)

    return df
