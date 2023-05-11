import pandas as pd
from src.python.services.helpers import get_yaml, get_casting
from src.python.services.database_connection import get_engine

credentials = {
    "host": "general.xxxxxxxxx.us-west-1.redshift.amazonaws.com",
    "user": "aimee",
    "password": "aimee",
    "database": "local",
    "port": "5439",
}


class DataHandler:
    def __init__(self):
        self.config = get_yaml("src/python/config", "mapping.yml")

    def _extract_data(self, path):
        df = pd.read_csv(path)

        return df

    def _transform_data(self, df, file_name):
        transformation = self.config["data_hanlder"]

        if file_name == "worldometer_data":
            df.drop(df.columns[[11, 12, 14, 15]], axis=1, inplace=True)
            df = get_casting(df, transformation, "worldometer")
            columns_sorted = transformation["worldometer"]["columns_sorted"]
            df_ = df[columns_sorted]

        if file_name == "country_wise_latest":
            df.drop(df.iloc[:, 8:], inplace=True, axis=1)
            df_ = get_casting(df, transformation, "country_wise")

        if file_name == "full_grouped":
            df.drop(df.iloc[:, 6:], inplace=True, axis=1)
            df_ = get_casting(df, transformation, "full_grouped")
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
            df_ = df.groupby(["country", "date"]).sum()

        if file_name == "covid_19_clean_complete":
            df.drop(df.columns[[0, 2, 3, 9]], axis=1, inplace=True)
            df_ = get_casting(df, transformation, "clean_complete")
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
            df_ = df.groupby(["country", "date"]).sum()

        return df_

    def _load_data(self, df, file_name):
        key = f"src/resources/refined/{file_name}.gz.parquet"
        df.to_parquet(key)
        engine = get_engine(**credentials)

        if file_name == "worldometer_data" or file_name == "country_wise_latest":
            df.to_sql(
                "covid19_global",
                engine,
                schema="staging",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=200,
            )

        else:
            df.to_sql(
                "covid19_global_dated",
                engine,
                schema="staging",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=200,
            )
