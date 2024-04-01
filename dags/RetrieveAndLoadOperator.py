import requests
import logging
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowNotFoundException, AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


def insert_on_conflict_update(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    insert_query = insert(table.table).values(data)
    upsert = insert_query.on_conflict_do_update(
        index_elements=[keys[0]],
        set_={field: getattr(insert_query.excluded, field) for field in keys}
        )

    result = conn.execute(upsert)
    return result.rowcount


class RetrieveAndLoadOperator(BaseOperator):
    def __init__(self, data_url: str, connection_id: str,
                 table_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_url = data_url
        self.connection_id = connection_id
        self.table_name = table_name

        

    def check_table_existence(self, hook) -> None:
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
        result = cur.fetchall()

        tables_list = [element[0] for element in result]

        cur.close()
        conn.close()
        if self.table_name not in tables_list:
            raise AirflowException(f"{self.table_name} not found in database.")

    def fetch(self) -> pd.DataFrame:
        response = requests.request("GET", self.data_url)

        if response.status_code != 200:
            raise AirflowNotFoundException(
                "Request to " + self.data_url +
                " failed. Please double check your url."
                )

        data = pd.read_csv(self.data_url)
        return data

    def clean_and_keep_new_items(self, data: pd.DataFrame,
                                 connection) -> pd.DataFrame:
        df_dto = pd.read_sql(self.table_name, connection)

        data.set_index(data.columns[0], inplace=True)
        data.sort_index(inplace=True)
        df_dto.set_index(df_dto.columns[0], inplace=True)
        df_dto.sort_index(inplace=True)

        date_cols = df_dto.select_dtypes(
            include=['datetime64']).columns.tolist()

        df_dto[date_cols] = df_dto[date_cols].replace('0023-10-31',
                                                      '2023-10-31')
        df_dto[date_cols] = df_dto[date_cols].apply(pd.to_datetime,
                                                    format='mixed')
        data[date_cols] = data[date_cols].replace('0023-10-31',
                                                  '2023-10-31')
        data[date_cols] = data[date_cols].apply(pd.to_datetime,
                                                format='mixed')

        df_dto = data.loc[data[~data.eq(df_dto)].dropna(how='all').index]

        logging.info(df_dto)
        return df_dto

    def load(self, data: pd.DataFrame, connection) -> None:
        row_inserted = data.to_sql(
            self.table_name, con=connection, if_exists='append',
            method=insert_on_conflict_update
            )
        logging.info(f"{row_inserted} rows upserted in {self.table_name}.")

    def execute(self, context: Context) -> None:
        postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        self.check_table_existence(hook=postgres_hook)

        data_brut = self.fetch()

        data_clean = self.clean_and_keep_new_items(
            data=data_brut, connection=engine)

        self.load(data=data_clean, connection=engine)
