import datetime

from RetrieveAndLoadOperator import RetrieveAndLoadOperator
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="process-propluvia",
    schedule=datetime.timedelta(minutes=20),
    start_date=datetime.datetime(2024, 3, 27),
    catchup=False,
    tags=["dev"],
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessPropluvia():
    create_zones_table = SQLExecuteQueryOperator(
        task_id="create_zones_table",
        conn_id="conn_id",
        sql="sql/zones.sql"
    )
    create_arretes_table = SQLExecuteQueryOperator(
        task_id="create_arretes_table",
        conn_id="conn_id",
        sql="sql/arretes.sql"
    )

    get_and_load_zones = RetrieveAndLoadOperator(
        task_id="get_and_load_zones",
        data_url="https://www.data.gouv.fr/fr/datasets/r/ac45ed59-7f4b-453a-9b3d-3124af470056",
        connection_id="conn_id",
        table_name="zones"
    )

    get_and_load_arretes = RetrieveAndLoadOperator(
        task_id="get_and_load_arretes",
        data_url="https://www.data.gouv.fr/fr/datasets/r/782aac32-29c8-4b66-b231-ab4c3005f574",
        connection_id="conn_id",
        table_name="arretes"
    )

    create_view = SQLExecuteQueryOperator(
        task_id="create_view",
        conn_id="conn_id",
        sql="sql/views.sql"
    )
    create_zones_table >> get_and_load_zones >> create_view
    create_arretes_table >> get_and_load_arretes >> create_view


dag = ProcessPropluvia()
