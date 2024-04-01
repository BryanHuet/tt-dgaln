import unittest
from unittest.mock import MagicMock
from airflow.exceptions import AirflowException

import os
import sys

script_dir = os.path.dirname( __file__ )
mymodule_dir = os.path.join( script_dir, '..', 'dags')
from RetrieveAndLoadOperator import RetrieveAndLoadOperator

class TestRetrieveAndLoadOperator(unittest.TestCase):

    def test_check_table_existence_exception(self):
        # given    
        mock_hook = MagicMock()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_hook.get_conn = mock_conn
        mock_cur.cursor = mock_cur
        
        mock_cur.fetchall = ["table1", "table2"]
        retrieve_and_load_operator = RetrieveAndLoadOperator(
            task_id="test",
            data_url="https://url",
            connection_id="my_id",
            table_name="table5"
        )

        # when - then
        with self.assertRaises(AirflowException):
            retrieve_and_load_operator.check_table_existence(mock_hook)


if __name__ == '__main__':
    unittest.main()

