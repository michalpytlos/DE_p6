from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Class used to perform data quality checks
    Args:
        redshift_conn_id (str): redshift connection id
        table (str): name of the table to be checked
        test_query (str): test query
        expected_res (int/float/str/bool): expected result of the query
    Attributes:
        redshift_conn_id (str): redshift connection id
        table (str): name of the table to be checked
        test_query (str): test query
        expected_res (int/float/str/bool): expected result of the query
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_conn_id, table, test_query, expected_res,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.test_query = test_query
        self.expected_res = expected_res

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        res = redshift.get_records(self.test_query)
        check_details = (
            "Test query: {} | Expected result: {} | Returned result: {}")

        if len(res) < 1:
            self.log.error(f'No records present in {self.table}')
            self.log.info(f'Tested query: {self.test_query}')
            raise ValueError(f'No records present in {self.table}')
        elif res[0][0] != self.expected_res:
            self.log.error(f'Table {self.table} failed data quality check')
            self.log.error(check_details.format(self.test_query,
                                                self.expected_res, res[0][0]))
            raise Exception(f'Table {self.table} failed data quality check')

        self.log.info(f'Table {self.table} passed data quality check')
        self.log.info(check_details.format(self.test_query,
                                           self.expected_res, res[0][0]))
