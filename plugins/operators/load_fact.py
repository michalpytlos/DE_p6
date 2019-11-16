from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Class used to load data to fact table
    Args:
        redshift_conn_id (str): redshift connection id
        table (str): name of the fact table the data is loaded to
        insert_query (str): insert query
    Attributes:
        redshift_conn_id (str): redshift connection id
        table (str): name of the fact table the data is loaded to
        insert_query (str): insert query
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, redshift_conn_id, table, insert_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Loading data to {self.table}')
        redshift.run(self.insert_query.format(self.table))
        self.log.info(f'Load completed ({self.table})')
