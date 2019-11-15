from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CopyFixedWidthRedshiftOperator(BaseOperator):
    """Class used to copy data in fixed-width format from S3 to Redshift tables
    Args:
        redshift_conn_id (str): redshift connection id
        table (str): name of the table the data is copied to
        s3_bucket (str): S3 bucket name
        s3_key (str): S3 key of the fixed-width file
        arn (str): ARN of IAM role allowing Redshift to read from S3
        fixedwidth_spec (str): file spec in the form of column:width pairs
            delimited by commas.
        maxerror (int): max allowed number of errors during copy
        region (str, optional): AWS region in which the S3 bucket is located.
            Defaults to 'us-east-1'
        delete_load (bool): delete-load mode flag. Defaults to False

    Attributes:
        redshift_conn_id (str): redshift connection id
        table (str): name of the table the data is copied to
        s3_bucket (str): S3 bucket name
        s3_key (str): S3 key of the fixed-width file
        arn (str): ARN of IAM role allowing Redshift to read from S3
        fixedwidth_spec (str): file spec in the form of column:width pairs
            delimited by commas.
        maxerror (int): max allowed number of errors during copy
        region (str): AWS region in which the S3 bucket is located
        delete_load (bool): delete-load mode flag
    """
    ui_color = '#358140'

    copy_query = """
    COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        REGION '{}'
        FIXEDWIDTH '{}'
        MAXERROR {}
    """

    @apply_defaults
    def __init__(self, redshift_conn_id, table, s3_bucket, s3_key,
                 arn, fixedwidth_spec, maxerror=1, region='us-east-1',
                 delete_load=False, *args, **kwargs):
        super(CopyFixedWidthRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.arn = arn
        self.fixedwidth_spec = fixedwidth_spec
        self.maxerror = maxerror
        self.region = region
        self.delete_load = delete_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_load:
            self.log.info(f'Deleting data from {self.table}')
            redshift.run(f'DELETE FROM {self.table}')
            self.log.info(f'Delete completed ({self.table})')

        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        query = CopyFixedWidthRedshiftOperator.copy_query.format(
            self.table,
            s3_path,
            self.arn,
            self.region,
            self.fixedwidth_spec,
            self.maxerror
        )

        self.log.info(f'Copying data from S3 to Redshift ({self.table})')
        redshift.run(query)
        self.log.info(f'Copy completed ({self.table})')
