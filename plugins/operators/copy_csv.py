from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CopyCsvRedshiftOperator(BaseOperator):
    """Class used to copy data in CSV format from S3 to Redshift tables
    Args:
        redshift_conn_id (str): redshift connection id
        table (str): name of the table the data is copied to
        s3_bucket (str): S3 bucket name
        s3_prefix (str): S3 key prefix common to all CSV files with data
        arn (str): ARN of IAM role allowing Redshift to read from S3
        region (str, optional): AWS region in which the S3 bucket is located.
            Defaults to 'us-east-1'
        ignore_header (int): number of rows in CSV files to be ignored when
            loading the data. Defaults to 0
    Attributes:
        redshift_conn_id (str): redshift connection id
        table (str): name of the table the data is copied to
        s3_bucket (str): S3 bucket name
        s3_prefix (str): S3 key prefix common to all CSV files with data
        arn (str): ARN of IAM role allowing Redshift to read from S3
        region (str): AWS region in which the S3 bucket is located
        ignore_header (int): number of rows in CSV files to be ignored when
            loading the data.
    """
    ui_color = '#358140'

    copy_query = """
    COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        REGION '{}'
        CSV
        IGNOREHEADER {}
    """

    @apply_defaults
    def __init__(self, redshift_conn_id, table, s3_bucket, s3_prefix,
                 arn, region='us-east-1', ignore_header=0, *args, **kwargs):
        super(CopyCsvRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.arn = arn
        self.region = region
        self.ignore_header = ignore_header

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        date = context['ds']
        s3_path = f's3://{self.s3_bucket}/{self.s3_prefix}/{date}/{self.table}.csv'

        query = CopyCsvRedshiftOperator.copy_query.format(
            self.table,
            s3_path,
            self.arn,
            self.region,
            self.ignore_header
        )

        self.log.info(f'Copying data from S3 to Redshift ({self.table})')
        redshift.run(query)
        self.log.info(f'Copy completed ({self.table})')
