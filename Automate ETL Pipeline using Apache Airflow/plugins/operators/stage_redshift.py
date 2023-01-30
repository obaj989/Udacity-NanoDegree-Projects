from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info(f"Deleted any existing data from {self.table}")
        self.s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        query = f"COPY {self.table} FROM '{s3_path}' " \
                f"ACCESS_KEY_ID '{credentials.access_key}' " \
                f"SECRET_ACCESS_KEY '{credentials.secret_key}' " \
                f"REGION 'us-west-2' " 
        query += f"FORMAT AS JSON '{self.json_path}'" if self.json_path else " JSON 'auto'"
        redshift.run(query)
        self.log.info(f"Copied data from S3 to {self.table}")