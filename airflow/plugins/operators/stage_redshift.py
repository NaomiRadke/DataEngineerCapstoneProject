from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION AS '{}'
            FORMAT AS '{}'

        """

  
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table = "",
                 s3_bucket="",
                 s3_prefix="",
                 options="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.options = options
        self.region = region
        


    def execute(self, context):
        self.log.info('Connecting to Redshift')
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_prefix.format(**context)
        path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.options
        )
        redshift.run(formatted_sql)





