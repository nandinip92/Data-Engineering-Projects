from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
     Copies JSON data from S3 to staging tables in Redshift data warehouse

     :param redshift_conn_id: Redshift connection ID
     :param aws_credentials_id: AWS credentials ID
     :param table: Target staging table in Redshift to copy data into
     :param s3_bucket: S3 bucket where JSON data resides
     :param s3_key: Path in S3 bucket where JSON data files reside
     :paran is_partitioned: Boolean value, True if data is partitioned by year and month like log_data
     :param json_format_option: Either a JSONPaths file or 'auto', mapping the data
         elements in the JSON source data to the columns in the target table
     :param region: AWS Region where the source data is located
     """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 is_partitioned = False,
                 region = "us-west-2",
                 json_format_option = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format_option = json_format_option
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.is_partitioned = is_partitioned

    def execute(self, context):
        self.log.info('StageToRedshiftOperator implementation')
        execution_date = context.get("execution_date")
        self.log.info("=====================>Execution Date :{}".format(execution_date))
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.is_partitioned :
            year = execution_date.year
            month = execution_date.month
            s3_path = s3_path+"/{}/{}/".format(year,month)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format_option
        )
        redshift.run(formatted_sql)
