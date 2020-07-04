from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
     Checks the quality of the data fo all tables

     :param redshift_conn_id: Redshift connection ID
     :param fact_table : Fact table name
     :param dim_tables : List of all dimension tables
     :param tests : list of dicts with test cases and their expected values
     """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table = "",
                 dim_tables = [],
                 tests = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_tables = dim_tables
        self.fact_table = fact_table
        self.tests = tests

    def execute(self, context):
        self.log.info('DataQualityOperator Implementation')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.tests:
            for t in self.tests:
                test = t["test"]
                value = t["expected"]
                records = redshift_hook.get_records(test)

                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed.\n{test} returned no results")

                num_records = records[0][0]
                if num_records != value:
                    raise ValueError(f"Data quality check failed.\n{test} returned :{num_records} expected :{value}")

                self.log.info(f"Data quality {test} check passed with {num_records} records")
