from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
     Loads the Fact table

     :param redshift_conn_id: Redshift connection ID
     :param fact_table : Fact table name
     :select_sql_statement : select statement to extract required data from stage tables
     """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 select_sql_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.select_sql_stmt = select_sql_statement

    def execute(self, context):
        self.log.info('LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from FACT table : {} in Redshift ".format(self.fact_table))
        redshift.run("DELETE FROM {}".format(self.fact_table))

        self.log.info("Inserting into Fact table : {} in Redshift ".format(self.fact_table))
        insert_statement = "INSERT INTO {} {}".format(self.fact_table,self.select_sql_stmt)

        redshift.run(insert_statement)
