from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
     Loads the Dimension tables

     :param redshift_conn_id: Redshift connection ID
     :param fact_table : Fact table name
     :param insert_mode : parameter for the mode of insertion
     :param primary_key : If given mode is 'insert_delete' then the primary key mist be specified
     """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table="",
                 select_sql_statement = "",
                 insert_mode = "append",
                 primary_key = None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table = dim_table
        self.select_sql_stmt = select_sql_statement
        self.insert_mode = insert_mode
        self.pk = primary_key

    def execute(self, context):
        self.log.info('LoadDimensionOperator implementation')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "append":
            self.log.info("Inserting into Dimension table : {} in Redshift [Append mode] ".format(self.dim_table))
            insert_statement = "INSERT INTO {} {}".format(self.dim_table,self.select_sql_stmt)
            redshift.run(insert_statement)
            return
        elif self.insert_mode == "insert_delete":
            if not self.pk:
                raise ValueError("Primary Key must be specified for 'insert-delete' mode")

            self.log.info("Insertion for Dimension table : {} in Redshift [INSERT-DELETE mode] ".format(self.dim_table))
            self.log.info("Creating a temporary table ST_{} and inserting data into it...".format(self.dim_table))
            temp_stage_table = """
                        CREATE TABLE IF NOT EXISTS ST_{0}  (LIKE {0});
                        INSERT INTO ST_{0} {1};
                    """.format(self.dim_table,self.select_sql_stmt)

            self.log.info("Deleting data from {} table...".format(self.dim_table))
            del_from_dim_table = """
                                    DELETE FROM {0}
                                    USING ST_{0}
                                    WHERE {0}.{1} = ST_{0}.{1};
                                """.format(self.dim_table,self.pk)

            self.log.info("Inserting records from ST_{0} data to {0} table".format(self.dim_table))

            insert_statement ="""INSERT INTO {0}
                                SELECT * FROM ST_{0};
                            """.format(self.dim_table)
            redshift.run(temp_stage_table)
            redshift.run(del_from_dim_table)
            redshift.run(insert_statement)

            self.log.info("Truncating ST_{0}".format(self.dim_table))
            truncate_st_table = f"TRUNCATE TABLE ST_{self.dim_table}"
            redshift.run(truncate_st_table)
            return
            #sql_statements = """
            #            CREATE TEMP TABLE ST_{0}  (LIKE {0});
            #            INSERT INTO ST_{0} {1};

            #            DELETE FROM {0}
            #            USING ST_{0}
            #            WHERE {0}.{2} = ST_{0}.{2};

            #            INSERT INTO {0} SELECT * FROM ST_{0};
            #""".format(self.dim_table,self.select_sql_stmt,self.pk)


        else:
            raise ValueError("insert_mode is given as {}. Only 'append' and 'insert-delete' are accepted".format(self.insert_mode))


        redshift.run(insert_statement)
