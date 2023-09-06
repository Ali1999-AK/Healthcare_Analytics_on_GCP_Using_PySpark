import logging
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger('validation')

def get_curr_date(spark):
        try:
                opDf = spark.sql(""" select current_date """)
                logging.info("Validate the spark object by printing Current - "+str(opDf.collect()))
        except NameError as exp:
                logging.error("NameError in get_curr_date method,Trace back to the Stack"+str(exp),exc_info=True)
                raise
        except Exception as exp:
                logging.error("Error in get_curr_date method,Trace back to the Stack"+str(exp),exc_info=True)
                raise
        else:
                logging.info("Spark object is validated")

def df_count(df):
        try:
                logging.info(f"df_count has started")
                df_cnt = df.count()
                logging.info(f"{df_cnt} is the count.")
                df_pandas = df.limit(10).toPandas()
                logging.info('\n \t'+df_pandas.to_string(index=False))

        except Exception as exp:
                logging.error("Error in the method df_count,check the stack trace"+str(exp),exc_info=True)
                raise
        else:
                logging.info("DataFrame validation is done.")


def df_print_schema(df):
        try:
                logging.info("Schema Validation Has started ..")
                sch = df.schema.fields
                for i in sch:
                        logging.info(f"\t{i}")
        except Exception as exp:
                logging.error("Error in Method, Please Check the Stack trace "+str(exp),exc_info=True)
                raise
        else:
                logging.info("DataFrame Schema validation is completed")
