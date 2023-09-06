import get_all_variables as gav
from create_objects import get_spark_object
from presc_run_data_preprocessing import perform_data_clean
import validations
import sys
import logging
from subprocess import Popen,PIPE
import logging.config
import os
from presc_run_data_persist import data_persist,data_persist_sql
from presc_run_data_ingest import load_files
from presc_run_data_extraction import extract_files
from presc_run_data_transform import city_report,top5_presc



logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    try:
        logging.info("main() is started ...")
        ### Get Spark Object
        spark = get_spark_object(gav.envn,gav.appName)
        validations.get_curr_date(spark)
        
        #validation of spark object
        file_dir="PrescPipeline/staging/dimension_city"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_city = load_files(spark = spark, file_dir = file_dir, file_format =file_format , header =header, inferSchema = inferSchema)





        file_dir="PrescPipeline/staging/fact"
        proc = Popen(['hdfs','dfs','-ls','-C',file_dir],stdout=PIPE,stderr=PIPE)
        (out,err)=proc.communicate()
        if 'parquet' in str(out):
            file_format='parquet'
            header='NA'
            inferSchema='NA'
        elif 'csv' in str(out):
            file_format='csv'
            header=gav.header
            inferSchema=gav.inferSchema
            
        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)

        #validate the dataframe

        #validations.df_count(df_city)
        #validations.df_count(df_fact)


        #data preprocessing

        df_city_select,df_fact_select = perform_data_clean(df_city,df_fact)
        # validations.df_count(df_city_select)
        # validations.df_count(df_fact_select)
        # validations.df_print_schema(df_fact_select)
        df_city_final = city_report(df_city_select,df_fact_select)
        df_presc_final = top5_presc(df_fact_select)
        validations.df_count(df_city_final)
        validations.df_count(df_presc_final)
        validations.df_print_schema(df_city_final)
        validations.df_print_schema(df_presc_final)

        CITY_PATH = gav.output_city
        extract_files(df_city_final,'json',CITY_PATH,1,False,'bzip2')
        PRESC_PATH = gav.output_fact
        extract_files(df_presc_final,'orc',PRESC_PATH,2,False,'snappy')
        #data_persist(spark = spark, df = df_city_final ,dfName = 'df_city_final' ,partitionBy = 'date',mode='append')
        #data_persist(spark = spark, df = df_presc_final,dfName = 'df_presc_final',partitionBy = 'date',mode='append')
        
        data_persist_sql(spark=spark, df=df_city_final, dfName='df_city_final', url="jdbc:postgresql://localhost:6432/prespipeline", driver="org.postgresql.Driver", dbtable='df_city_final', mode="append", user="sparkuser1", password="user123")
        data_persist_sql(spark=spark, df=df_presc_final, dfName='df_presc_final', url="jdbc:postgresql://localhost:6432/prespipeline", driver="org.postgresql.Driver", dbtable='df_presc_final', mode="append", user="sparkuser1", password="user123")
        
        logging.info('presc pipeline is completed..')


    except Exception as asp:
        logging.error("Error in Main method,Check the stack Trace of respective module. "+str(asp),exc_info=True)

        sys.exit(1)

        #error handling and logging Done


if __name__ == '__main__':
    logging.info("Presc Pipeline started ..")
    main()

