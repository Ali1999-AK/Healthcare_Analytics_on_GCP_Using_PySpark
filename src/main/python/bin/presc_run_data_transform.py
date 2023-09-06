#city Report with Transform logic

# Data Frame city:
# City Name, State Name , Country Name, City Population
#Number of Zips, Prescriber Counts, Total Trx Counts


#1.Calculate the number of zips in each city
#2. calculate the number of distinct prescriber assigned for each city
#3. Calculate total TRX_CNT Prescibed for each city
# Do not report the city if no prescriber found


from pyspark.sql.functions import col,upper,countDistinct,sum,split,udf,dense_rank
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from udfs import column_split_cnt
import logging
import logging.config
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def city_report(df_city_sel,df_fact_sel):
     try:
          logger.info(f"Transform - city_report() is started...")
          df_city_split=df_city_sel.withColumn('zip_counts',column_split_cnt(df_city_sel.zips))
          df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state,df_fact_sel.presc_city).agg(
               countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
          df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_id == df_fact_grp.presc_state) & (df_city_split.city == df_fact_grp.presc_city), 'inner')
          df_city_join.show()
          df_city_final = df_city_join.select("city", "state_name", "country_name", "population", "zip_counts", "trx_counts","presc_counts")

     except Exception as exp:
          logger.error("Error in the method - city_report(). Please check the Stack Trace. " + str(exp), exc_info=True)
          raise
     else:
          logger.info("Transform - city_report() is completed...")
     return df_city_final

def top5_presc(df_fact_sel):
     try:
          df_fact_filter = df_fact_sel.filter((df_fact_sel.years_of_exp >=20) & (df_fact_sel.years_of_exp<=50))
          filter_cnt =df_fact_filter.count()
          print(f"{filter_cnt} is the fact_table count after applying filter")
          spec = Window.partitionBy("presc_state").orderBy(col("trx_cnt"))
          df_rank = df_fact_filter.withColumn("dense_rank",dense_rank().over(spec))
          df_rank.select('presc_id','presc_state','trx_cnt','dense_rank')
          df_presc_final = df_rank.filter(df_rank.dense_rank<=5).\
               select('presc_id','presc_name','presc_state','Country_name','years_of_exp','trx_cnt','total_day_supply','total_drug_cost','dense_rank')
     except Exception as exp:
          logger.error("Error in the method - top5_presc(). Please check the Stack Trace. " + str(exp), exc_info=True)
          raise
     else:
          logger.info("Transform - top5_presc() is completed...")
     return df_presc_final


