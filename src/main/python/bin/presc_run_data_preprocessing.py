import logging
import logging.config
from pyspark.sql.functions import upper,lit,col,concat_ws,regexp_extract,count,when,isnan, coalesce, round, avg
from pyspark.sql.window import Window


logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def perform_data_clean(df1,df2):
    #clean the city df like batman and send the lower case city,state, and country to upper case - IM VENGEANCE!!
    try:
        logging.info("perform_data_clean has started df_city")
        df_city_sel = df1.select(upper(df1.city).alias('city'),df1.state_id,
                                 upper(df1.state_name).alias('state_name'),upper(df1.county_name).alias("country_name"),
                                 df1.population,df1.zips)

        #cleaning df2 which is fact table
        #SELECT Only Required columns
        #Rename the columns

        logging.info("perform_data_clean has started for df_fact")
        df_fact_sel = df2.select(df2.npi.alias('presc_id'),
                                 df2.nppes_provider_last_org_name.alias('presc_lname'),\
                                 df2.nppes_provider_first_name.alias('presc_fname'),\
                                 df2.nppes_provider_city.alias('presc_city'),\
                                 df2.nppes_provider_state.alias('presc_state'),\
                                 df2.specialty_description.alias('presc_spclt'),\
                                 df2.years_of_exp,\
                                 df2.drug_name,df2.total_claim_count.alias("trx_cnt"),\
                                 df2.total_day_supply,\
                                 df2.total_drug_cost
                                 )
        #Add country field USA lit is liternal value
        df_fact_sel = df_fact_sel.withColumn("Country_name",lit("USA"))

        df_fact_sel = df_fact_sel.withColumn("presc_name",concat_ws(" ", df_fact_sel["presc_fname"], df_fact_sel["presc_lname"]))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp",regexp_extract(col("years_of_exp"),pattern,idx))
        df_fact_sel = df_fact_sel.withColumn("presc_id",regexp_extract(col("presc_id"),pattern,idx))
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", df_fact_sel["years_of_exp"].cast("int"))

        #Clean years_of_exp filed
        #years_of_exp(str) to Number
        #combine First Name and last name
        #check and clean all the Null/Nan values
        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_fact_sel.columns]).show()

        #delete the rows where presc_id,drug_name is null and or nan

        df_fact_sel = df_fact_sel.dropna(subset='presc_id')
        df_fact_sel = df_fact_sel.dropna(subset='drug_name')


        #impute  Tex_Cnt Where it is null as avg of trx_cnt for the prescriber

        windowSpec = Window.partitionBy('presc_id')
        df_fact_sel=df_fact_sel.withColumn('trx_cnt',coalesce('trx_cnt',round(avg("trx_cnt").over(windowSpec))))

        df_fact_sel=df_fact_sel.withColumn('trx_cnt',col('trx_cnt').cast('integer'))

        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_fact_sel.columns]).show()





    except Exception as exp:
        logging.error("Error in the method perform_data_clean, please check the stack Trace"+str(exp),exc_info=True)
        raise
    else:
        logging.info("perform_data_clean is done")

    return df_city_sel,df_fact_sel
