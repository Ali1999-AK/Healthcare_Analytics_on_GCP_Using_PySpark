import logging
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def load_files(spark,file_dir,file_format,header,inferSchema):
    try:
        logging.info("The Load_files() method is started..")
        if file_format=='parquet':
            df = spark.read.format(file_format).load(file_dir)

        elif file_format == 'csv':
            df = spark.read.\
                format(file_format).\
                options(header=header).\
                options(inferSchema=inferSchema).\
                load(file_dir)
    except Exception as exp:
        logging.error("Error in the method -load_file(),check the stack trace"+str(exp),exc_info=True)
        raise
    else:
        logging.info(f"{file_dir} is loaded to the data frame")

    return df
