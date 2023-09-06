import logging
import logging.config


logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def extract_files(df,format,filepath,split_no,headerReq,compressionType):
    try:
        logging.info(f"Extraction -extract_files is started ...")
        df.coalesce(split_no) \
          .write \
          .format(format) \
          .save(filepath, header=headerReq, compression=compressionType)
    except Exception as exp:
        logger.error("Error in the method -extract(). Please check the stack Trace. "+str(exp),exc_info=True)
    else:
        logger.info("Extraction is done..")
