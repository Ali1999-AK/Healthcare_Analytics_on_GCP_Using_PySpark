#Devloped By : Ali K
#Devloped Date : Sep 2 2023
#Description: to copy input files from local to HDFS

############

#Var to hold unix script name

JOBNAME="copy_files_hdfs_to_local.ksh"


#Current date


date=$(date +%Y-%m-%d_%H:%M:%S)


#Define a Log File

LOGFILE="/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/logs/${JOBNAME}_${date}.log"

{
	echo "${JOBNAME} Started ...: $(date)"
	LOCAL_OUTPUT_PATH="/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/output"
	LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city 
	LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

	HDFS_OUTPUT_PATH=PrescPipeline/output
	HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/dimension_city
	HDFS_FACT_DIR=${HDFS_OUTPUT_PATH}/fact

	hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
	hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/

	echo "${JOBNAME} is completed..: $(date)"

}>${LOGFILE} 2>&1
