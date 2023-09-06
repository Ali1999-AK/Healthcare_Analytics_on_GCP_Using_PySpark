###
#Devloped By : Ali K
#Devloped Date : Sep 1 2023
#Description: to copy input files from local to HDFS

############

#Var to hold unix script name

JOBNAME="copy_files_local_to_hdfs.ksh"


#Current date


date=$(date +%Y-%m-%d_%H:%M:%S)


#Define a Log File

LOGFILE="/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/logs/copy_files_local_to_hdfs_${data}.log"

{
	echo "${JOBNAME} Started.... $(date)"
	LOCAL_STAGING_PATH=/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/staging
	LOCAL_CITY_DIR=/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/staging/dimension_city
	LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact


	HDFS_STAGING_PATH=PrescPipeline/staging
	HDFS_CITY_DIR=${HDFS_STAGING_PATH}/dimension_city
	HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact
	hdfs dfs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
	hdfs dfs -put -f ${LOCAL_FACT_DIR}/* ${HDFS_FACT_DIR}/
	echo "${JOBNAME} is Completed... $(date)"
} > ${LOGFILE}  2>&1 
