#Devloped By : Ali K
#Devloped Date : Sep 2 2023
#Description: to copy input files from local to HDFS

############


JOBNAME='hdfs_output_paths.ksh'


date=$(date +%Y-%m-%d_%H:%M:%S)


#Define a Log File

LOGFILE="/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/logs/${JOBNAME}_${date}.log"
{
echo "${JOBNAME} Started...: $(date)"

CITY_PATH=PrescPipeline/output/dimension_city

hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ]
	then
		echo "The HDFS Output directory $CITY_PATH is available, Proceed to delete"
		hdfs dfs -rm -r -f $CITY_PATH
		echo "The HDFS Output directory $CITY_PATH is deleted extraction."		
	fi

FACT_PATH=PrescPipeline/output/fact

hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ]
        then
                echo "The HDFS Output directory $FACT_PATH is available, Proceed to delete"
                hdfs dfs -rm -r -f $FACT_PATH
		echo "The HDFS Output directory $FACT_PATH is deleted extraction."
fi
echo "${JOBNAME} is completed... : $(date)"
} > ${LOGFILE}  2>&1

