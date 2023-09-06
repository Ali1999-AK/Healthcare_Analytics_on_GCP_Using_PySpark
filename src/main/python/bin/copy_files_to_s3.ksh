#Devloped Date : Sep 3 2023
#Description: to copy input files from local to S3

############

#Var to hold unix script name

JOBNAME="copy_files_to_s3.ksh"


#Current date


date=$(date +%Y-%m-%d_%H:%M:%S)
subdir_date=$(date +%Y-%m-%d_%H:%M:%S)




#Define a Log File

LOGFILE="/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/logs/${JOBNAME}_${date}.log"

{
        echo "${JOBNAME} Started ...: $(date)"
        LOCAL_OUTPUT_PATH="/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/output"
        LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
        LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc
	for file in ${LOCAL_CITY_DIR}/*.*
	do
		aws s3 --profile external cp ${file} "s3://cityprescreportpipeline/dimension_city/$subdir_date/"
		echo "City File $file is pushed to s3"
	done
	for file in ${LOCAL_FACT_DIR}/*.*
        do
		aws s3 --profile external cp ${file} "s3://cityprescreportpipeline/presc/$subdir_date/"
                echo "City File $file is pushed to s3"
        done
        echo "${JOBNAME} is completed..: $(date)"

}>${LOGFILE} 2>&1
