


#/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/bin/copy_files_local_to_hdfs.ksh

printf "delete_hdfs files"
/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/bin/hdfs_output_paths.ksh

printf "Execute delete_hdfs is completed"


printf "Callin run_presc_pipeline.py"


spark3-submit --master yarn --num-executors 28 run_presc_pipeline.py

printf "End run_presc_pipeline.py"


printf "part2"

/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/bin/copy_files_hdfs_to_local.ksh 
/home/krishnanikhilmalisetty/Prescriber_analytics_pipeline/src/main/python/bin/copy_files_to_s3.ksh
