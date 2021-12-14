url=""

current_time=`date +%d-%m-%Y_%H:%M:%S`

curl url >> ./data_$current_time

hdfs dfs -put ./data_$current_time /data/grp5

rm ./data_$current_time
