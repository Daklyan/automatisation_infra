#!/bin/sh

current_hour=`date +%Hh%Mm%Ss`
today=`date +%Y-%m-%d`
yesterday=$(date -d "yesterday" '+%Y-%m-%d')

url=https://opendata.paris.fr/api/datasets/1.0/search/?q=data_processed%3C${today}T$current_hour%20and%20data_processed%3E${yesterday}T$current_hour
curl $url >> ./data_${today}_$current_hour

hdfs dfs -put ./data_${today}_$current_hour /data/grp5

rm ./data_${today}_$current_hour
