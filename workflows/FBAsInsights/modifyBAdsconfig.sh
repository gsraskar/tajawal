#!/bin/bash

#hdfs dfs -copyToLocal /user/oozie/tajawal_uat/workflows/FBAsInsights/FBAdsConfig.properties /tmp/FBAdsConfig.properties
#date_range_since=`date -d @$1 +%Y-%m-%d`
date_range_since=`date -d @$1 +%Y-%m-%d`
date_range_till=`date -d @$2 +%Y-%m-%d`

echo "date_range_since=${date_range_since}" > "/tmp/last_successful_$3.time"
echo "date_range_till=${date_range_till}" >> "/tmp/last_successful_$3.time"
#echo "exec_time=${date_range_till}" >> "/tmp/last_successful_facebook.time"
hdfs dfs -copyFromLocal -f /tmp/last_successful_$3.time /user/oozie/workflows/tajawal/data/lastExecTimes/


#echo $date_range_till = date -d $2 +%Y-%m-%d 
#echo "${breakdowns}" # SHOULD OUTPUT Erl
#set_config date_range_since $date_range_since # SETS THE NEW VALUE
#set_config date_range_till $date_range_till # SETS THE NEW VALUE
#hdfs dfs -copyFromLocal -f /tmp/FBAdsConfig.properties /user/oozie/tajawal_uat/workflows/FBAsInsights/

