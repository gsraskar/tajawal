#!/bin/bash

start_date=`date -d @$1 +%Y-%m-%d`
end_date=`date -d @$2 +%Y-%m-%d`

echo "start_date=${start_date}" > "/tmp/last_successful_$3.time"
echo "end_date=${end_date}" >> "/tmp/last_successful_$3.time"
hdfs dfs -copyFromLocal -f /tmp/last_successful_$3.time /user/oozie/workflows/tajawal/data/lastExecTimes


#echo $date_range_till = date -d $2 +%Y-%m-%d 
#echo "${breakdowns}" # SHOULD OUTPUT Erl
#set_config date_range_since $date_range_since # SETS THE NEW VALUE
#set_config date_range_till $date_range_till # SETS THE NEW VALUE
#hdfs dfs -copyFromLocal -f /tmp/FBAdsConfig.properties /user/oozie/tajawal_uat/workflows/FBAsInsights/

