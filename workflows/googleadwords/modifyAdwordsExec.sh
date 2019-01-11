#!/bin/bash

#hdfs dfs -copyToLocal /user/oozie/tajawal_uat/workflows/FBAsInsights/FBAdsConfig.properties /tmp/FBAdsConfig.properties
#date_range_since=`date -d @$1 +%Y-%m-%d`
fromDate=`date -d @$1 +%Y%m%d`
toDate1=`date -d @$2 +%Y%m%d`
toDate=$(date -d "${toDate1} -1 days" +%Y%m%d)

echo "fromDate=${fromDate}" > "/tmp/last_successful_$3.time"
echo "toDate=${toDate}" >> "/tmp/last_successful_$3.time"
#echo "exec_time=${date_range_till}" >> "/tmp/last_successful_facebook.time"
hdfs dfs -copyFromLocal -f /tmp/last_successful_$3.time /user/oozie/workflows/tajawal/data/lastExecTimes



