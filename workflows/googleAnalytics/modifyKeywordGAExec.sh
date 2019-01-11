#!/bin/bash

#hdfs dfs -copyToLocal /user/oozie/tajawal_uat/workflows/FBAsInsights/FBAdsConfig.properties /tmp/FBAdsConfig.properties
#date_range_since=`date -d @$1 +%Y-%m-%d`
set -ex
fromDate=`date +%Y-%m-%d`
tmptoDate=`date -d @$2 +%Y-%m-%d`
#toDate=$(date -d "${tmptoDate} -1 days" +%Y-%m-%d)
backDate=`date -d "${fromDate} -30 days" +%Y-%m-%d`
echo "start_date=${backDate}" > "/tmp/last_successful_$3.time"
echo "end_date=${tmptoDate}" >> "/tmp/last_successful_$3.time"
#echo "exec_time=${date_range_till}" >> "/tmp/last_successful_facebook.time"
hdfs dfs -copyFromLocal -f /tmp/last_successful_$3.time /user/oozie/workflows/tajawal/data/lastExecTimes



