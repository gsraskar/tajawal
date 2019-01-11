date_range_till=`date -d @$1 +%Y-%m-%d`
echo "date_range_since=${date_range_till}" > "/tmp/last_successful_$2.time"
echo "date_range_till=${date_range_till}" >> "/tmp/last_successful_$2.time"
hdfs dfs -copyFromLocal -f /tmp/last_successful_$2.time /user/oozie/workflows/tajawal/data/lastExecTimes
