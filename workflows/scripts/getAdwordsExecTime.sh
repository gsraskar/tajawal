if [ -f "/tmp/last_successful_$1" ]; then
  rm -f "/tmp/last_successful_$1"
fi
OUTPUT=`hdfs dfs -cat /user/oozie/workflows/tajawal/data/lastExecTimes/last_successful_$1.time`
if [ -z "$OUTPUT" ]; then
  echo "date_range_since=0 date_range_till=0" > "/tmp/last_successful_$1.time"
  hdfs dfs -copyFromLocal -f /tmp/last_successful_$1.time /user/oozie/workflows/tajawal/data/lastExecTimes/
  OUTPUT="fromDate=0 toDate=0"
fi
echo $OUTPUT | awk {'print $1'}
echo $OUTPUT | awk {'print $2'}
