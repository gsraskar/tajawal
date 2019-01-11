if [ -f "/tmp/last_successful_$1" ]; then
  rm -f "/tmp/last_successful_$1"
fi
OUTPUT=`hdfs dfs -cat /user/oozie/workflows/tajawal/data/lastExecTimes/last_successful_$1.time`
if [ -z "$OUTPUT" ]; then
  echo "start_date=0 end_date=0" > "/tmp/last_successful_$1.time" >> /tmp/getExec.log 2>&1
  hdfs dfs -copyFromLocal -f /tmp/last_successful_$1.time /user/oozie/workflows/tajawal/data/lastExecTimes/
  OUTPUT="start_date=0 end_date=0"
fi
echo $OUTPUT | awk {'print $1'}
echo $OUTPUT | awk {'print $2'}
