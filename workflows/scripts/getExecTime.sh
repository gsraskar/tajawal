if [ -f "/tmp/last_successful_$1" ]; then
  rm -f "/tmp/last_successful_$1"
fi
OUTPUT=`hdfs dfs -cat /user/oozie/workflows/tajawal/data/lastExecTimes/last_successful_$1.time`
if [ -z "$OUTPUT" ]; then
  echo "exec_time 1104537600" > "/tmp/last_successful_$1.time"
  hdfs dfs -copyFromLocal -f /tmp/last_successful_$1.time /user/oozie/workflows/tajawal/data/lastExecTimes/
  OUTPUT="exec_time 1104537600"
fi
echo $OUTPUT
