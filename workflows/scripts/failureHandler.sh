

#!/bin/bash
set +x
EMAIL_RECEIVER=$(echo $1 | sed 's/,/ /g')
WF_NAME=$2
WF_STATUS=$3
CURRENT_TIME=`date`
FAILED_WF_ID=$4
OOZIE_SERVER="http://ip-10-85-14-134.eu-west-1.compute.internal:11000/oozie"
#rm -f /tmp/oozie_wf_log.log
oozie job -log $FAILED_WF_ID -oozie $OOZIE_SERVER >> /tmp/oozie_wf_log.log
#hdfs dfs -copyFromLocal -f /tmp/oozie_wf_log.log /tmp/swf_resources/
#rm -f /tmp/oozie_wf_info.log
oozie job -info $FAILED_WF_ID -oozie $OOZIE_SERVER >> /tmp/oozie_wf_info.log
#hdfs dfs -copyFromLocal -f /tmp/oozie_wf_info.log /tmp/swf_resources/

SUBJECT="$WF_NAME, $CURRENT_TIME, $WF_STATUS as failed"
#hdfs dfs -copyToLocal /tmp/swf_resources/oozie_wf_info.log /tmp
#hdfs dfs -copyToLocal /tmp/swf_resources/oozie_wf_log.log /tmp
BODY=$(</tmp/oozie_wf_info.log)
#echo "$BODY" | mailx -s "$SUBJECT" -a /tmp/oozie_wf_log.log -r alert@dataeaze.io $EMAIL_RECEIVER
rm -f /tmp/oozie_wf_info.log
rm -f /tmp/oozie_wf_log.log


