#!/bin/bash
set +x
#hdfs dfs -copyFromLocal -f /tmp/oozie_wf_log.log /tmp/swf_resources/
#rm -f /tmp/oozie_wf_info.log
#hdfs dfs -copyFromLocal -f /tmp/oozie_wf_info.log /tmp/swf_resources/

SUBJECT="Test Email"
BODY="Hello"
echo "$BODY" | mailx -s "$SUBJECT" -r chaitanya.chapekar@extrapreneursindia.com anmol.nagpal@tajawal.com,chaitanya.chapekar@extrapreneursindia.com	


~                                                                                                                                               
~                                                                                                                                               
~                                                                                                                                               
~                      
