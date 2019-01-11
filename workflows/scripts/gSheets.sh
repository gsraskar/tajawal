#$1 hdfs creds dir
#$2 client secret json
#$3 sheets params
#$4 libs
if [ "$#" -ne 4 ]; then
   echo "missing arguments"
fi
if [ -d "/tmp/gSheetExecDir" ]; then
   rm -rf /tmp/gSheetExecDir
fi
mkdir  /tmp/gSheetExecDir
chmod -R 777 /tmp/gSheetExecDir
mkdir  /tmp/gSheetExecDir/lib/
cd /tmp/gSheetExecDir
echo $4
hdfs dfs -copyToLocal $1 .
hdfs dfs -copyToLocal $4 .
java -cp lib/googlesheet-api-connector-1.0.0-SNAPSHOT.jar:lib/* com.tajawal.googlesheet.sdk.api.connector.GSheetCLI -sheets $3 -cj $2
baseName=`basename $1`
dirName=`dirname $1`
hdfs dfs -copyFromLocal -f $baseName $dirName
