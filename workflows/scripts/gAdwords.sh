#$1 lib
#$2 function
#$3 property file path
#$4adwords prop
#$5 fromDate
#$6 toDate

set -x

echo `ifconfig`;
echo `hostname`;
if [ "$#" -ne 6 ]; then
   echo "missing arguments"
fi
if [ -d "/tmp/gAdwordsExecDir" ]; then
   rm -rf /tmp/gAdwordsExecDir
fi
rm -rf /tmp/gAdwordsExecDir
mkdir /tmp/gAdwordsExecDir
chmod -R 777 /tmp/gAdwordsExecDir
cd /tmp/gAdwordsExecDir
hdfs dfs -copyToLocal $1 .
hdfs dfs -copyToLocal $3 .
filename1=`basename $3`
hdfs dfs -copyToLocal $4 .
filename2=`basename $4`
echo $filename2
echo $5
echo $6
current_path=`pwd`;
echo $current_path/$filename2
echo "adPropertyPath=$current_path/$filename2" >> $filename1
echo "fromDate=$5" >> $filename1
echo "toDate=$6" >> $filename1

java -cp lib/googleAdwordsSDK-0.0.1-SNAPSHOT.jar:lib/* sdf.googleAdwordsSDK.GoogleAdwordsCLI --function $2 --properties $filename1
   
