
set -x;

if [ "$#" -ne 5 ]; then
  echo "Required 5 arguments"
  exit 1
fi

dataSetDir=$1
inputPath=$2
outputPath=$3
inputType=$4
libHdfsDir=$5

baseDir=`pwd`

# Create local lib dir
rm -rf lib/
hdfs dfs -copyToLocal $libHdfsDir lib
hdfs dfs -rm -R ${outputPath}

# Create classpath and libjars path variables
j="";
k="";
for i in `ls $baseDir/lib/`; do j=$baseDir/lib/$i:$j; k=$baseDir/lib/$i,$k; done; 
export HADOOP_CLASSPATH=`echo "${j::-1}"`
export HADOOP_LIBDIR=`echo "${k::-1}"`
echo $HADOOP_CLASSPATH;
echo $HADOOP_LIBDIR;

echo `whoami`

# Execute job
hadoop jar $baseDir/lib/tajawal-hotels-0.0.1-SNAPSHOT.jar com.tajawal.hotel.mapreduce.HotelDriver -libjars $HADOOP_LIBDIR -Dmapreduce.framework.name=yarn -Dmapred.reduce.tasks=1 -Ddataset.schema.path=${dataSetDir} ${inputPath} ${outputPath} ${inputType}
