#$1 <src file path>
#$2 <dest hdfs path>
echo $1 $2 >> /tmp/kedarlog3

echo "step 1"
mkdir -p $1


if $(hadoop fs -test -d $2) ;
then
        hdfs dfs -rm -r $2 2>&1
fi

#echo "step 2"
#hdfs dfs -mkdir $2/
whoami
hdfs dfs -copyFromLocal $1 $2 2>&1
