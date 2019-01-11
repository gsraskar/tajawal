set -x

if [ "$#" -ne 3 ];
then
  echo "Provide arguments as,"
  echo "1. Output path"
  echo "2. Data set schema path"
  echo "3. Hive db name"
  exit 1
fi

for i in `hdfs dfs -cat $2 | grep data_set_name | cut -d\" -f4`
do

echo $i
echo "${i//_/}"
isEmpty=$(hdfs dfs -count $1${i}  | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "Given Path is empty"
    #Do some operation
else
hive -e "set hive.mv.files.thread=0; load data inpath '$1${i}/*' into table ${3}.${i}";

fi
done

