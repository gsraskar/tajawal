set -ex

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
echo $1${i//_/}
isEmpty=$(hdfs dfs -count $1${i//_/}*  | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "Given Path is empty"
    #Do some operation
else
hive -e "Drop table if exists ${3}.${i}_intermediate";
hive -e "create table if not exists ${3}.${i}_intermediate like ${3}.$i";
hive -e "set hive.mv.files.thread=0;load data inpath '$1${i//_/}*' into table ${3}.${i}_intermediate";
hive -e " Drop table ${3}.$i";
hive -e " Alter table ${3}.${i}_intermediate RENAME TO ${3}.$i";
fi

done

