set -ex

if [ "$#" -ne 4 ];
then
  echo "Provide arguments as,"
  echo "1. hive db name"
  echo "2. table name"
  echo "3. loadpath"
  echo "4. loadfilename"

  exit 1
fi

hive -e "set hive.mv.files.thread=0";
#hive -e "load data inpath '$1${i//_/}*' into table ${3}.${i}_intermediate";
hive -e "set hive.mv.files.thread=0;load data inpath '$3$4*' into table $1.$2";

