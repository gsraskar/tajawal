#!/bin/bash
set -x
listFilePath=$1
echo $listFilePath
whoami
while IFS=';' read -r hiveTable hiveDb mysqlTable mysqlDB delimiter mysqlUser mysqlHost mysqlPassword hiveWarehousePath
 do
if [ ! -z "$hiveTable" ]
  then
    # do stuff with "$input1" and "$input2"
 echo $mysqlUser 
 TEMP_TABLE_NAME=$mysqlTable"_intermediate"
 echo $TEMP_TABLE_NAME
 echo "EXECUTING DATA EXPORT!"
  mysql -u$mysqlUser -p$mysqlPassword -e 'SET GLOBAL max_allowed_packet=173741824' -h$mysqlHost
  mysql -u$mysqlUser -p$mysqlPassword $mysqlDB -e 'drop table if exists '$TEMP_TABLE_NAME -h$mysqlHost
  mysql -u$mysqlUser -p$mysqlPassword $mysqlDB -e "create table if not exists $TEMP_TABLE_NAME like $mysqlTable" -h$mysqlHost
echo "After mysql connection"
 sqoop export -connect jdbc:mysql://$mysqlHost:3306/$mysqlDB --username $mysqlUser --password $mysqlPassword --table $TEMP_TABLE_NAME -export-dir $hiveWarehousePath'/'$hiveDb'.db/'$hiveTable --input-fields-terminated-by $delimiter --input-null-string  "\\\N" --input-null-non-string "\\\N"

echo $test
echo "pre post sqoop"
 mysql -u$mysqlUser -p$mysqlPassword $mysqlDB -e 'DROP TABLE IF EXISTS '$mysqlTable -h$mysqlHost
 mysql -u$mysqlUser -p$mysqlPassword $mysqlDB -e 'RENAME TABLE '$TEMP_TABLE_NAME' TO '$mysqlTable -h$mysqlHost
 echo "End of data exporting!"
fi
 done < <(hdfs dfs -cat  $listFilePath)
