USER_NAME=$1
PASSWORD=$2
DB_NAME=$3
TABLE_NAME=$4
#TEMP_TABLE_POSTFIX="_intermediate"
#TEMP_TABLE_NAME=$TABLE_NAME$TEMP_TABLE_POSTFIX
TEMP_TABLE_NAME=$5
HOST=$6

#mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'create table if not exists '$TABLE_NAME' (month varchar(200), hour_of_day varchar(200), app_name varchar(200), screen_id varchar(200), device_name varchar(200), os_name varchar(200), os_version varchar(200), screen_resolution varchar(200), screen_size varchar(200), screen_view_count bigint, uniq_users bigint, time_spent double )' -h$HOST
n=0
until [ $n -ge 10 ]
do
  mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'DROP TABLE IF EXISTS '$TABLE_NAME -h$HOST && break
  n=$[$n+1]
  sleep 10
done
mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'RENAME TABLE '$TEMP_TABLE_NAME' TO '$TABLE_NAME -h$HOST

#mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'REPLACE INTO '$TABLE_NAME' SELECT * FROM '$TEMP_TABLE_NAME -h$HOST
#mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'drop table if exists '$TEMP_TABLE_NAME -h$HOST


