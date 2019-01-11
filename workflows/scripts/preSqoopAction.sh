USER_NAME=$1
PASSWORD=$2
DB_NAME=$3
TABLE_NAME=$4
#TEMP_TABLE_POSTFIX="_intermediate"
#TEMP_TABLE_NAME=$TABLE_NAME$TEMP_TABLE_POSTFIX
TEMP_TABLE_NAME=$5
HOST=$6

set -x

echo "EXECUTING DATA UPLOAD!"
mysql -u$USER_NAME -p$PASSWORD -e 'SET GLOBAL max_allowed_packet=173741824' -h$HOST
#mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'create database if not exists '$DB_NAME -h$HOST
mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'drop table if exists '$TEMP_TABLE_NAME -h$HOST
mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e "create table if not exists $TEMP_TABLE_NAME like $TABLE_NAME" -h$HOST
#mysql -u$USER_NAME -p$PASSWORD $DB_NAME -e 'create table if not exists '$TEMP_TABLE_NAME' (month varchar(200), hour_of_day varchar(200), app_name varchar(200), screen_id varchar(200), device_name varchar(200), os_name varchar(200), os_version varchar(200), screen_resolution varchar(200), screen_size varchar(200), screen_view_count bigint, uniq_users bigint, time_spent double )' -h$HOST


