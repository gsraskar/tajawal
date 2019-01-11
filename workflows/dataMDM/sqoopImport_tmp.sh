echo '>>> Sqoop Export Started'

MYSQL_HOST="10.85.11.173"
MYSQL_USER_NAME="bi_user"
MYSQL_PASSWORD="EyjAN51Ar912"
MYSQL_DB_NAME="uat_bi"
MYSQL_TABLE_NAME="dim_hotel_mdm"
HIVE_DB_NAME="tajawal_uat_bi"
HIVE_TABLE_NAME="dim_hotel_mdm"
HIVE_TEMP_TABLE_NAME="dim_hotel_mdm_intermediate"
SQOOP_TARGET_DIR="/tmp/dim_hotel_mdm"
DELEMETER="\t"

#MYSQL_HOST=$1
#MYSQL_USER_NAME=$2
#MYSQL_PASSWORD=$3
#MYSQL_DB_NAME=$4
#MYSQL_TABLE_NAME=$5
#HIVE_DB_NAME=$6
#HIVE_TABLE_NAME=$7
#HIVE_TEMP_TABLE_NAME=$8
#SQOOP_TARGET_DIR=$9
#DELEMETER=${10}


echo '>>> Drop Exiating hive Table'$HIVE_DB_NAME.$HIVE_TEMP_TABLE_NAME
rm -rf $SQOOP_TARGET_DIR
hive -e "Set hive.execution.engine=mr;drop table if exists $HIVE_DB_NAME.$HIVE_TEMP_TABLE_NAME";
echo '>>> Create Temp table'$HIVE_DB_NAME.$HIVE_TEMP_TABLE_NAME
hive -e "Set hive.execution.engine=mr;create table if not exists $HIVE_DB_NAME.$HIVE_TEMP_TABLE_NAME like $HIVE_DB_NAME.$HIVE_TABLE_NAME";
echo '>>> Import into Temp Hive Table :-'$HIVE_TEMP_TABLE_NAME
echo '>>> Delemeter :-'$DELEMETER

sqoop import --connect jdbc:mysql://$MYSQL_HOST:3306/$MYSQL_DB_NAME  -m 1 --username $MYSQL_USER_NAME --password $MYSQL_PASSWORD --table $MYSQL_TABLE_NAME  --target-dir $SQOOP_TARGET_DIR --fields-terminated-by "$DELEMETER" --hive-import --hive-overwrite --hive-table $HIVE_DB_NAME.$HIVE_TEMP_TABLE_NAME

echo '>>> Sqoop Import complete'
echo '>>> drop '$HIVE_DB_NAME.$HIVE_TABLE_NAME
echo '>>> rename '$HIVE_DB_NAME.$HIVE_TEMP_TABLE_NAME

hive -e "Set hive.execution.engine=mr;drop table $HIVE_DB_NAME.$HIVE_TABLE_NAME";
hive -e "Set hive.execution.engine=mr;alter table $HIVE_DB_NAME.$HIVE_TEMP_TABLE_NAME rename to  $HIVE_DB_NAME.$HIVE_TABLE_NAME";
