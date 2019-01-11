#MYSQL_HOST="10.85.11.173"
#MYSQL_USER_NAME="bi_user"
#MYSQL_PASSWORD="EyjAN51Ar912"
#MYSQL_DB_NAME="uat_bi"
#MYSQL_ADJUST_MULTIAPP_TABLE="fact_adjust_data_multi_app"
#MYSQL_ADJUST_EXT_TABLE="fact_adjust_data_multi_app_ext"
#MYSQL_ADJUSt_MISSING_TABLE="fact_adjust_data_multi_app_missing_networks"

MYSQL_HOST=$1
MYSQL_USER_NAME=$2
MYSQL_PASSWORD=$3
MYSQL_DB_NAME=$4
MYSQL_ADJUST_MULTIAPP_TABLE=$5
MYSQL_ADJUST_EXT_TABLE=$6
MYSQL_ADJUSt_MISSING_TABLE=$7

echo '>>> fact_adjust_data_multi_app_ext deleting'
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -p$MYSQL_PASSWORD $MYSQL_DB_NAME -e "truncate table $MYSQL_DB_NAME.$MYSQL_ADJUST_EXT_TABLE"
echo '>>> fact_adjust_data_multi_app_ext deleted'



echo '>>> fact_adjust_data_multi_app_ext exec strted'
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -p$MYSQL_PASSWORD $MYSQL_DB_NAME -e "insert into $MYSQL_DB_NAME.$MYSQL_ADJUST_EXT_TABLE 
 		select replace(network, ' ', '') as network, 
		sum(installs) as total_installs, sum(revenue_events) as total_revenue_events,
		sum(revenue) as revenue, 
 		min(date) as start_date, max(date) as latest_date 
 		from bi.$MYSQL_ADJUST_MULTIAPP_TABLE
 		group by replace(network, ' ', '')"

echo '>>> fact_adjust_data_multi_app_ext exec END'
	
echo '>>> fact_adjust_data_multi_app_missing_networks deleating'
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -p$MYSQL_PASSWORD $MYSQL_DB_NAME -e "truncate table $MYSQL_DB_NAME.$MYSQL_ADJUSt_MISSING_TABLE"
echo '>>> fact_adjust_data_multi_app_missing_networks deleated'

echo '>>> fact_adjust_data_multi_app_missing_networks exec started'
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -p$MYSQL_PASSWORD $MYSQL_DB_NAME -e "insert into  $MYSQL_DB_NAME.$MYSQL_ADJUSt_MISSING_TABLE
		select
		network as network,
		sum(installs) as installs,
		sum(revenue_events) as revenue_events,
		min(date) as min_date,
		max(date) as max_date
		from bi.$MYSQL_ADJUST_MULTIAPP_TABLE group by network"
echo '>>> fact_adjust_data_multi_app_missing_networks exec END'
