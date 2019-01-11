MYSQL_HOST="10.85.11.173"
MYSQL_USER_NAME="bi_user"
MYSQL_PASSWORD="EyjAN51Ar912"
MYSQL_DB_NAME="uat_bi"
MYSQL_ADJUST_MULTIAPP_TABLE="fact_adjust_data_multi_app"
MYSQL_ADJUST_TABLE="fact_adjust_data_multi_app_ext"
MYSQL_ADJUSt_MISSING_TABLE="fact_adjust_data_multi_app_missing_network"


mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -p$MYSQL_PASSWORD $MYSQL_DB_NAME -e "select * from $MYSQL_DB_NAME.$MYSQL_ADJUST_MULTIAPP_TABLE limit 2"

echo 'query 2'
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -p$MYSQL_PASSWORD $MYSQL_DB_NAME -e "
insert into fact_adjust_data_multi_app_missing_networks
select
network as network,
sum(installs) as installs,
sum(revenue_events) as revenue_events,
min(date) as min_date,
max(date) as max_date
from fact_adjust_data_multi_app group by network"
