#Load to bing_data table
echo '>>>> loading into bing'
hive -e "Set hive.execution.engine=mr;truncate table ${1}.${2}";
hive -e "set hive.mv.files.thread=0;load data inpath '${3}' into table ${1}.${2}";
