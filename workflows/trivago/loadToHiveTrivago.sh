#Load to trivago_hotel_data
echo '>>>> loading into hotel'
hive -e "Set hive.execution.engine=mr;truncate table ${1}.${2}";
hive -e "set hive.mv.files.thread=0;load data inpath '${3}' into table ${1}.${2}";
echo '>>>> loades into hotel'
#TRIVAGO POS DATA LOAD
hdfs dfs -cp -f $4* $5
hive -e "truncate table ${1}.${6}";

input_path_POS=$5
echo '>>>>>>>>>>>>>>>>>>>>>TRIVAGO POS DATA LOAD>>>>>>>>>>>>>>>>>>>>>>>>>>'

hdfs dfs -chown anonymous $input_path_POS'*'

for filename in `hadoop fs -ls $input_path_POS  | awk '{print $NF}' | grep .csv$ | tr '\n' ' '`

do
#echo $filename; 

length="${#filename}"
from=$(expr $length - 12)
partition_date=${filename:$from:8}
from_group=$(expr $length - 28)
partition_group=${filename:$from_group:4}
partition_group_name='NA'

 if [ "$partition_group" == '2356' ]; then
      partition_group_name='Almosafer'
 elif [ "$partition_group" == '2520' ]; then
      partition_group_name='Tajawal'

fi

echo 'loading ->>>>'$filename 'into hive'
#echo 'before partition>>>>'$1.$6'--'$partition_date'--'$partition_group_name
hive -e "Set hive.execution.engine=mr;ALTER TABLE  ${1}.${6} ADD IF NOT EXISTS  PARTITION (day_date = '$partition_date',group_name = '$partition_group_name')";
echo 'partition created->>>>' $partition_date
hive -e "Set hive.execution.engine=mr;LOAD DATA INPATH \"$filename\" into table ${1}.${6} PARTITION(day_date = '$partition_date',group_name = '$partition_group_name')";
done



# TRIVAGO HOTEL DATA LOAD

#input_path_hotel='/tmp/output_trivago_hotel/'
#db='tajawal_uat_bi'
#echo '>>>>>>>>>>>>>>>>>>TRIVAGO HOTEL DATA INSERT>>>>>>>>>>>>>>>>>>>'

#hdfs dfs -chown anonymous $input_path_hotel'*'
#hdfs dfs -chmod 777 $input_path_hotel'*'

#for filename in `hadoop fs -ls $input_path_hotel  | awk '{print $NF}' | grep .csv$ | tr '\n' ' '`

#do
# echo $filename; 
#hdfs dfs -cat $filename | sed '/^"/s/amp;/\&/g' | hdfs dfs -put -f - $filename
#hdfs dfs -chown anonymous $filename 
#length="${#filename}"
#from=$(expr $length - 12)
#partition_date=${filename:$from:8}
#from_group=$(expr $length - 30)
#partition_group=${filename:$from_group:4}
#partition_group_name='NA'
 
# if [ "$partition_group" == '2356' ]; then
#      partition_group_name='Almosafer'
# elif [ "$partition_group" == '2520' ]; then
#      partition_group_name='Tajawal'
  
#fi

#echo $partition_group_name
#echo "'"$filename"'"
#echo 'loading ->>'$filename 'into hive'
#beeline -u 'jdbc:hive2://localhost:10000/' -e "ALTER TABLE  tajawal_uat_bi.trivago_hotel_data ADD IF NOT EXISTS  PARTITION (day_date = '$partition_date',group_name = '$partition_group_name')";
#echo 'partition created->' $partition_name
#beeline -u 'jdbc:hive2://localhost:10000/' -e "LOAD DATA INPATH \"$filename\" into table tajawal_uat_bi.trivago_hotel_data PARTITION(day_date = '$partition_date',group_name = '$partition_group_name')";
#done

#--------------------------------------------------------------------------------------------------------------------------------

#TRIVAGO POS DATA LOAD
