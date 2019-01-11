rm -f /tmp/final_dataupload.sql
#hdfs dfs -copyToLocal /user/oozie/workflows/tajawal_hotel/tajawal_management_kpi/final_dataupload.sql /tmp/
hdfs dfs -copyToLocal $1/tajawal_hotel/tajawal_management_kpi/final_dataupload.sql /tmp/
mysql -h10.85.11.173 -ubi_user -pEyjAN51Ar912 $2 < /tmp/final_dataupload.sql
