
add jar hdfs://ip-10-85-11-249.eu-west-1.compute.internal:8020/user/oozie/tajawal_uat/workflows/udf_libs/before-spend-udfs-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION getAppSearchColumn as 'com.tajawal.spend.udfs.GetAppSearchColumn';
drop table if exists tajawal_uat_bi.fact_google_analytics_search_app_before_inter;
alter table tajawal_uat_bi.fact_google_analytics_search_app_before rename to tajawal_uat_bi.fact_google_analytics_search_app_before_inter;
--Drop table if exists tajawal_uat_bi.fact_adjust_data;
Create table if not exists tajawal_uat_bi.fact_google_analytics_search_app_before ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
--tblproperties("skip.header.line.count"="1")
AS
  select
  `ga_lan`,
  `country`,
  `gadate` , 
  `sourcemedium` , 
  `devicecategory`, 
  `screen_name`,
  `total_events`,
  `uniq_events`,
  `view_id`,
   getAppSearchColumn(screen_name,'product') as product,
   getAppSearchColumn(screen_name,'product_detail') as product_detail,
   getAppSearchColumn(screen_name,'from_date') as from_date,
   getAppSearchColumn(screen_name,'to_date') as to_date,
   getAppSearchColumn(screen_name,'class') as class,
   getAppSearchColumn(screen_name,'pax') as pax
  from tajawal_uat_bi.fact_google_analytics_search_app_before_inter where screen_name like '%flight-search%' or screen_name like '%/hotels/search%'
; 
