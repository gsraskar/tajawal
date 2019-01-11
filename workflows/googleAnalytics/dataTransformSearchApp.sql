set hive.mv.files.thread=0;
add jar hdfs://ip-10-85-11-249.eu-west-1.compute.internal:8020/user/oozie/tajawal_uat/workflows/udf_libs/spend-udfs-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION getAppSearchColumn as 'com.tajawal.spend.udfs.GetAppSearchColumn';
drop table if exists tajawal_bi.fact_google_analytics_search_app_incr_inter;
alter table tajawal_bi.fact_google_analytics_search_app_incr rename to tajawal_bi.fact_google_analytics_search_app_incr_inter;
--Drop table if exists tajawal_bi.fact_adjust_data;
Create table if not exists tajawal_bi.fact_google_analytics_search_app_incr_inter2 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
--tblproperties("skip.header.line.count"="1")
AS
  select
  `galang`,
  `country`,
  `gadate`, 
  `sourcemedium` , 
  `devicecategory`, 
  `event_action`,
  `event_label`,
  `total_events`,
  `uniq_total_events`,
  `view_id`,
   case when lower(getAppSearchColumn(concat('/',event_action,'/',event_label),'product')) like '%hotel%' 
   then 'hotel' 
   else case when lower(getAppSearchColumn(concat('/',event_action,'/',event_label),'product')) like '%flight%'
     then 'flight'
     else 'na'
     end
   end as product,
   getAppSearchColumn(concat('/',event_action,'/',event_label),'product_detail') as product_detail,
   getAppSearchColumn(concat('/',event_action,'/',event_label),'from_date') as from_date,
   getAppSearchColumn(concat('/',event_action,'/',event_label),'to_date') as to_date,
   getAppSearchColumn(concat('/',event_action,'/',event_label),'class') as class,
   getAppSearchColumn(concat('/',event_action,'/',event_label),'pax') as pax
  from tajawal_bi.fact_google_analytics_search_app_incr_inter where event_action like '%search_flight%' or event_action like '%search_hotel%'
; 

drop table if exists tajawal_bi.fact_google_analytics_search_app_incr_inter3;
create table if not exists tajawal_bi.fact_google_analytics_search_app_incr_inter3 row format delimited fields terminated by '|' AS
select C.*, B.dim_channel_id as dim_channel_id
from tajawal_bi.fact_google_analytics_search_app_incr_inter2 C
left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', ''))) A on lower(regexp_replace(sourcemedium, ' ', '')) = lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
  left outer join (select dim_channel_id, max(channel_group) as channel_group from tajawal_bi.dim_spend_channels group by dim_channel_id ) B on A.dim_channel_id = B.dim_channel_id
;

alter table tajawal_bi.fact_google_analytics_search_app_incr_inter2 rename to tajawal_bi.fact_google_analytics_search_app_incr;
drop table tajawal_bi.fact_google_analytics_search_app_incr_final;
alter table tajawal_bi.fact_google_analytics_search_app_incr_inter3 rename to tajawal_bi.fact_google_analytics_search_app_incr_final;

drop table if exists tajawal_bi.fact_google_analytics_search_app_30days;
create table tajawal_bi.fact_google_analytics_search_app_30days like tajawal_bi.fact_google_analytics_search_app_final;
insert into tajawal_bi.fact_google_analytics_search_app_30days
select * 
from tajawal_bi.fact_google_analytics_search_app_final
where datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),from_unixtime(unix_timestamp(gadate , 'yyyyMMdd'),'yyyy-MM-dd')) < 31;

insert into tajawal_bi.fact_google_analytics_search_app_30days
select * from tajawal_bi.fact_google_analytics_search_app_incr_final;
