set hive.mv.files.thread=0;
add jar hdfs://ip-10-85-11-249.eu-west-1.compute.internal:8020/user/oozie/tajawal_uat/workflows/udf_libs/spend-udfs-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION getWebSearchColumn as 'com.tajawal.spend.udfs.GetWebSearchColumn';

drop table if exists tajawal_bi.fact_google_analytics_search_web_incr_inter;
alter table tajawal_bi.fact_google_analytics_search_web_incr rename to tajawal_bi.fact_google_analytics_search_web_incr_inter;
--Drop table if exists tajawal_bi.fact_adjust_data;
drop table if exists tajawal_bi.fact_google_analytics_web_pageviews_incr;
create table tajawal_bi.fact_google_analytics_web_pageviews_incr like tajawal_bi.fact_google_analytics_web_pageviews;
insert into tajawal_bi.fact_google_analytics_web_pageviews_incr
select
  'bookings' as pagetype,
  `galang`,
  `country`,
  `gadate` ,
  case when ((`channel_grouping` like '%Search Brand%' or `channel_grouping` like '%Search Non Brand%' or `channel_grouping` like '%Paid Search%') and  regexp_replace(`sourcemedium`,' ','') like '%google/cpc%') then 'google/cpc/SN'
       when `channel_grouping` like '%Display%' and regexp_replace(`sourcemedium`,' ','') like '%google/cpc%' then 'google/cpc/CN'
       else `sourcemedium` end
  as `sourcemedium` , 
  `devicecategory`,
  `hostname`,
  cast( encode(pagePath , 'UTF-8') as string) as pagePath,
  `channel_grouping`,
  (CASE WHEN pagePath like '%/booking/A%' then 'flight' 
        when pagepath like '%hotel%' then 'hotel' 
        else 'na' 
    end) as product,
  `pageViews`,
  `uniqpageviews`,
  `view_id`

  from tajawal_bi.fact_google_analytics_search_web_incr_inter where pagePath like '%/booking/A%' or pagePath like '%/hotel/booking/%' ;


insert into tajawal_bi.fact_google_analytics_web_pageviews_incr
select
  'payment' as pagetype,
  `galang`,
  `country`,
  `gadate` ,
  case when ((`channel_grouping` like '%Search Brand%' or `channel_grouping` like '%Search Non Brand%' or `channel_grouping` like '%Paid Search%') and  regexp_replace(`sourcemedium`,' ','') like '%google/cpc%') then 'google/cpc/SN'
       when `channel_grouping` like '%Display%' and regexp_replace(`sourcemedium`,' ','') like '%google/cpc%' then 'google/cpc/CN'
       else `sourcemedium` end
  as `sourcemedium` , 
  `devicecategory`,
  `hostname`,
  cast( encode(pagePath , 'UTF-8') as string) as pagePath,
  `channel_grouping`,
  CASE WHEN pagePath like '%flight%' then 'flight' 
       when pagepath like '%hotel%' then 'hotel' 
       else 'na' 
       end as product,
  `pageviews`,
  `uniqpageviews`,
  `view_id`
  from tajawal_bi.fact_google_analytics_search_web_incr_inter where pagePath like '%/flight/seat/%' or pagePath like '%/hotel/room/%'
;

insert into tajawal_bi.fact_google_analytics_web_pageviews_incr
select
  'details' as pagetype,
  `galang`,
  `country`,
  `gadate` ,
  case when ((`channel_grouping` like '%Search Brand%' or `channel_grouping` like '%Search Non Brand%' or `channel_grouping` like '%Paid Search%') and  regexp_replace(`sourcemedium`,' ','') like '%google/cpc%') then 'google/cpc/SN'
       when `channel_grouping` like '%Display%' and regexp_replace(`sourcemedium`,' ','') like '%google/cpc%' then 'google/cpc/CN'
       else `sourcemedium` end
  as `sourcemedium` , 
  `devicecategory`,
  `hostname`,
  cast( encode(pagePath , 'UTF-8') as string) as pagePath,
  `channel_grouping`,
  CASE WHEN pagePath like '%/flight/traveller/%' then 'flight' 
       when pagepath like '%/hotel/details/%' then 'hotel'
       else 'na' end as product,
  `pageViews`,
  `uniqpageviews`,
  `view_id`
  from tajawal_bi.fact_google_analytics_search_web_incr_inter where pagePath like '%/flight/traveller/%' or pagePath like '%/hotel/details/%'
;


Create table if not exists tajawal_bi.fact_google_analytics_search_web_incr ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
--tblproperties("skip.header.line.count"="1")
AS
  select
  `galang`,
  `country`,
  `gadate` , 
  case when ((`channel_grouping` like '%Search Brand%' or `channel_grouping` like '%Search Non Brand%' or `channel_grouping` like '%Paid Search%') and  regexp_replace(`sourcemedium`,' ','') like '%google/cpc%') then 'google/cpc/SN'
       when `channel_grouping` like '%Display%' and regexp_replace(`sourcemedium`,' ','') like '%google/cpc%' then 'google/cpc/CN'
       else `sourcemedium` end
  as `sourcemedium` , 
  `devicecategory`, 
  `hostname`,
  cast( encode(pagePath , 'UTF-8') as string) as pagePath,
  `channel_grouping`,
  `pageViews`,
  `uniqpageviews`,
  `view_id`,
  getWebSearchColumn(`pagePath`,'language') as language,
  case when getWebSearchColumn(`pagePath`,'product') like '%flight%' then 'flights'
       when getWebSearchColumn(`pagePath`,'product') like '%hotel%' then 'hotels'
       else 'na' end
  as product,
  getWebSearchColumn(`pagePath`,'product_detail') as product_detail,
  substr(getWebSearchColumn(`pagePath`,'from_date'), 1, 1000) as start_date,
  substr(getWebSearchColumn(`pagePath`,'to_date'), 1, 1000) as end_date,
  substr(getWebSearchColumn(`pagePath`,'class'), 1, 99) as class,
  substr(getWebSearchColumn(`pagePath`,'pax'), 1, 100) as pax
  from tajawal_bi.fact_google_analytics_search_web_incr_inter where pagePath  like '%/flights/%' or pagePath like '%/hotels/%'
; 
drop table if exists tajawal_bi.fact_google_analytics_search_web_incr_inter;

create table if not exists tajawal_bi.fact_google_analytics_search_web_incr_inter row format delimited fields terminated by '|' AS
select C.*, B.dim_channel_id as dim_channel_id
from tajawal_bi.fact_google_analytics_search_web_incr C
left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', ''))) A on lower(regexp_replace(sourcemedium, ' ', '')) = lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
  left outer join (select dim_channel_id, max(channel_group) as channel_group from tajawal_bi.dim_spend_channels group by dim_channel_id ) B on A.dim_channel_id = B.dim_channel_id
;
drop table tajawal_bi.fact_google_analytics_search_web_incr;
alter table tajawal_bi.fact_google_analytics_search_web_incr_inter rename to tajawal_bi.fact_google_analytics_search_web_incr;

drop table if exists tajawal_bi.fact_google_analytics_search_web_30days;
create table tajawal_bi.fact_google_analytics_search_web_30days like tajawal_bi.fact_google_analytics_search_web;
insert into tajawal_bi.fact_google_analytics_search_web_30days
select *
from tajawal_bi.fact_google_analytics_search_web
where datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),from_unixtime(unix_timestamp(gadate , 'yyyyMMdd'),'yyyy-MM-dd')) < 31;

insert into tajawal_bi.fact_google_analytics_search_web_30days
select * from tajawal_bi.fact_google_analytics_search_web_incr;
