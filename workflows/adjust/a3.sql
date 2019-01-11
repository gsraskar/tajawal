set hive.mv.files.thread=0;
set hive.exec.dynamic.partition.mode=nonstrict;
drop table if exists tajawal_uat_bi.fact_google_analytics_ad_brand;
create table tajawal_uat_bi.fact_google_analytics_ad_brand like tajawal_bi.fact_google_analytics_ad;
insert into tajawal_uat_bi.fact_google_analytics_ad_brand partition(partition_date)
select
gadate,source,sourcemedium,device_category,adwords_campaign_id,adwords_adgroup_id,adwords_creative_id,campaign,impressions,adclicks,adcost,transactions,transactions_revenue,sessions,bounces,entraces,view_id,product,pos,mobile_os,language,
case when lower(ltrim(rtrim(channel_group))) like '%sem%' and lower(ltrim(rtrim(campaign))) like '%-brand-%' then 'Brand'
       when lower(ltrim(rtrim(channel_group))) like '%crm%' or lower(ltrim(rtrim(channel_group))) like  '%display%' then 'Brand'
       else 'Non Brand'
  end as brand_nonbrand,
objective,a.dim_channel_id,partition_date
from tajawal_bi.fact_google_analytics_ad a left outer join tajawal_bi.dim_spend_channels ds on a.dim_channel_id = ds.dim_channel_id;
