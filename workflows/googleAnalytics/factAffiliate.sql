
drop table if exists tajawal_uat_bi.fact_affiliate_data_intermediate;
create table if not exists tajawal_uat_bi.fact_affiliate_data_intermediate
row format delimited fields terminated by "\t"
as
select 
channel_group, channel, group_name, gadate, sourcemedium,
device_category, campaign, 
sum(impressions) as impressions,
sum(adclicks) as adclicks, 
sum(adcost) as adcost,
sum(transactions) as transactions,
sum(transactions_revenue) as transactions_revenue,
sum(sessions) as sessions,
view_id,
product,
pos, 
mobile_os,
language,
brand_nonbrand
from (
-- QUERY for extracting bookings and revenue and cost

-- select 
-- channel_group, channel, group_name, gadate, sourcemedium,
-- device_category, campaign, impressions, adclicks, adcost,
-- transactions, transactions_revenue, sessions, view_id,
-- product, pos, mobile_os, language, brand_nonbrand
-- from

select 
lower(ds.channel_group) as channel_group,
ds.channel,
gv.group_name,
fh.dim_bookingdate_id as gadate,
fga.sourcemedium,
gv.mobile_os as device_category,
coalesce(ga_tr_campaign.campaign, 'na') as campaign,
0 as impressions,
0 as adclicks,
case when lower(fga.metric_name) like '%cpa%' 
then count(distinct(id)) * fga.value
else 
  case when lower(fga.metric_name) like '%crr%' 
  then (sum(payment_amount_usd) * fga.value) / 100 
  else 0.0 
  end
end as adcost,
count(distinct(id)) as transactions,
sum(fh.payment_amount_usd) as transactions_revenue,
0 as sessions,
fga.view_id,
fh.order_type as product,
case when lower(coalesce(dst.pos, 'na')) = 'null' then 'na' else dst.pos end as pos,
gv.mobile_os as mobile_os,
fh.language,
-- TODO
'na' as brand_nonbrand
from 
(
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, 
     dim_language_id as language, device_category, dim_channel_id
   from tajawal_bi.fact_hotel_orders_final
   where dim_status_id != 91 and dim_group_id in (1,2)
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
   union
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, dim_language as language, device_category, dim_channel_id
   from tajawal_bi.fact_flight_orders_final
   where dim_status_id != 91 and dim_group_id in (1,2)
   union
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id, order_type, iov as payment_amount_usd, 
     dim_language_id as language, 'na' as device_category,  dim_channel_id
   from tajawal_bi.fact_package_orders_final
   where dim_status_id != 91 and dim_group_id in (1,2)
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
) fh
inner join 
( 
  select transaction_id, dim_channel_id, max(view_id) as view_id, max(metric_name) as metric_name, max(value) as value
  ,max(sourcemedium) as sourcemedium
  from tajawal_bi.fact_google_analytics_transactions_final where view_id in ('109503410', '130709589') group by transaction_id, dim_channel_id
) fga on fh.order_no = fga.transaction_id and fh.dim_channel_id = fga.dim_channel_id
-- tajawal_bi.fact_google_analytics_transactions_final A
inner join tajawal_bi.dim_spend_ga_views gv on fga.view_id = gv.dim_view_id
inner join tajawal_bi.dim_spend_channels ds on fh.dim_channel_id = ds.dim_channel_id
inner join tajawal_bi.dim_store dst on fh.dim_store_id = dst.store_id
left outer join 
(
  select transaction_id, max(campaign) as campaign from 
  tajawal_bi.fact_google_analytics_transtocamp_mapping 
  group by transaction_id
) ga_tr_campaign on fga.transaction_id = ga_tr_campaign.transaction_id
where lower(ds.channel_group) like '%affiliate%'
group by
lower(ds.channel_group),
ds.channel,
gv.group_name,
fh.dim_bookingdate_id,
fga.sourcemedium,
gv.mobile_os,
coalesce(ga_tr_campaign.campaign, 'na'),
fga.metric_name,
fga.value,
fga.view_id, fh.order_type, case when lower(coalesce(dst.pos, 'na')) = 'null' then 'na' else dst.pos end, gv.mobile_os, fh.language

union all

select C.channel_group, C.channel, B.group_name, A.gadate, A.sourcemedium, A.device_category, A.campaign, A.impressions, A.adclicks, 0 as adcost,
0 as transactions, 0 as transactions_revenue, A.sessions, A.view_id, A.product, A.pos, A.mobile_os, A.language, A.brand_nonbrand
from tajawal_bi.fact_google_analytics_ad A inner join tajawal_bi.dim_spend_ga_views B on A.view_id = B.dim_view_id
inner join
(
    select B.channel_group, B.channel, A.dim_channel_id, A.ga_sourcemedium, count(*)
    from tajawal_bi.dim_spend_ga_sourcemedium_channels A inner join tajawal_bi.dim_spend_channels B
    on A.dim_channel_id = B.dim_channel_id
    where lower(B.channel_group) like '%affiliate%'
    group by B.channel_group, B.channel, A.dim_channel_id, A.ga_sourcemedium
) C on regexp_replace(A.sourcemedium, ' ', '') = regexp_replace(C.ga_sourcemedium, ' ', '')

) A
group by
channel_group, channel, group_name, gadate, sourcemedium,
device_category, campaign, 
view_id,
product,
pos, 
mobile_os,
language,
brand_nonbrand
;

drop table if exists tajawal_uat_bi.fact_affiliate_data;
alter table tajawal_uat_bi.fact_affiliate_data_intermediate rename to tajawal_uat_bi.fact_affiliate_data;


