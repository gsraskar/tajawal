set hive.mv.files.thread=0;

-- Step 1 : Extract required fields from adjust
DROP TABLE IF EXISTS ${hiveDB}.fact_spend_data_channelwise_inter;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_spend_data_channelwise_inter as
select
`date` as dim_date_id,
ds.channel_group,
ds.channel,
apps.group_name,
adj.product,
adj.pos,
'app' as device,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end as device_category,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective,
-- spend
-- sum(adj.cost) as spend,
0 as spend,
-- bookings, exclude bookings and revenue for channel_groups : Meta, Organic Search, Partnership, Direct, SEO,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- -- case when lower(adj.campaign) not like '%package%' and lower(adj.campaign) not like '%_p_%' then 
-- -- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
--  sum(adj.revenue_events)
-- end
-- end as bookings,
0 as bookings,
-- revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- -- case when lower(adj.campaign) not like '%package%' and lower(adj.campaign) not like '%_p_%' then 
-- -- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(adj.revenue)
-- -- end
-- end as revenue,
0 as revenue,
-- installs,
sum(adj.installs) as installs,
-- sessions,
sum(adj.sessions) as sessions,
-- search,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(hotelsearch_events) + sum(flightsearch_events) 
-- end 
0 as searches,
-- impressions,
sum( case when ds.dim_channel_id in ('8f1e274b7445ec086397b66c03ac86fa', '943d05bfcdc319151ed3a785d6509b8a', '343cb386562b91edeb3a0c6a4473b1c8', '7bcec112f2cab01e170641fa07eeb238') then 0 else adj.impressions end) as impressions,
-- clicks,
sum( case when ds.dim_channel_id in ('8f1e274b7445ec086397b66c03ac86fa', '943d05bfcdc319151ed3a785d6509b8a', '343cb386562b91edeb3a0c6a4473b1c8', '7bcec112f2cab01e170641fa07eeb238') then 0 else adj.clicks end) as clicks
,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(hotelpayment_events) + sum(flightpayment_events)  
-- end 
0 as payment_visits
,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(hotelpage_events) + sum(flightdetails_events) 
-- end 
0 as details_visits
,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(hotelguestdetails_events) + sum(flightpax_events) 
-- end 
0 as traveller_visits
, 
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(hotelsearch_events) + sum(flightsearch_events) 
-- end
0 as uniq_searches
from 
tajawal_bi.fact_adjust_data_multi_app_final adj inner join tajawal_bi.dim_spend_adjust_apps apps on adj.app_token = apps.dim_app_id left outer join (select lower(regexp_replace(adjust_network_name, ' ', '')) as adjust_network_name, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_adjust_network_channels group by lower(regexp_replace(adjust_network_name, ' ', ''))) adjn on lower(regexp_replace(adj.network,' ','')) = lower(regexp_replace(adjn.adjust_network_name, ' ', '')) left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on adjn.dim_channel_id = ds.dim_channel_id
group by 
adj.`date`,
ds.channel_group,
ds.channel,
apps.group_name,
adj.product,
adj.pos,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective
;

-- Step 1.1 : Bring bookings and revenue of Meta, Direct, Partnerships, SEO, OrganicSearch for Hotel from Adjust
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
`date` as dim_date_id,
ds.channel_group,
ds.channel,
apps.group_name,
'hotel' as product,
adj.pos,
'app' as device,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end as device_category,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective,
-- spend
-- Data Exclusion Condition : Exclude cost for mobile network channel, cost_metric_name CPA for dates after 1st November 2017 
-- Data Exclusion Reason : Excluded because adjust data hotel_events and flight_events fields have issue in source data, events are being recorder with huge values.
sum(
case when `date` > '20171101' and ( lower(cost_metric_name) like '%cpa%' or lower(cost_metric_name) like '%crr%' ) 
-- and lower(ds.channel_group) like '%mobile%network%'
and (lower(ds.channel_group) like '%mobile%network%' or ( lower(ds.channel_group) like 'retargeting' and lower(ds.channel) not like '%criteo%' ))
then 0
else hotel_spend
end
) as spend,
-- bookings, exclude bookings and revenue for channel_groups : Meta, Organic Search, Partnership, Direct, SEO,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then sum(adj.hotel_events)
-- then 
-- sum(adj.revenue_events) - sum(adj.flight_events)
-- else 0.0
-- end 
sum (
-- case when `date` > '20171101' and ( lower(cost_metric_name) like '%cpa%' or lower(cost_metric_name) like '%crr%' ) and lower(ds.channel_group) like '%mobile%network%'
case when `date` >= '20171201'
-- and (lower(ds.channel_group) like '%mobile%network%' or ( lower(ds.channel_group) like 'retargeting' and lower(ds.channel) not like '%criteo%' ))
then 
0
else adj.revenue_events - adj.flight_events
end
)
as bookings,
-- revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then sum(adj.hotel_revenue)
sum (
-- case when `date` > '20171101' and ( lower(cost_metric_name) like '%cpa%' or lower(cost_metric_name) like '%crr%' ) and lower(ds.channel_group) like '%mobile%network%'
case when `date` >= '20171201'
-- and (lower(ds.channel_group) like '%mobile%network%' or ( lower(ds.channel_group) like 'retargeting' and lower(ds.channel) not like '%criteo%' ))
then 
0
else adj.revenue - adj.flight_revenue
end
)
-- else 0.0
--end 
as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
sum(hotelsearch_events) as searches,
-- impressions,
0 as impressions,
-- clicks,
0 as clicks
,sum(hotelpayment_events)  as payment_visits
,sum(hotelpage_events)  as details_visits
,sum(hotelguestdetails_events) as traveller_visits
, sum(hotelsearch_events) as uniq_searches
from 
tajawal_bi.fact_adjust_data_multi_app_final adj inner join tajawal_bi.dim_spend_adjust_apps apps on adj.app_token = apps.dim_app_id inner join (select lower(regexp_replace(adjust_network_name, ' ', '')) as adjust_network_name, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_adjust_network_channels group by lower(regexp_replace(adjust_network_name, ' ', ''))) adjn on lower(regexp_replace(adj.network,' ','')) = lower(regexp_replace(adjn.adjust_network_name, ' ', '')) inner join 
(
  select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid 
  from tajawal_bi.dim_spend_channels 
  -- where lower(channel_group) like '%meta%' or lower(channel_group) like '%organic%search%' or lower(channel_group) like '%seo%' or lower(channel_group) like '%partnerships%' or lower(channel_group) like '%direct%' or lower(channel_group) like '%crm%' 
  group by dim_channel_id
) ds on adjn.dim_channel_id = ds.dim_channel_id
group by 
adj.`date`,
ds.channel_group,
ds.channel,
apps.group_name,
adj.product,
adj.pos,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective
;

-- Step 1.2 : Bring bookings and revenue of Meta, Direct, Partnerships, SEO, OrganicSearch for Flight from Adjust
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
`date` as dim_date_id,
ds.channel_group,
ds.channel,
apps.group_name,
'flight' as product,
adj.pos,
'app' as device,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end as device_category,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective,
-- spend
-- Data Exclusion Condition : Exclude cost for mobile network channel, cost_metric_name CPA for dates after 1st November 2017 
-- Data Exclusion Reason : Excluded because adjust data hotel_events and flight_events fields have issue in source data, events are being recorder with huge values.
sum(
case when `date` > '20171101' and ( lower(cost_metric_name) like '%cpa%' or lower(cost_metric_name) like '%crr%' ) 
-- and lower(ds.channel_group) like '%mobile%network%'
and (lower(ds.channel_group) like '%mobile%network%' or ( lower(ds.channel_group) like 'retargeting' and lower(ds.channel) not like '%criteo%' ))
then 0
else flight_spend
end
) as spend,
-- sum(flight_spend) as spend,
-- bookings, exclude bookings and revenue for channel_groups : Meta, Organic Search, Partnership, Direct, SEO,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then 
sum(
-- case when `date` > '20171101' and ( lower(cost_metric_name) like '%cpa%' or lower(cost_metric_name) like '%crr%' ) and lower(ds.channel_group) like '%mobile%network%'
case when `date` >= '20171201'
then 
0
else adj.flight_events
end
-- adj.flight_events
)
-- else 0.0 
-- end 
as bookings,
-- revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then 
sum(
-- case when `date` > '20171101' and ( lower(cost_metric_name) like '%cpa%' or lower(cost_metric_name) like '%crr%' ) and lower(ds.channel_group) like '%mobile%network%'
case when `date` >= '20171201'
then 
0
else adj.flight_revenue
end
-- adj.flight_revenue
)
-- else 0.0 
-- end 
as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
sum(flightsearch_events) as searches,
-- impressions,
0 as impressions,
-- clicks,
0 as clicks
,sum(flightpayment_events)  as payment_visits
,sum(flightdetails_events)  as details_visits
,sum(flightpax_events) as traveller_visits
, sum(flightsearch_events) as uniq_searches
from 
tajawal_bi.fact_adjust_data_multi_app_final adj inner join tajawal_bi.dim_spend_adjust_apps apps on adj.app_token = apps.dim_app_id inner join 
(
  select lower(regexp_replace(adjust_network_name, ' ', '')) as adjust_network_name, max(dim_channel_id) as dim_channel_id 
  from tajawal_bi.dim_spend_adjust_network_channels 
  group by lower(regexp_replace(adjust_network_name, ' ', ''))) adjn on lower(regexp_replace(adj.network,' ','')) = lower(regexp_replace(adjn.adjust_network_name, ' ', '')
) inner join 
(
  select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid 
  from tajawal_bi.dim_spend_channels 
  -- where lower(channel_group) like '%meta%' or lower(channel_group) like '%organic%search%' or lower(channel_group) like '%seo%' or lower(channel_group) like '%partnerships%' or lower(channel_group) like '%direct%' or lower(channel_group) like '%crm%' 
  group by dim_channel_id
) ds on adjn.dim_channel_id = ds.dim_channel_id
group by  
adj.`date`,
ds.channel_group,
ds.channel,
apps.group_name,
adj.product,
adj.pos,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective
;

-- Step 1.3 : Bring bookings and revenue of Meta, Direct, Partnerships, SEO, OrganicSearch for package from Adjust
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
`date` as dim_date_id,
ds.channel_group,
ds.channel,
apps.group_name,
'package' as product,
adj.pos,
'app' as device,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end as device_category,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective,
-- spend
sum(package_spend) as spend,
-- bookings, exclude bookings and revenue for channel_groups : Meta, Organic Search, Partnership, Direct, SEO,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then 
sum(
case when `date` >= '20171201'
then 0
else
adj.holidays_revenue_events
end
)
-- else 0.0 
-- end 
-- 0
as bookings,
-- revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then 
sum(
case when `date` >= '20171201'
then 0
else
adj.holidays_revenue
end
)
-- else 0.0 
-- end 
-- 0
as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
0 as searches,
-- impressions,
0 as impressions,
-- clicks,
0 as clicks
,sum(holidays_payment_events)  as payment_visits
,sum(holidays_package_details_events)  as details_visits
,sum(holidays_guest_details_events) as traveller_visits
, 0 as uniq_searches
from 
tajawal_bi.fact_adjust_data_multi_app_final adj inner join tajawal_bi.dim_spend_adjust_apps apps on adj.app_token = apps.dim_app_id inner join 
(
  select lower(regexp_replace(adjust_network_name, ' ', '')) as adjust_network_name, max(dim_channel_id) as dim_channel_id 
  from tajawal_bi.dim_spend_adjust_network_channels 
  group by lower(regexp_replace(adjust_network_name, ' ', ''))) adjn on lower(regexp_replace(adj.network,' ','')) = lower(regexp_replace(adjn.adjust_network_name, ' ', '')
) inner join 
(
  select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid 
  from tajawal_bi.dim_spend_channels 
  -- where lower(channel_group) like '%meta%' or lower(channel_group) like '%organic%search%' or lower(channel_group) like '%seo%' or lower(channel_group) like '%partnerships%' or lower(channel_group) like '%direct%' or lower(channel_group) like '%crm%' 
  group by dim_channel_id
) ds on adjn.dim_channel_id = ds.dim_channel_id
group by  
adj.`date`,
ds.channel_group,
ds.channel,
apps.group_name,
adj.product,
adj.pos,
case when lower(adj.app_name) like '%ios%' then 'ios' else 'android' end,
adj.language,
ds.paid_unpaid,
adj.brand_nonbrand,
adj.objective
;
-- Step 1.4 Hotel Spend : dim_bookingdate_id >= '20171101' and lower(ds.type) like '%app%' and lower(dsc.channel_group) like '%mobile%network%'
-- CASE : Calculate spend, bookings and revenue for 'mobile_network' channels which are CPA and CRR channels for date > 20171101
-- REASON : There is an issue with source adjust data as : hotel and flight events are very high for dates > 20171101.
--    In order to resolve this, we are calculating spend, bookings and revenue from fact order tables for above combination.
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select 
dim_bookingdate_id as dim_date_id,
ds.channel_group,
ds.channel,
A.group_name,
'hotel' as product,
pos as pos,
'app' as device,
lower(A.mobile_os) as device_category,
lower(case when dim_language like '%en%' then 'en' else case when lower(dim_language) like '%ar%' then 'ar' else 'na' end end) as language,
ds.paid_unpaid,
'non brand' as brand_nonbrand,
'aquisition' as objective,
case when lower(dsc.cost_metric_name) like '%cpa%' then count(distinct(id)) * cost_metric_value
else
case when lower(dsc.cost_metric_name) like '%crr%' then sum(ibv) * cost_metric_value else 0 end
end
as spend,
case when dim_bookingdate_id >= '20171201' then count(distinct(id)) else 0 end as bookings,
case when dim_bookingdate_id >= '20171201' then sum(ibv) else 0 end as revenue,
0 as installs,
0 as sessions,
0 as searches,
0 as impressions,
0 as clicks,
0 as payment_visits,
0  as details_visits,
0 as traveller_visits,
0 as uniq_searches
from 
(
  select dim_bookingdate_id, id, ibv, dim_channel_id, gv.group_name group_name, ds.pos as pos, ds.mobile_os as mobile_os, dim_language_id as dim_language
  from
  tajawal_bi.fact_hotel_orders_final A
  inner join tajawal_bi.dim_store ds on A.dim_store_id = ds.store_id
  inner join tajawal_bi.dim_groups gv on A.dim_group_id = gv.group_id
  where -- dim_bookingdate_id >= '20171101' and 
  ds.type = 'app' 
) A 
left outer join 
(
  select A.dim_channel_id, 
  `date` as dim_date_id,
  apps.mobile_os,
  apps.group_name,
  max(dsc.channel_group) as channel_group, 
  max(dsc.channel) as channel, 
  max(dsc.paid_unpaid) as paid_unpaid,
  max(cost_metric_name) as cost_metric_name,
  case when lower(max(cost_metric_name)) like '%cpa%' then coalesce(max(flight_spend / flight_events), max(cost_metric_value)) 
  else
    case when lower(max(cost_metric_name)) like '%crr%' then coalesce(max(flight_spend / flight_revenue), max(cost_metric_value)) else 0.0 end
  end
  as cost_metric_value, count(*) 
  from tajawal_bi.fact_adjust_data_multi_app_final A 
  inner join tajawal_bi.dim_spend_channels dsc on A.dim_channel_id = dsc.dim_channel_id
  inner join tajawal_bi.dim_spend_adjust_apps apps on lower(ltrim(rtrim(A.app_token))) = lower(ltrim(rtrim(apps.dim_app_id)))
  where (lower(dsc.channel_group) like '%mobile%network%' or ( lower(dsc.channel_group) like 'retargeting' and lower(dsc.channel) not like '%criteo%' ))
  and (lower(cost_metric_name) like '%cpa' or lower(cost_metric_name) like '%crr%')
  group by A.dim_channel_id, `date`, apps.mobile_os, apps.group_name
) dsc
on A.dim_channel_id = dsc.dim_channel_id and A.dim_bookingdate_id = dsc.dim_date_id and lower(ltrim(rtrim(A.mobile_os))) = lower(ltrim(rtrim(dsc.mobile_os)))
and lower(ltrim(rtrim(A.group_name))) = lower(ltrim(rtrim(dsc.group_name)))
left outer join tajawal_bi.dim_spend_channels ds on A.dim_channel_id = ds.dim_channel_id
group by
dim_bookingdate_id,
ds.channel_group,
ds.channel,
A.group_name,
pos,
lower(A.mobile_os),
lower(case when dim_language like '%en%' then 'en' else case when lower(dim_language) like '%ar%' then 'ar' else 'na' end end),
ds.paid_unpaid,
dsc.cost_metric_name,
dsc.cost_metric_value;

-- Step 1.5 : Same as 1.4 but for flight
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select 
dim_bookingdate_id as dim_date_id,
ds.channel_group,
ds.channel,
A.group_name,
'flight' as product,
pos as pos,
'app' as device,
lower(A.mobile_os) as device_category,
lower(case when dim_language like '%en%' then 'en' else case when lower(dim_language) like '%ar%' then 'ar' else 'na' end end) as language,
ds.paid_unpaid,
'non brand' as brand_nonbrand,
'aquisition' as objective,
case when lower(dsc.cost_metric_name) like '%cpa%' then count(distinct(id)) * cost_metric_value
else
case when lower(dsc.cost_metric_name) like '%crr%' then sum(ibv) * cost_metric_value else 0 end
end
as spend,
-- Prior to December 2017 GA Transactions did not have channel information in it, so not including bookings and revenue from here
case when dim_bookingdate_id >= '20171201' then count(distinct(id)) else 0 end as bookings,
case when dim_bookingdate_id >= '20171201' then sum(ibv) else 0 end as revenue,
0 as installs,
0 as sessions,
0 as searches,
0 as impressions,
0 as clicks,
0 as payment_visits,
0  as details_visits,
0 as traveller_visits,
0 as uniq_searches
from 
(
  select dim_bookingdate_id, id, ibv, dim_channel_id, gv.group_name group_name, ds.pos as pos, ds.mobile_os as mobile_os, dim_language
  from
  tajawal_bi.fact_flight_orders_final A
  inner join tajawal_bi.dim_store ds on A.dim_store_id = ds.store_id
  inner join tajawal_bi.dim_groups gv on A.dim_group_id = gv.group_id
  where -- dim_bookingdate_id >= '20171101' and 
   ds.type = 'app' 
) A 
left outer join 
(
  select A.dim_channel_id, 
  `date` as dim_date_id,
  apps.mobile_os,
  apps.group_name,
  max(dsc.channel_group) as channel_group, 
  max(dsc.channel) as channel, 
  max(dsc.paid_unpaid) as paid_unpaid,
  max(cost_metric_name) as cost_metric_name,
  case when lower(max(cost_metric_name)) like '%cpa%' then coalesce(max(flight_spend / flight_events), max(cost_metric_value)) 
  else
    case when lower(max(cost_metric_name)) like '%crr%' then coalesce(max(flight_spend / flight_revenue), max(cost_metric_value)) else 0.0 end
  end
  as cost_metric_value, count(*) 
  from tajawal_bi.fact_adjust_data_multi_app_final A 
  inner join tajawal_bi.dim_spend_channels dsc on A.dim_channel_id = dsc.dim_channel_id
  inner join tajawal_bi.dim_spend_adjust_apps apps on lower(ltrim(rtrim(A.app_token))) = lower(ltrim(rtrim(apps.dim_app_id)))
  where 
  -- lower(dsc.channel_group) like '%mobile%network%' and 
  (lower(dsc.channel_group) like '%mobile%network%' or ( lower(dsc.channel_group) like 'retargeting' and lower(dsc.channel) not like '%criteo%' )) and
  (lower(cost_metric_name) like '%cpa' or lower(cost_metric_name) like '%crr%')
  group by A.dim_channel_id, `date`, apps.mobile_os, apps.group_name
) dsc
on A.dim_channel_id = dsc.dim_channel_id and A.dim_bookingdate_id = dsc.dim_date_id and lower(ltrim(rtrim(A.mobile_os))) = lower(ltrim(rtrim(dsc.mobile_os)))
and A.group_name = dsc.group_name
left outer join tajawal_bi.dim_spend_channels ds on A.dim_channel_id = ds.dim_channel_id
group by
dim_bookingdate_id,
ds.channel_group,
ds.channel,
A.group_name,
pos,
lower(A.mobile_os),
lower(case when dim_language like '%en%' then 'en' else case when lower(dim_language) like '%ar%' then 'ar' else 'na' end end),
ds.paid_unpaid,
dsc.cost_metric_name,
dsc.cost_metric_value;

-- Step 1.6 : Same as 1.4 but for package
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select 
dim_bookingdate_id as dim_date_id,
ds.channel_group,
ds.channel,
group_name,
'package' as product,
pos as pos,
'app' as device,
lower(A.mobile_os) as device_category,
lower(case when dim_language like '%en%' then 'en' else case when lower(dim_language) like '%ar%' then 'ar' else 'na' end end) as language,
ds.paid_unpaid,
'non brand' as brand_nonbrand,
'aquisition' as objective,
case when lower(dsc.cost_metric_name) like '%cpa%' then count(distinct(id)) * cost_metric_value
else
case when lower(dsc.cost_metric_name) like '%crr%' then sum(ibv) * cost_metric_value else 0 end
end
as spend,
-- Prior to December 2017, GA transactions data did not have channel information in it.
case when dim_bookingdate_id >= '20171201' then count(distinct(id)) else 0 end as bookings,
case when dim_bookingdate_id >= '20171201' then sum(ibv) else 0 end as revenue,
0 as installs,
0 as sessions,
0 as searches,
0 as impressions,
0 as clicks,
0 as payment_visits,
0  as details_visits,
0 as traveller_visits,
0 as uniq_searches
from 
(
  select dim_bookingdate_id, id, iov as ibv, dim_channel_id, gv.group_name group_name, ds.pos as pos, ds.mobile_os as mobile_os, dim_language_id as dim_language
  from
  tajawal_bi.fact_package_orders_final A
  inner join tajawal_bi.dim_store ds on A.dim_store_id = ds.store_id
  inner join tajawal_bi.dim_groups gv on A.dim_group_id = gv.group_id
  where -- dim_bookingdate_id >= '20171101' and 
  ds.type = 'app' 
) A 
left outer join 
(
  select A.dim_channel_id, 
  `date` as dim_date_id,
  apps.mobile_os,
  max(dsc.channel_group) as channel_group, 
  max(dsc.channel) as channel, 
  max(dsc.paid_unpaid) as paid_unpaid,
  max(cost_metric_name) as cost_metric_name,
  case when lower(max(cost_metric_name)) like '%cpa%' then coalesce(max(flight_spend / flight_events), max(cost_metric_value)) 
   else
    case when lower(max(cost_metric_name)) like '%crr%' then coalesce(max(flight_spend / flight_revenue), max(cost_metric_value)) else 0.0 end
  end
  as cost_metric_value, count(*) 
  from tajawal_bi.fact_adjust_data_multi_app_final A 
  inner join tajawal_bi.dim_spend_channels dsc on A.dim_channel_id = dsc.dim_channel_id
  inner join tajawal_bi.dim_spend_adjust_apps apps on lower(ltrim(rtrim(A.app_token))) = lower(ltrim(rtrim(apps.dim_app_id)))
  where 
  -- lower(dsc.channel_group) like '%mobile%network%' and 
  (lower(dsc.channel_group) like '%mobile%network%' or ( lower(dsc.channel_group) like 'retargeting' and lower(dsc.channel) not like '%criteo%' )) and
  (lower(cost_metric_name) like '%cpa' or lower(cost_metric_name) like '%crr%')
  group by A.dim_channel_id, `date`, apps.mobile_os
) dsc
on A.dim_channel_id = dsc.dim_channel_id and A.dim_bookingdate_id = dsc.dim_date_id and lower(ltrim(rtrim(A.mobile_os))) = lower(ltrim(rtrim(dsc.mobile_os)))
left outer join tajawal_bi.dim_spend_channels ds on A.dim_channel_id = ds.dim_channel_id
group by
dim_bookingdate_id,
ds.channel_group,
ds.channel,
group_name,
pos,
lower(A.mobile_os),
lower(case when dim_language like '%en%' then 'en' else case when lower(dim_language) like '%ar%' then 'ar' else 'na' end end),
ds.paid_unpaid,
dsc.cost_metric_name,
dsc.cost_metric_value;

-- Step 2 : Combine adjust fields with Social media FB and Twitter
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
date_start as dim_date_id,
ds.channel_group,
ds.channel,
fba.brand as group_name,
fb.product,
fb.pos,
fb.device,
case when lower(fb.device) like '%web%' then case when lower(fb.mobile_os) like '%ios%' or lower(fb.mobile_os) like '%android%' then 'mobile_web' else 'web' end else lower(fb.mobile_os) end as device_category,
-- fb.mobile_os as device_category,
fb.language,
ds.paid_unpaid,
fb.brand_nonbrand,
fb.objective,
-- spend
sum(fb.spend) as spend,
-- bookings,
0 as bookings,
-- revenue,
0 as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
0 as searches,
-- impressions,
sum(fb.impressions) as impressions,
-- clicks,
sum(fb.clicks) as clicks
, 0 as payment_visits
, 0 as details_visits
, 0 as traveller_visits
, 0 as uniq_searches
from
tajawal_bi.fact_facebook_spend fb
left outer join tajawal_bi.dim_spend_fb_accounts fba on fb.account_id = fba.account_id
left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on fb.dim_channel_id = ds.dim_channel_id
where lower(date_start) like '2016%' or lower(date_start) like '2017%' or lower(date_start) like '2018%' or lower(date_start) like '2019%' 
group by
date_start,
ds.channel_group,
ds.channel,
fba.brand,
fb.product,
fb.pos,
fb.device,
fb.mobile_os,
fb.language,
ds.paid_unpaid,
fb.brand_nonbrand,
fb.objective
;

-- Step 2.1 : Combine adjust fields with Social media Twitter
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
day_date as dim_date_id,
tw.channel as channel_group,
tw.sub_channel as channel,
tw.group_name as group_name,
product,
pos,
device,
case when device like '%web%' 
then 
  case when lower(tw.mobile_os) like '%ios%' or lower(tw.mobile_os) like '%android%' 
  then 'mobile_web' 
  else 'web' 
  end 
else tw.mobile_os 
end 
as device_category,
language,
ds.paid_unpaid,
tw.brand,
tw.objective,
-- spend
sum(spend) as spend,
-- bookings,
0 as bookings,
-- revenue
0 as revenue,
-- installs
0 as installs,
-- sessions
0 as sessions,
-- search
0 as searches,
-- impressions
sum(impressions) as impressions,
-- clicks,
sum(clicks) as clicks
, 0 as payment_visits
, 0 as details_visits
, 0 as traveller_visits
, 0 as uniq_searches
from tajawal_bi.fact_twitter_data tw
left outer join tajawal_bi.dim_spend_twitter_accounts twa on tw.accountid = twa.dim_account_id
left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on tw.dim_channel_id = ds.dim_channel_id
group by
day_date,
tw.channel,
tw.sub_channel,
tw.group_name,
product,
pos,
device,
tw.mobile_os,
language,
ds.paid_unpaid,
tw.brand,
tw.objective
;


-- Step 3 : Take union of adjust app data with GA app and web data
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
gad.gadate as dim_date_id,
ds.channel_group,
ds.channel,
gav.group_name as group_name,
gad.product,
gad.pos,
gav.device,
case when lower(gad.device_category) like '%desktop%' then 'web' else 
  case when lower(gad.device_category) like '%tablet%' or lower(gad.device_category) like '%mobile%' then 'mobile_web' 
    else 'na' 
  end 
end as device_category,
gad.language,
ds.paid_unpaid,
gad.brand_nonbrand,
gad.objective,
-- spend
case when lower(ds.channel) like '%trivago%' or lower(ds.channel_group) like '%affiliate%' or lower(ds.channel_group) like '%meta%' then 0 else sum( gad.adcost) end as spend,
-- bookings,
0 as bookings,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(case when gad.view_id in ('109503410', '130709589') then gad.transactions else 0 end) 
-- end as bookings,
-- revenue,
0 as revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' then 0.0
-- else
-- sum(case when gad.view_id in ('109503410', '130709589') then gad.transactions_revenue else 0 end) 
-- end as revenue,
-- installs,
0 as installs,
-- sessions,
sum(case when gad.view_id in ('109503410', '130709589') then gad.sessions else 0 end) as sessions,
-- search,
0 as searches,
-- impressions,
sum(case when gad.view_id in ('109503410', '130709589') then gad.impressions else 0 end) as impressions,
-- clicks,
sum(case when gad.view_id in ('109503410', '130709589') then gad.adclicks else 0 end) as clicks
, 0 as payment_visits
, 0 as details_visits
, 0 as traveller_visits
, 0 as uniq_searches
from
tajawal_bi.fact_google_analytics_ad gad
left outer join tajawal_bi.dim_spend_ga_views gav on gad.view_id = gav.dim_view_id
left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as dim_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', ''))) gasc on lower(regexp_replace(gad.sourcemedium, ' ', '')) = lower(regexp_replace(gasc.dim_sourcemedium, ' ', ''))
left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on gasc.dim_channel_id = ds.dim_channel_id
where gad.view_id in ('109503410', '130709589')
group by 
gad.gadate,
ds.channel_group,
ds.channel,
gav.group_name,
gad.product,
gad.pos,
gav.device,
case when lower(gad.device_category) like '%desktop%' then 'web' else 
  case when lower(gad.device_category) like '%tablet%' or lower(gad.device_category) like '%mobile%' then 'mobile_web' 
    else 'na' 
  end 
end,
gad.language,
ds.paid_unpaid,
gad.brand_nonbrand,
gad.objective;

-- Step 3.1 : Take in bookings and revenue from GA Transactions table
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
Select
fh.dim_bookingdate_id as dim_date_id,
ltrim(rtrim(dsc.channel_group)) as channel_group,
ltrim(rtrim(dsc.channel)) as channel,
gv.group_name,
fh.order_type as product,
-- ds.pos,
case when lower(coalesce(ds.pos, 'na')) = 'null' then 'na' else ds.pos end as pos,
gv.device,
case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'mobile_web' 
else case when lower(fh.device_category) like '%mweb%' then 'mweb' else 'web' end
end as device_category,
fh.language,
dsc.paid_unpaid,
'na' as brand_nonbrand,
'na' as objective,
case when lower(ltrim(rtrim(dsc.channel_group))) like '%meta%' or lower(ltrim(rtrim(dsc.channel_group))) like '%affiliate%' then
case when lower(ltrim(rtrim(dsc.channel))) like '%trivago%' then 0 else sum(case when lower(fga.metric_name) like '%cpa%' then fga.value else case when lower(fga.metric_name) like '%crr%' then fga.value * fh.payment_amount_usd / 100 else coalesce(fga.value, '0.0') end end) end 
else 0.0
end
as spend,
count(distinct(fh.id)) as bookings,
sum(fh.payment_amount_usd) as revenue,
0 as installs,
0 as sessions,
0 as searches,
0 as impressions,
0 as clicks
, 0 as payment_visits
, 0 as details_visits
, 0 as traveller_visits
, 0 as uniq_searches
from
(  
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, dim_language_id as language, device_category, dim_channel_id
   from tajawal_bi.fact_hotel_orders_final 
   where dim_status_id != 91 and dim_group_id in (1,2)
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
   union 
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, dim_language as language, device_category, dim_channel_id
   from tajawal_bi.fact_flight_orders_final 
   where dim_status_id != 91 and dim_group_id in (1,2)
   union
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id, order_type, iov as payment_amount_usd, dim_language_id as language, 'na' as device_category,  dim_channel_id
   from tajawal_bi.fact_package_orders_final
   where dim_status_id != 91 and dim_group_id in (1,2)
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
) fh 
inner join tajawal_bi.dim_groups dg on fh.dim_group_id = dg.group_id
---- inner join (
--   select fga.transaction_id, max(fga.view_id) as view_id, max(dsc.channel_group) as channel_group,
--   max(dsc.channel) as channel, max(paid_unpaid) as paid_unpaid
--   from
--   ( select transaction_id, max(sourcemedium) as sourcemedium, max(view_id) as view_id from tajawal_bi.fact_google_analytics_transactions_final where view_id in ('109503410', '130709589') group by transaction_id) fga
--   inner join 
--   (
--      select regexp_replace(lower(dsc.ga_sourcemedium), ' ', '') as ga_sourcemedium, max(dsch.channel_group) as channel_group, max(dsch.channel) as channel, max(paid_unpaid) as paid_unpaid, count(*) as total 
--      from tajawal_bi.dim_spend_ga_sourcemedium_channels dsc 
--      inner join (
--         select * from tajawal_bi.dim_spend_channels ds 
--         -- where lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%'
--      ) dsch on dsc.dim_channel_id = dsch.dim_channel_id
--      group by regexp_replace(lower(dsc.ga_sourcemedium), ' ', '')
--   ) dsc on regexp_replace(lower(fga.sourcemedium),' ','') = regexp_replace(lower(dsc.ga_sourcemedium),' ','')
--   group by fga.transaction_id 
--) fga 
inner join (
  select transaction_id, max(view_id) as view_id, max(metric_name) as metric_name, max(value) as value, max(dim_channel_id) as dim_channel_id 
  from tajawal_bi.fact_google_analytics_transactions_final where view_id in ('109503410', '130709589') group by transaction_id
) fga
on fh.order_no = fga.transaction_id 
left outer join tajawal_bi.dim_spend_channels dsc on fh.dim_channel_id = dsc.dim_channel_id
inner join tajawal_bi.dim_spend_ga_views gv on fga.view_id = gv.dim_view_id 
inner join tajawal_bi.dim_store ds on fh.dim_store_id = ds.store_id
Group by 
fh.dim_bookingdate_id,
ltrim(rtrim(dsc.channel_group)),
ltrim(rtrim(dsc.channel)),
gv.group_name,
fh.order_type,
-- ds.pos,
case when lower(coalesce(ds.pos, 'na')) = 'null' then 'na' else ds.pos end,
gv.device,
case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'mobile_web'
else case when lower(fh.device_category) like '%mweb%' then 'mweb' else 'web' end
end,
fh.language,
dsc.paid_unpaid;


-- Step 4 : Insert data for app searches.
-- INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
-- select
-- gsap.gadate as dim_date_id,
-- ds.channel_group,
-- ds.channel,
-- gav.group_name as group_name,
-- case when lower(gsap.product) like '%hotel%' then 'hotel' else case when lower(gsap.product) like '%flight%' then 'flight' else 'NA' end end as product,
-- case when lower(country) like '%saudi%' or lower(country) like '%sa%' then 'Saudi Arabia' else
--   case when lower(country) like '%arab%em%' or lower(country) like '%uae%' then 'United Arab Emirates' else 
--     case when lower(country) like '%kuwait%' then 'Kuwait' else 
--       case when lower(country) like '%bahrain%' then 'Bahrain' else 'NA'
--       end
--     end
--   end
-- end as pos,
-- gav.device as device,
-- -- galang as language,
-- case when lower(galang) like '%ar%' then 'ar' else 
--   case when lower(galang) like '%en%' then 'en' else 'NA'
--   end
-- end as language,
-- ds.paid_unpaid,
-- -- spend
-- 0 as spend,
-- -- bookings,
-- 0 as bookings,
-- -- revenue,
-- 0 as revenue,
-- -- installs,
-- 0 as installs,
-- -- sessions,
-- 0 as sessions,
-- -- search,
-- sum(total_events) as searches,
-- -- impressions,
-- 0 as impressions,
-- -- clicks,
-- 0 as clicks
-- , 0 as payment_visits
-- , 0 as details_visits
-- , 0 as traveller_visits
-- , sum(uniq_total_events) as uniq_searches
-- from
-- tajawal_bi.fact_google_analytics_search_app gsap
-- left outer join tajawal_bi.dim_spend_ga_views gav on gsap.view_id = gav.dim_view_id
-- left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', '')) ) gasc on lower(regexp_replace(gsap.sourcemedium, ' ', '')) = lower(regexp_replace(gasc.ga_sourcemedium, ' ', ''))
-- left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on gasc.dim_channel_id = ds.dim_channel_id
-- where lower(gsap.product) like '%hotel%' or lower(gsap.product) like '%flight%'
-- and gsap.view_id not in ('151627485')
-- group by 
-- gsap.gadate,
-- ds.channel_group,
-- ds.channel,
-- gav.group_name,
-- gsap.product,
-- galang,
-- country,
-- gav.device,
-- ds.paid_unpaid;

-- Step 5 : Insert data for web searches.
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
gsap.gadate as dim_date_id,
ds.channel_group,
ds.channel,
gav.group_name as group_name,
case when lower(gsap.product) like '%hotel%' then 'hotel' else case when lower(gsap.product) like '%flight%' then 'flight' else 'NA' end end as product,
-- gsap.country as pos,
case when lower(country) like '%saudi%' or lower(country) like '%sa%' then 'Saudi Arabia' else
  case when lower(country) like '%arab%em%' or lower(country) like '%uae%' then 'United Arab Emirates' else 
    case when lower(country) like '%kuwait%' then 'Kuwait' else 
      case when lower(country) like '%bahrain%' then 'Bahrain' else 'NA'
      end
    end
  end
end as pos,
gav.device as device,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'na'
  end
end as device_category,
-- gsap.language,
case when lower(gsap.language) like '%ar%' then 'ar' else 
  case when lower(gsap.language) like '%en%' then 'en' else 'NA'
  end
end as language,
ds.paid_unpaid,
'na' as brand_nonbrand,
'na' as objective,
-- spend
0 as spend,
-- bookings,
0 as bookings,
-- revenue,
0 as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
sum(pageviews) as searches,
-- impressions,
0 as impressions,
-- clicks,
0 as clicks
, 0 as payment_visits
, 0 as details_visits
, 0 as traveller_visits
, sum(uniqpageviews) as uniq_searches
from
tajawal_bi.fact_google_analytics_search_web gsap
left outer join tajawal_bi.dim_spend_ga_views gav on gsap.view_id = gav.dim_view_id
left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', '')) ) gasc on lower(regexp_replace(gsap.sourcemedium, ' ', '')) = lower(regexp_replace(gasc.ga_sourcemedium, ' ', ''))
left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on gasc.dim_channel_id = ds.dim_channel_id
-- where lower(gsap.product) like '%hotel%' or lower(gsap.product) like '%flight%'
where gsap.view_id not in ('151627485')
group by 
gsap.gadate,
ds.channel_group,
ds.channel,
gav.group_name,
gsap.product,
gsap.country,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'na'
  end
end,
gsap.language,
gav.device,
ds.paid_unpaid;


-- Step 5.1 : Insert data for GA page views
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
gsap.gadate as dim_date_id,
ds.channel_group,
ds.channel,
gav.group_name as group_name,
case when lower(gsap.product) like '%hotel%' then 'hotel' else case when lower(gsap.product) like '%flight%' then 'flight' else 'NA' end end as product,
-- gsap.country as pos,
case when lower(country) like '%saudi%' or lower(country) like '%sa%' then 'Saudi Arabia' else
  case when lower(country) like '%arab%em%' or lower(country) like '%uae%' then 'United Arab Emirates' else 
    case when lower(country) like '%kuwait%' then 'Kuwait' else 
      case when lower(country) like '%bahrain%' then 'Bahrain' else 'NA'
      end
    end
  end
end as pos,
gav.device as device,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'na'
  end
end as device_category,
-- gsap.language,
case when lower(gsap.galang) like '%ar%' then 'ar' else 
  case when lower(gsap.galang) like '%en%' then 'en' else 'NA'
  end
end as language,
ds.paid_unpaid,
'na' as brand_nonbrand,
'na' as objective,
-- spend
0 as spend,
-- bookings,
0 as bookings,
-- revenue,
0 as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
0 as searches,
-- impressions,
0 as impressions,
-- clicks,
0 as clicks
, sum(case when lower(pagetype) like '%payment%' then uniq_pageviews else 0 end) as payment_visits
, sum(case when lower(pagetype) like '%detail%' then uniq_pageviews else 0 end) as details_visits
, sum(case when lower(pagetype) like '%detail%' then uniq_pageviews else 0 end) as traveller_visits
, 0 as uniq_searches
from
tajawal_bi.fact_google_analytics_web_pageviews gsap
left outer join tajawal_bi.dim_spend_ga_views gav on gsap.view_id = gav.dim_view_id
left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', ''))) gasc on lower(regexp_replace(gsap.sourcemedium, ' ', '')) = lower(regexp_replace(gasc.ga_sourcemedium, ' ', ''))
left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on gasc.dim_channel_id = ds.dim_channel_id
-- where lower(gsap.product) like '%hotel%' or lower(gsap.product) like '%flight%'
where gsap.view_id not in ('151627485')
group by 
gsap.gadate,
ds.channel_group,
ds.channel,
gav.group_name,
-- gsap.pagetype,
gsap.product,
gsap.country,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'na'
  end
end,
gsap.galang,
gav.device,
ds.paid_unpaid;


-- Step 5.2 : Combine criteo data in this
INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
dayDate as dim_date_id,
ds.channel_group,
ds.channel,
group_name as group_name,
case when lower(product) like '%hotel%' then 'hotel' else case when lower(product) like '%flight%' then 'flight' else 'NA' end end as product,
-- case when lower(campaignname) like '%package%' or lower(campaignname) like '%\_p\_%' then 'package' else case when lower(product) like '%hotel%' then 'hotel' else case when lower(product) like '%flight%' then 'flight' else 'NA' end end end as product, 
coalesce(case when lower(pos) like '%saudi%' or lower(pos) like '%sa%' then 'Saudi Arabia' else
  case when lower(pos) like '%arab%em%' or lower(pos) like '%uae%' then 'United Arab Emirates' else
    case when lower(pos) like '%kuwait%' then 'Kuwait' else
      case when lower(pos) like '%bahrain%' then 'Bahrain' else 'NA'
      end
    end
  end
end, 'NA') as pos,
device as device,
case when device like '%web%' 
then 
  case when lower(cr.mobile_os) like '%ios%' or lower(cr.mobile_os) like '%android%' 
  then 'mobile_web' 
  else 'web' 
  end 
else cr.mobile_os 
end 
as device_category,
case when lower(language) like '%ar%' then 'ar' else
  case when lower(language) like '%en%' then 'en' else 'NA'
  end
end as language,
ds.paid_unpaid,
'na' as brand_nonbrand,
'na' as objective,
-- spend
sum(cost) as spend,
-- bookings,
0 as bookings,
-- revenue,
0 as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
0 as searches,
-- impressions,
sum(impressions) as impressions,
-- clicks,
sum(click) as clicks
, 0 as payment_visits
, 0 as details_visits
, 0 as traveller_visits
, 0 as uniq_searches
from
tajawal_bi.fact_criteo_data cr
left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on cr.dim_channel_id = ds.dim_channel_id
-- where lower(gsap.product) like '%hotel%' or lower(gsap.product) like '%flight%'
group by
dayDate,
ds.channel_group,
ds.channel,
cr.group_name,
-- gsap.pagetype,
product,
-- case when lower(campaignname) like '%package%' or lower(campaignname) like '%\_p\_%' then 'package' else case when lower(product) like '%hotel%' then 'hotel' else case when lower(product) like '%flight%' then 'flight' else 'NA' end end end,
pos,
language,
device,
cr.mobile_os,
ds.paid_unpaid;

-- == Insert Trivago Cost data into spend consolidation

INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select 
-- CASE WHEN q1.dim_bookingdate_id='null' then q2.day_date end as dim_date_id, 
COALESCE(q2.day_date, q1.dim_bookingdate_id) as dim_date_id,
-- q1.dim_channel_id as , 
dsc.channel_group,
dsc.channel as channel,
case when q1.dim_group_id=1 then 'Tajawal' when q1.dim_group_id=2 then 'Almosafer' else q2.group_name end as group_name,
'hotel' as product,
CASE WHEN q1.pos='null' THEN 'NA' ELSE q1.pos END,
CASE WHEN q1.device='null' THEN 'NA' ELSE q1.device END,
CASE WHEN q1.device_category='null' THEN 'NA' ELSE q1.device_category END,
CASE WHEN q1.language='null' THEN 'NA' ELSE q1.language END,
dsc.paid_unpaid,
'na' as brand_nonbrand,
'na' as objective,
CASE WHEN q2.cost * q1.revenue_ratio='null' then q2.cost ELSE q2.cost * q1.revenue_ratio end as spend, 
-- q2.bookings * q1.bookings_ratio as bookings, 
0 as bookings,
0 as revenue,
0 as installs,
0 as sessions,
0 as searches,
0 as impressions,
0 as clicks,
0 as payment_visits,
0 as details_visits,
0 as traveller_visits,
0 as uniq_searches
from
(
    select 
    q1.dim_bookingdate_id,
    q1.dim_channel_id,
    q1.dim_group_id,
    q1.pos,
    q1.device,
    q1.device_category,
    q1.language,
    q1.bookings,
    q1.revenue,
    q1.bookings/q2.bookings as bookings_ratio,
    q1.revenue/q2.revenue as revenue_ratio 
    from 
    (
        select  count(distinct(id)) bookings, 
        sum(ibv) as revenue,
        case when d.`type` like '%mobile%' then 'app' else d.`type` end as device,
        case when d.`type` like '%mobile%' then d.mobile_os else device_category end device_category,
        case when dim_language_id like '%ar%' then 'ar' when dim_language_id like '%en%' then 'en' end as language,
        dim_bookingdate_id, 
        dim_channel_id, 
        dim_group_id,
        d.pos
        from tajawal_bi.fact_hotel_orders_final h
        inner join tajawal_bi.dim_store d on h.dim_store_id=d.store_id
        group by dim_bookingdate_id, dim_channel_id, dim_group_id, d.pos, d.`type`,
        case when d.`type` like '%mobile%' then d.mobile_os else device_category end, 
        case when dim_language_id like '%ar%' then 'ar' when dim_language_id like '%en%' then 'en' end
    ) q1
    inner join
    (
        select 
        count(distinct(id)) as bookings, 
        sum(ibv) as revenue,
        dim_bookingdate_id, 
        dim_channel_id, 
        dim_group_id 
        from 
        tajawal_bi.fact_hotel_orders_final
        group by dim_bookingdate_id, dim_channel_id, dim_group_id
    ) q2
   on q1.dim_bookingdate_id=q2.dim_bookingdate_id and q1.dim_channel_id=q2.dim_channel_id and q1.dim_group_id=q2.dim_group_id
) q1
right outer join 
(
    select sum(bookings) as bookings, sum(cost) as cost,
    day_date, dim_channel_id, group_name 
    from tajawal_bi.fact_trivago_hotel_data 
    group by day_date, dim_channel_id, group_name
) q2
on q1.dim_bookingdate_id=q2.day_date and q1.dim_channel_id = q2.dim_channel_id and
case when q1.dim_group_id=1 then 'Tajawal' when q1.dim_group_id=2 then 'Almosafer' end = q2.group_name
left outer join 
(
    select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel,
    max(paid_unpaid) as paid_unpaid
    from tajawal_bi.dim_spend_channels
    group by dim_channel_id
) dsc on q2.dim_channel_id = dsc.dim_channel_id; 


INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select 
q1.dim_bookingdate_id as dim_date_id, 
-- q1.dim_channel_id as , 
dsc.channel_group,
dsc.channel,
case when q1.dim_group_id=1 then 'Tajawal' when q1.dim_group_id=2 then 'Almosafer' end as group_name,
'hotel' as product,
q1.pos,
q1.device,
q1.device_category,
q1.language,
dsc.paid_unpaid,
'na' as brand_nonbrand,
'na' as objective,
q2.cost * q1.revenue_ratio as spend, 
-- q2.bookings * q1.bookings_ratio as bookings, 
0 as bookings,
0 as revenue,
0 as installs,
0 as sessions,
0 as searches,
0 as impressions,
0 as clicks,
0 as payment_visits,
0 as details_visits,
0 as traveller_visits,
0 as uniq_searches
from
(
    select 
    q1.dim_bookingdate_id,
    q1.dim_channel_id,
    q1.dim_group_id,
    q1.pos,
    q1.device,
    q1.device_category,
    q1.language,
    q1.bookings,
    q1.revenue,
    q1.bookings/q2.bookings as bookings_ratio,
    q1.revenue/q2.revenue as revenue_ratio 
    from 
    (
        select  count(distinct(id)) bookings, 
        sum(ibv) as revenue,
        case when d.`type` like '%app%' then 'app' else d.`type` end as device,
        case when d.`type` like '%app%' then d.mobile_os else device_category end device_category,
        case when dim_language_id like '%ar%' then 'ar' when dim_language_id like '%en%' then 'en' end as language,
        dim_bookingdate_id, 
        dim_channel_id, 
        dim_group_id,
        d.pos
        from tajawal_bi.fact_hotel_orders_final h
        inner join tajawal_bi.dim_store d on h.dim_store_id=d.store_id
        group by dim_bookingdate_id, dim_channel_id, dim_group_id, d.pos, d.`type`,
        case when d.`type` like '%app%' then d.mobile_os else device_category end, 
        case when dim_language_id like '%ar%' then 'ar' when dim_language_id like '%en%' then 'en' end
    ) q1
    inner join
    (
        select 
        count(distinct(id)) as bookings, 
        sum(ibv) as revenue,
        dim_bookingdate_id, 
        dim_channel_id, 
        dim_group_id 
        from 
        tajawal_bi.fact_hotel_orders_final
        group by dim_bookingdate_id, dim_channel_id, dim_group_id
    ) q2
   on q1.dim_bookingdate_id=q2.dim_bookingdate_id and q1.dim_channel_id=q2.dim_channel_id and q1.dim_group_id=q2.dim_group_id
) q1
inner join 
(
    select sum(bookings) as bookings, sum(cost) as cost,
    day_date, dim_channel_id, group_name 
    from tajawal_bi.fact_trivago_hotel_data 
    group by day_date, dim_channel_id, group_name
) q2
on q1.dim_bookingdate_id=q2.day_date and q1.dim_channel_id = q2.dim_channel_id and
case when q1.dim_group_id=1 then 'Tajawal' when q1.dim_group_id=2 then 'Almosafer' end = q2.group_name
left outer join 
(
    select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel,
    max(paid_unpaid) as paid_unpaid
    from tajawal_bi.dim_spend_channels
    group by dim_channel_id
) dsc on q2.dim_channel_id = dsc.dim_channel_id;

-- Add BING data into spend

INSERT INTO ${hiveDB}.fact_spend_data_channelwise_inter
select
day_Date as dim_date_id,
ds.channel_group,
ds.channel,
group_name as group_name,
case when lower(product) like '%hotel%' then 'hotel' 
  else case when lower(product) like '%flight%' then 'flight' 
  else case when lower(product) like '%both%' then 'both'
  else case when lower(product) like '%package%' then 'package'
  else 'NA' end end end end as product,
coalesce(case when lower(pos) like '%saudi%' or lower(pos) like '%sa%' then 'Saudi Arabia' else
  case when lower(pos) like '%arab%em%' or lower(pos) like '%uae%' then 'United Arab Emirates' else
    case when lower(pos) like '%kuwait%' then 'Kuwait' else
      case when lower(pos) like '%bahrain%' then 'Bahrain' else 
        case when lower(pos) like '%qatar%' then 'Qatar' else 
          case when lower(pos) like '%egypt%' then 'Egypt' else 
          case when lower(pos) like '%oman%' then 'Oman' else 'others'
          end 
          end 
         end
      end
    end
  end
end, 'NA') as pos,
device as device,
'NA' device_category,
case when lower(language) like '%ar%' then 'ar' else
  case when lower(language) like '%en%' then 'en' else 'NA'
  end
end as language,
ds.paid_unpaid,
'NA' as brand_nonbrand,
'NA' as objective,
-- spend
sum(spend) as spend,
-- bookings,
0 as bookings,
-- revenue,
0 as revenue,
-- installs,
0 as installs,
-- sessions,
0 as sessions,
-- search,
0 as searches,
-- impressions,
sum(impressions) as impressions,
-- clicks,
sum(clicks) as clicks
, 0 as payment_visits
, 0 as details_visits
, 0 as traveller_visits
, 0 as uniq_searches
from
tajawal_bi.fact_bing_data cr
left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel, max(paid_unpaid) as paid_unpaid 
  from tajawal_bi.dim_spend_channels group by dim_channel_id) ds on cr.dim_channel_id = ds.dim_channel_id
-- where lower(gsap.product) like '%hotel%' or lower(gsap.product) like '%flight%'
group by
day_Date,
ds.channel_group,
ds.channel,
cr.group_name,
-- gsap.pagetype,
product,
pos,
language,
device,
--cr.mobile_os,
ds.paid_unpaid;

-- Step 6 : Combine all toghether
DROP TABLE IF EXISTS ${hiveDB}.fact_spend_data_channelwise_inter2;


-- Step 6 : Combine all toghether
DROP TABLE IF EXISTS ${hiveDB}.fact_spend_data_channelwise_inter2;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_spend_data_channelwise_inter2 as
select 
dim_date_id,
lower(ltrim(rtrim(channel_group))) as channel_group,
lower(ltrim(rtrim(channel))) as channel,
coalesce(lower(group_name), 'NA') as group_name,
case when lower(product) like '%na%' then 'NA' else lower(product) end as product,
case when lower(pos) like 'uae' then 'United Arab Emirates' else 
  case when lower(pos) like 'na' then 'others' else 
    case when lower(pos) like 'saudi arabia' then 'Saudi Arabia' else
      case when lower(pos) like 'baharain' then 'Bahrain' else
        case when lower(pos) like 'kuwait' then 'Kuwait' else
          case when lower(pos) like '%egypt%' then 'Egypt' else
            ltrim(rtrim(coalesce(pos, 'others'))) 
          end
        end
      end
    end
  end 
end as pos,
coalesce(case when lower(device) like '%mobile%' then 'app' else lower(device) end, 'NA') as device,
case when lower(ltrim(rtrim(device_category))) = 'mobile web' then 'mobile_web' else lower(device_category) end as device_category,
case when lower(language) like '%na%' then 'NA' else lower(language) end as language,
case when ltrim(rtrim(lower(coalesce(paid_unpaid, 'other')))) like '' then 'other' else ltrim(rtrim(lower(coalesce(paid_unpaid, 'other')))) end as paid_unpaid
, lower(case when lower(ltrim(rtrim(channel_group))) like '%sem%' then brand_nonbrand  else 
    case when lower(ltrim(rtrim(channel_group))) like '%mobile%network%' then 'Non Brand'  else
     case when lower(ltrim(rtrim(channel_group))) like '%affiliate%' then 'Non Brand' else
      case when lower(ltrim(rtrim(channel_group))) like '%retargeting%' then  'Non Brand' else
       case when lower(ltrim(rtrim(channel_group))) like '%social%media%' then 'Non Brand'  else
        case when lower(ltrim(rtrim(channel_group))) like '%meta%' then 'Non Brand'  else
         case when lower(ltrim(rtrim(channel_group))) like '%seo%' then  'Non Brand' else
          case when lower(ltrim(rtrim(channel_group))) like '%direct%' then 'Non Brand'  else
           case when lower(ltrim(rtrim(channel_group))) like '%crm%' then 'Brand' else 
            case when lower(ltrim(rtrim(channel_group))) like '%display%' then 'Brand' else 
             case when lower(ltrim(rtrim(channel_group))) like '%organic%search%' then 'Non Brand' else
              case when lower(ltrim(rtrim(channel_group))) like '%other%' then 'Non Brand' else
                case when lower(ltrim(rtrim(channel_group))) like '%re%engagement%' then 'Non Brand' else 
                  case when lower(ltrim(rtrim(channel_group))) like '%partnerships%' then 'Non Brand' else 
                    case when lower(ltrim(rtrim(channel_group))) like '%uac%' then 'Non Brand' else 
                     case when lower(ltrim(rtrim(channel_group))) like '%\\n%' then 'Non Brand' else 'Non Brand'
                      end
                     end
                    end
                   end
                  end
                 end
                end
              end
             end
            end
           end
          end
         end
        end
       end 
      end) as brand_nonbrand
  ,lower(case when lower(ltrim(rtrim(channel_group))) like '%sem%' then 'Aquisition'  else 
    case when lower(ltrim(rtrim(channel_group))) like '%mobile%network%' then 'Aquisition'  else
     case when lower(ltrim(rtrim(channel_group))) like '%affiliate%' then 'Aquisition' else
      case when lower(ltrim(rtrim(channel_group))) like '%retargeting%' then  'Retargeting' else
       case when lower(ltrim(rtrim(channel_group))) like '%social%media%' then  objective  else
        case when lower(ltrim(rtrim(channel_group))) like '%meta%' then 'Aquisition'  else
         case when lower(ltrim(rtrim(channel_group))) like '%seo%' then  'Aquisition' else
          case when lower(ltrim(rtrim(channel_group))) like '%direct%' then 'NA'  else
           case when lower(ltrim(rtrim(channel_group))) like '%crm%' then 'Retargeting'  else
             case when lower(ltrim(rtrim(channel_group))) like '%display%' then 'Aquisition' else
              case when lower(ltrim(rtrim(channel_group))) like '%organic%search%' then 'Aquisition' else
               case when lower(ltrim(rtrim(channel_group))) like '%other%' then 'Aquisition' else
                case when lower(ltrim(rtrim(channel_group))) like '%re%engagement%' then 'Retargeting' else
                  case when lower(ltrim(rtrim(channel_group))) like '%partnerships%' then 'Aquisition' else
                    case when lower(ltrim(rtrim(channel_group))) like '%uac%' then 'Aquisition' else
                     case when lower(ltrim(rtrim(channel_group))) like '%\\n%' then 'Aquisition' else 'Aquisition'
                      end
                     end
                    end
                   end
                  end
                 end
                end
               end
             end
            end
           end
          end
         end
        end
       end 
      end) as objective ,
-- spend
case when lower(ltrim(rtrim(channel))) like '%trivago%' then sum(spend) else 
  sum(case when lower(ltrim(rtrim(channel_group))) like 'meta' then revenue * 0.02 else spend end) 
end
as spend,
-- bookings,
sum(bookings) as bookings,
-- revenue,
sum(revenue) as revenue,
-- installs,
sum(installs) as installs,
-- sessions,
sum(sessions) as sessions,
-- search,
sum(searches) as searches,
-- impressions,
sum(impressions) as impressions,
-- clicks,
sum(clicks) as clicks
, sum(payment_visits) as payment_visits
, sum(details_visits) as details_visits
, sum(traveller_visits) as traveller_visits
, sum(uniq_searches) as uniq_searches
from
${hiveDB}.fact_spend_data_channelwise_inter
group by 
dim_date_id,
lower(ltrim(rtrim(channel_group))),
lower(ltrim(rtrim(channel))),
coalesce(lower(group_name), 'NA'),
product,
pos,
device,
case when lower(ltrim(rtrim(device_category))) = 'mobile web' then 'mobile_web' else lower(device_category) end,
language,
paid_unpaid,
lower(case when lower(ltrim(rtrim(channel_group))) like '%sem%' then brand_nonbrand  else  
    case when lower(ltrim(rtrim(channel_group))) like '%mobile%network%' then 'Non Brand'  else
     case when lower(ltrim(rtrim(channel_group))) like '%affiliate%' then 'Non Brand' else
      case when lower(ltrim(rtrim(channel_group))) like '%retargeting%' then  'Non Brand' else
       case when lower(ltrim(rtrim(channel_group))) like '%social%media%' then 'Non Brand'  else
        case when lower(ltrim(rtrim(channel_group))) like '%meta%' then 'Non Brand'  else
         case when lower(ltrim(rtrim(channel_group))) like '%seo%' then  'Non Brand' else
          case when lower(ltrim(rtrim(channel_group))) like '%direct%' then 'Non Brand'  else
           case when lower(ltrim(rtrim(channel_group))) like '%crm%' then 'Brand' else 
            case when lower(ltrim(rtrim(channel_group))) like '%display%' then 'Brand' else
             case when lower(ltrim(rtrim(channel_group))) like '%organic%search%' then 'Non Brand' else
              case when lower(ltrim(rtrim(channel_group))) like '%other%' then 'Non Brand' else
                case when lower(ltrim(rtrim(channel_group))) like '%re%engagement%' then 'Non Brand' else
                  case when lower(ltrim(rtrim(channel_group))) like '%partnerships%' then 'Non Brand' else
                    case when lower(ltrim(rtrim(channel_group))) like '%uac%' then 'Non Brand' else
                     case when lower(ltrim(rtrim(channel_group))) like '%\\n%' then 'Non Brand' else 'Non Brand'
                      end
                     end
                    end
                   end
                  end
                 end
                end 
               end
             end
            end
           end
          end
         end
        end
       end
      end)
  ,lower(case when lower(ltrim(rtrim(channel_group))) like '%sem%' then 'Aquisition'  else
    case when lower(ltrim(rtrim(channel_group))) like '%mobile%network%' then 'Aquisition'  else
     case when lower(ltrim(rtrim(channel_group))) like '%affiliate%' then 'Aquisition' else
      case when lower(ltrim(rtrim(channel_group))) like '%retargeting%' then  'Retargeting' else
       case when lower(ltrim(rtrim(channel_group))) like '%social%media%' then  objective  else
        case when lower(ltrim(rtrim(channel_group))) like '%meta%' then 'Aquisition'  else
         case when lower(ltrim(rtrim(channel_group))) like '%seo%' then  'Aquisition' else
          case when lower(ltrim(rtrim(channel_group))) like '%direct%' then 'NA'  else
           case when lower(ltrim(rtrim(channel_group))) like '%crm%' then 'Retargeting'  else 
             case when lower(ltrim(rtrim(channel_group))) like '%display%' then 'Aquisition' else
              case when lower(ltrim(rtrim(channel_group))) like '%organic%search%' then 'Aquisition' else
               case when lower(ltrim(rtrim(channel_group))) like '%other%' then 'Aquisition' else
                case when lower(ltrim(rtrim(channel_group))) like '%re%engagement%' then 'Retargeting' else
                  case when lower(ltrim(rtrim(channel_group))) like '%partnerships%' then 'Aquisition' else
                    case when lower(ltrim(rtrim(channel_group))) like '%uac%' then 'Aquisition' else
                     case when lower(ltrim(rtrim(channel_group))) like '%\\n%' then 'Aquisition' else 'Aquisition'
                      end
                     end
                    end
                   end
                  end
                 end
                end
              end
             end
            end
           end
          end
         end
        end
       end
      end)
;

drop table if exists ${hiveDB}.fact_spend_data_channelwise;
alter table ${hiveDB}.fact_spend_data_channelwise_inter2 rename to ${hiveDB}.fact_spend_data_channelwise;
