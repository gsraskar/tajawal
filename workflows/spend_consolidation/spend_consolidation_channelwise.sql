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
-- sum(adj.cost) 
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
sum(hotel_spend) as spend,
-- bookings, exclude bookings and revenue for channel_groups : Meta, Organic Search, Partnership, Direct, SEO,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then sum(adj.hotel_events)
-- then 
sum(adj.revenue_events) - sum(adj.flight_events)
-- else 0.0
-- end 
as bookings,
-- revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then sum(adj.hotel_revenue)
sum(adj.revenue) - sum(adj.flight_revenue)
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
sum(flight_spend) as spend,
-- bookings, exclude bookings and revenue for channel_groups : Meta, Organic Search, Partnership, Direct, SEO,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then 
sum(adj.flight_events)
-- else 0.0 
-- end 
as bookings,
-- revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then 
sum(adj.flight_revenue)
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
sum(adj.holidays_revenue_events)
-- else 0.0 
-- end 
as bookings,
-- revenue,
-- case when lower(ds.channel_group) like '%meta%' or lower(ds.channel_group) like '%organic%search%' or lower(ds.channel_group) like '%seo%' or lower(ds.channel_group) like '%partnerships%' or lower(ds.channel_group) like '%direct%' or lower(ds.channel_group) like '%crm%' 
-- then 
sum(adj.holidays_revenue)
-- else 0.0 
-- end 
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
left outer join ${hiveDB}.dim_spend_fb_accounts fba on fb.account_id = fba.account_id
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
case when lower(ds.channel) like '%trivago%' then 0 else sum( gad.adcost) end as spend,
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
case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'mobile_web' else 'web'
end as device_category,
fh.language,
dsc.paid_unpaid,
'na' as brand_nonbrand,
'na' as objective,
sum(case when lower(fga.metric_name) like '%cpa%' then fga.value else case when lower(fga.metric_name) like '%crr%' then fga.value * fh.payment_amount_usd / 100 else fga.value end end) as spend,
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
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id, order_type, iov as payment_amount_usd, dim_language_id as language, 'na' as device_category, '-1' as dim_channel_id
   from ${hiveDB}.fact_package_orders_final
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
inner join (select transaction_id, max(view_id) as view_id, max(metric_name) as metric_name, max(value) as value from tajawal_uat_bi.fact_google_analytics_transactions_final_incr where view_id in ('109503410', '130709589') group by transaction_id) fga
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
case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'mobile_web' else 'web'
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

-- ===============
-- ===== Create new CRM Table
-- ===============

--===CRM Table data

-- Query 1, Part 1 : Include GA Transactions data into it
Drop table if exists tajawal_bi.inter_fact_crm_spend_consolidation;
Create table if not exists 
tajawal_bi.inter_fact_crm_spend_consolidation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
Select
'notarget' as recordtype,
gv.group_name,
fh.dim_bookingdate_id as `booking_date`,
gv.device,
gv.mobile_os,
case when lower(coalesce(ds.pos, 'na')) = 'null' then 'na' else ds.pos end as pos,
fh.order_type as product,
ltrim(rtrim(fga.channel_group)) as channel_group,
ltrim(rtrim(fga.channel)) as channel,
0.0 as revenue_target,
0.0 as booking_target,
0.0 as aov_target,
0.0 as share_of_revenue_target,
count(distinct(fh.order_no)) as bookings,
sum(fh.payment_amount_usd) as revenue,
0.0 as share_of_revenue,
sum(fh.payment_amount_usd)  / count(distinct(fh.order_no)) as aov
from
(  
   select order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd
   from tajawal_bi.fact_hotel_orders_final 
   where dim_status_id != 91
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
   union 
   select order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd
   from tajawal_bi.fact_flight_orders_final 
   where dim_status_id != 91
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
) fh 
inner join tajawal_bi.dim_groups dg on fh.dim_group_id = dg.group_id
inner join (
   select fga.transaction_id, max(fga.view_id) as view_id, max(dsc.channel_group) as channel_group,
   max(dsc.channel) as channel
   from
   ( select * from tajawal_bi.fact_google_analytics_transactions_final where view_id in ('109503410', '130709589') ) fga
   inner join 
   (
      select dsc.ga_sourcemedium, max(dsch.channel_group) as channel_group, max(dsch.channel) as channel, count(*) as total 
      from tajawal_bi.dim_spend_ga_sourcemedium_channels dsc 
      inner join (
         select * from tajawal_bi.dim_spend_channels where lower(ltrim(rtrim(channel_group))) = 'crm'
      ) dsch on dsc.dim_channel_id = dsch.dim_channel_id
      group by dsc.ga_sourcemedium
   ) dsc on regexp_replace(lower(fga.sourcemedium),' ','') = regexp_replace(lower(dsc.ga_sourcemedium),' ','')
   group by fga.transaction_id 
) fga 
on fh.order_no = fga.transaction_id 
inner join tajawal_bi.dim_spend_ga_views gv on fga.view_id = gv.dim_view_id 
inner join tajawal_bi.dim_store ds on fh.dim_store_id = ds.store_id
Group by 
gv.group_name,
fh.dim_bookingdate_id,
gv.device,
gv.mobile_os,
-- ds.pos,
case when lower(coalesce(ds.pos, 'na')) = 'null' then 'na' else ds.pos end,
fh.order_type,
ltrim(rtrim(fga.channel_group)),
ltrim(rtrim(fga.channel));

-- Query 1, Part 2.1 : Include Flight Adjust data into it
insert into 
tajawal_bi.inter_fact_crm_spend_consolidation
Select
'notarget' as recordtype,
B.group_name,
A.`date` as `booking_date`,
'app' as device,
case when lower(B.app_name) like 'ios' then 'ios' else 
  case when lower(B.app_name) like 'android' then 'android' else 'na'
  end
end as mobile_os,
A.pos,
'flight' as product,
ltrim(rtrim(ds.channel_group)) as channel_group,
ltrim(rtrim(ds.channel)) as channel,
0.0 as revenue_target,
0.0 as booking_target,
0.0 as aov_target,
0.0 as share_of_revenue_target,
sum(A.flight_events) as bookings,
sum(A.flight_revenue) as revenue,
0.0 as share_of_revenue,
sum(A.flight_revenue)  / sum(A.flight_events) as aov

-- select substr(`date`, 1, 6) as month, ds.channel_group, ds.channel, sum(cost), sum(A.revenue_events), sum(revenue)
from
tajawal_bi.fact_adjust_data_multi_app_final A
inner join tajawal_bi.dim_spend_adjust_apps B on lower(ltrim(rtrim(A.app_token))) = lower(ltrim(rtrim(B.dim_app_id)))
inner join (
   select lower(regexp_replace(A.adjust_network_name, ' ', '')) as adjust_network_name, 
   max(channel_group) as channel_group, max(channel) as channel 
   from tajawal_bi.dim_spend_adjust_network_channels A inner join 
   tajawal_bi.dim_spend_channels B on A.dim_channel_id = B.dim_channel_id 
   where lower(ltrim(rtrim(B.channel_group))) = 'crm' 
   group by lower(regexp_replace(A.adjust_network_name, ' ', ''))
) ds
on lower(regexp_replace(A.network, ' ', '')) = adjust_network_name
-- group by substr(`date`, 1, 6), ds.channel_group, ds.channel order by month, ds.channel_group, ds.channel asc;

group by B.group_name,
A.`date`,
case when lower(B.app_name) like 'ios' then 'ios' else 
  case when lower(B.app_name) like 'android' then 'android' else 'na'
  end
end,
A.pos,
ltrim(rtrim(ds.channel_group)),
ltrim(rtrim(ds.channel));

-- Query 1, Part 2.2 : Include Hotel Adjust data into it

insert into 
tajawal_bi.inter_fact_crm_spend_consolidation
Select
'notarget' as recordtype,
B.group_name,
A.`date` as `booking_date`,
'app' as device,
case when lower(B.app_name) like 'ios' then 'ios' else 
  case when lower(B.app_name) like 'android' then 'android' else 'na'
  end
end as mobile_os,
A.pos,
'hotel' as product,
ltrim(rtrim(ds.channel_group)) as channel_group,
ltrim(rtrim(ds.channel)) as channel,
0.0 as revenue_target,
0.0 as booking_target,
0.0 as aov_target,
0.0 as share_of_revenue_target,
-- sum(A.hotel_events) as bookings,
sum(A.revenue_events) - sum(A.flight_events) as bookings,
-- sum(A.hotel_revenue) as revenue,
sum(A.revenue) - sum(A.flight_revenue) as revenue,
0.0 as share_of_revenue,
-- sum(A.hotel_revenue)  / sum(A.hotel_events) as aov
(sum(A.revenue) - sum(A.flight_revenue)) / (sum(A.revenue_events) - sum(A.flight_events)) as aov

-- select substr(`date`, 1, 6) as month, ds.channel_group, ds.channel, sum(cost), sum(A.revenue_events), sum(revenue)
from
tajawal_bi.fact_adjust_data_multi_app_final A
inner join tajawal_bi.dim_spend_adjust_apps B on lower(ltrim(rtrim(A.app_token))) = lower(ltrim(rtrim(B.dim_app_id)))
inner join (
   select lower(regexp_replace(A.adjust_network_name, ' ', '')) as adjust_network_name, 
   max(channel_group) as channel_group, max(channel) as channel 
   from tajawal_bi.dim_spend_adjust_network_channels A inner join 
   tajawal_bi.dim_spend_channels B on A.dim_channel_id = B.dim_channel_id 
   where lower(ltrim(rtrim(B.channel_group))) = 'crm' 
   group by lower(regexp_replace(A.adjust_network_name, ' ', ''))
) ds
on lower(regexp_replace(A.network, ' ', '')) = adjust_network_name
-- group by substr(`date`, 1, 6), ds.channel_group, ds.channel order by month, ds.channel_group, ds.channel asc;

group by B.group_name,
A.`date`,
case when lower(B.app_name) like 'ios' then 'ios' else 
  case when lower(B.app_name) like 'android' then 'android' else 'na'
  end
end,
A.pos,
ltrim(rtrim(ds.channel_group)),
ltrim(rtrim(ds.channel));

-- == Verification query : select substr(booking_date, 1, 6) as month, channel_group, channel, sum(bookings),sum(booking_targets) from tajawal_bi.fact_crm_spend_consolidation group by substr(booking_date, 1, 6), channel_group, channel order by month asc;

-- ===Query 2 : Pre-calculate metrices at pos, device, os level.

Drop table if exists tajawal_bi.inter2_fact_crm_spend_consolidation;
Create table tajawal_bi.inter2_fact_crm_spend_consolidation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
select * from tajawal_bi.inter_fact_crm_spend_consolidation 
Union
Select 
'target' as recordtype,
group_name,
booking_date,
'all' as device,
'all' as mobile_os,
'all' as pos,
product,
channel_group,
'all' as channel,
0.0 as revenue_target,
0.0 as booking_target,
0.0 as aov_target,
0.0 as share_of_revenue_target,
sum(bookings) as bookings,
sum(revenue) as revenue,
0.0 as share_of_revenue,
sum(revenue)  / sum(bookings) as aov
From tajawal_bi.inter_fact_crm_spend_consolidation 
Group by 
group_name,
booking_date,
product,
channel_group;
Drop table tajawal_bi.inter_fact_crm_spend_consolidation;
Alter table tajawal_bi.inter2_fact_crm_spend_consolidation rename to tajawal_bi.inter_fact_crm_spend_consolidation;

--=======Query 3 : Combine Spend into final table

Drop table if exists tajawal_bi.inter2_fact_crm_spend_consolidation;
Create table tajawal_bi.inter2_fact_crm_spend_consolidation  -- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
as 
Select
recordtype,
group_name,
booking_date,
device,
mobile_os,
case when lower(pos) like '%united%' then 'uae' else pos end as pos,
a.product,
channel_group,
a.channel as channel,
COALESCE(dsp.ibv_target,0.0) as ibv_target,
COALESCE(dsp.bookings_target,0.0) as bookings_target,
COALESCE(dsp.ibv_target / dsp.bookings_target, 0.0) as aov_target,
COALESCE(dsp.revenue_share_target,0.0) as share_of_revenue_target,
bookings,
revenue,
share_of_revenue,
aov
From
tajawal_bi.inter_fact_crm_spend_consolidation a
left outer join 
(
   select dim_date, product, brand, lower(channel) as channel, 
   -- case when lower(sub_channel) like '%push%' then 'Push'
   --   else case when lower(sub_channel) like '%email%' then 'Email'
   --    else case when lower(sub_channel) like '%sms%' then 'SMS'
   --      else case when lower(sub_channel) like '%insider%' then 'Insider' else 'na' end
   --    end
   --   end
   -- end as sub_channel,
   sum(ibv) as ibv_target, sum(bookings_target) as bookings_target, sum(revenue_share_target) as revenue_share_target 
   from googlesheets.dim_spend_daily_targets 
   where (lower(product) like '%hotel%' or lower(product) like '%flight%') and lower(channel) like 'crm' 
   group by dim_date, product, brand, lower(channel) 
   -- case when lower(sub_channel) like '%push%' then 'Push'
   --   else case when lower(sub_channel) like '%email%' then 'Email'
   --    else case when lower(sub_channel) like '%sms%' then 'SMS'
   --      else case when lower(sub_channel) like '%insider%' then 'Insider' else 'na' end
   --    end
   --   end
   -- end
) dsp 
on a.booking_date = dsp.dim_date 
and a.product = case when lower(dsp.product) like '%hotel%' then 'hotel' else case when lower(dsp.product) like '%flight%' then 'flight' else 'na' end end 
and a.device = 'all' and a.mobile_os = 'all' and a.pos = 'all' 
and a.channel = 'all'
and ltrim(rtrim(lower(a.group_name))) = ltrim(rtrim(lower(dsp.brand)))
-- and ltrim(rtrim(lower(a.channel))) = ltrim(rtrim(lower(dsp.sub_channel)))
;
Drop table if exists tajawal_bi.fact_crm_spend_consolidation;
Alter table tajawal_bi.inter2_fact_crm_spend_consolidation rename to tajawal_bi.fact_crm_spend_consolidation;



truncate table tajawal_bi.dim_spend_daily_targets;
insert into tajawal_bi.dim_spend_daily_targets select * from googlesheets.dim_spend_daily_targets;

 truncate table tajawal_bi.dim_spend_monthly_targets_final; insert into tajawal_bi.dim_spend_monthly_targets_final select * from googlesheets.dim_spend_monthly_targets_final;

drop table tajawal_bi.fact_per_route_orders_searches;
create table tajawal_bi.fact_per_route_orders_searches ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
as
select case when A.dim_channel_id='\N' then NULL else A.dim_channel_id end as dim_channel_id,
case when lower(A.pos) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(A.pos) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(A.pos) like '%bahrain%' then 'Bahrain' else
      case when lower(A.pos) like '%kuwait%' then 'Kuwait' else 'other' end
    end
  end
end as pos,
A.device,A.dim_group_id,A.device_category,A.route,A.booking_date,sum(A.orders) as orders,sum(A.revenue) as revenue,sum(A.views) as views,sum(A.uniq_views) as uniq_views
from
(
select 
    fgatf.dim_channel_id as dim_channel_id,
    S2.pos as pos,
    case when S1.xplatform = 'android' or S1.xplatform = 'ios' then 'app' else 'web' end as device,
    case when S1.dim_group_id=1 then 'tajawal' when S1.dim_group_id=2 then 'almosafer' end as dim_group_id, 
    S1.device_category as device_category, 
    S1.route as route, 
    S1.booking_date as booking_date, 
    COALESCE(sum(S1.orders),0) as orders, 
    COALESCE(sum(S1.revenue),0) as revenue,
    0 as views,
    0 as uniq_views
from (select
    order_no,
    dim_store_id,
xplatform, 
dim_group_id,
device_category,
concat(lower(ltrim(rtrim(dim_origin))),'-',lower(ltrim(rtrim(dim_destination)))) as route,
dim_bookingdate_id as booking_date,
count(distinct(id)) as orders,
sum(ibv) as revenue
from 
tajawal_bi.fact_flight_orders_final 
group by order_no,dim_store_id,xplatform,dim_group_id,
          device_category,concat(lower(ltrim(rtrim(dim_origin))),'-',lower(ltrim(rtrim(dim_destination)))),dim_bookingdate_id) S1
inner join tajawal_bi.dim_store S2 
on S1.dim_store_id=S2.store_id
left outer join 
(
    select transaction_id, max(dim_channel_id) as dim_channel_id 
    from tajawal_bi.fact_google_analytics_transactions_final 
    group by transaction_id
  )
 fgatf
on S1.order_no=fgatf.transaction_id group by fgatf.dim_channel_id ,S2.pos ,case when S1.xplatform = 'android' or S1.xplatform = 'ios' then 'app' else 'web' end ,
    S1.dim_group_id , S1.device_category , 
    S1.route, S1.booking_date 

union all

select 
dim_channel_id,
country as pos, 
device,
group_name, 
devicecategory as device_category,
route, 
gadate as booking_date,
0 as orders,
0 as revenue,
COALESCE(sum(views),0) as views,
COALESCE(sum(uniq_views),0) as uniq_views 
from 
    (select 
        T2.devicecategory as devicecategory,
        T1.mobile_os as mobile_os,
         T1.group_name as group_name,
        T1.device as device,  
        T2.gadate as gadate,
        split(regexp_replace(T2.route,',nearby',''),'~|,')[0] as route,
        T2.country as country,
        T2.views as views,
        T2.uniq_views as uniq_views,
        T2.dim_channel_id as dim_channel_id
from 
tajawal_bi.dim_spend_ga_views T1
inner join
(
select dim_channel_id,case when devicecategory='tablet' or devicecategory='mobile' then 'mobile_web'  else 'web' end as devicecategory,
country,gadate,view_id,lower(ltrim(rtrim(product_detail))) as route ,sum(pageviews) as views,sum(uniqpageviews) as uniq_views
from tajawal_bi.fact_google_analytics_search_web where product like '%flight%' group by dim_channel_id,devicecategory,lower(ltrim(rtrim(product_detail))),gadate,view_id,country
union
select dim_channel_id,'native app' as devicecategory,country,gadate,view_id,lower(ltrim(rtrim(product_detail)))  as route ,sum(total_events) as views,sum(uniq_total_events) as uniq_views
from tajawal_bi.fact_google_analytics_search_app_final where product like '%flight%' group by dim_channel_id,lower(ltrim(rtrim(product_detail))),gadate,view_id,country
) T2 on T1.dim_view_id=T2.view_id
) T3 
group by T3.dim_channel_id,T3.devicecategory,T3.mobile_os,T3.group_name, T3.device, T3.gadate, T3.route, T3.country
) A group by case when A.dim_channel_id='\N' then NULL else A.dim_channel_id end,A.pos,A.device,A.dim_group_id,A.device_category,A.route,A.booking_date;


-- == New table for funnel
drop table if exists ${hiveDB}.report_tajawal_funnel_inter_inter;
create table if not exists ${hiveDB}.report_tajawal_funnel_inter_inter row format delimited fields terminated by '\t'
as
select 
`date` as dim_date, 
b.group_name,
'app' as device, 
case when lower(a.app_name) like '%ios%' then 'ios' else 'android' end as mobile_os,
pos,
sum(case when lower(product) like '%hotel%' then installs else 0 end) as hotel_installs,
sum(case when lower(product) like '%flight%' then installs else 0 end) as flight_installs,
sum(case when lower(product) like '%hotel%' then sessions else 0 end) as hotel_sessions,
sum(case when lower(product) like '%flight%' then sessions else 0 end) as flight_sessions,
sum(hotelsearch_events) as hotel_searches,
sum(flightsearch_events) as flight_searches,
sum(hotelsearch_events) + sum(flightsearch_events) as totalsearch_events,
sum(hotelpage_events) as hotel_details,
sum(flightdetails_events) as flight_details,
sum(hotelpage_events) + sum(flightdetails_events) as total_details,
sum(hotelguestdetails_events) as hotel_guest_details,
sum(flightpax_events) as flight_guest_details,
sum(hotelguestdetails_events) + sum(flightpax_events) as total_guest_details,
sum(hotelpayment_events) as hotel_payment_events,
sum(flightpayment_events) as flight_payment_events,
sum(hotelpayment_events) + sum(flightpayment_events) as total_payment_events,
-- sum(hotel_events) as hotel_bookings,
sum(revenue_events) - sum(flight_events) as hotel_bookings,
sum(flight_events) as flight_bookings,
-- sum(hotel_events) + sum(flight_events) as total_bookings,
sum(revenue_events) as total_bookings,
-- sum(hotel_revenue) as hotel_ibv,
sum(revenue) - sum(flight_revenue) as hotel_ibv,
sum(flight_revenue) as flight_ibv,
-- sum(hotel_revenue) + sum(flight_revenue) as total_ibv
sum(revenue) as total_ibv
from 
tajawal_bi.fact_adjust_data_multi_app_final a inner join tajawal_bi.dim_spend_adjust_apps b 
on a.app_token = b.dim_app_id
group by `date`, b.group_name, case when lower(a.app_name) like '%ios%' then 'ios' else 'android' end, pos

union

-- From GA
-- IBV and Bookings
select dim_bookingdate_id as dim_date,
group_name,
device,
mobile_os,
pos,
0 as hotel_installs,
0 as flight_installs,
sum(hotel_sessions) as hotel_sessions,
sum(flight_sessions) as flight_sessions,
sum(hotel_searches) as hotel_searches,
sum(flight_searches) as flight_searches,
sum(hotel_searches) + sum(flight_searches) as total_searches,
sum(hotel_details) as hotel_details,
sum(flight_details) as flight_details,
sum(hotel_details) + sum(flight_details) as total_details,
sum(hotel_guest_details) as hotel_guest_details,
sum(flight_guest_details) as flight_guest_details,
sum(hotel_guest_details) + sum(flight_guest_details) as total_guest_details,
sum(hotel_payment_events) as hotel_payment_events,
sum(flight_payment_events) as flight_payment_events,
sum(hotel_payment_events) + sum(flight_payment_events) as total_payment_events,
sum(hotel_bookings) as hotel_bookings,
sum(flight_bookings) as flight_bookings,
sum(hotel_bookings) + sum(flight_bookings) as total_bookings,
sum(hotel_ibv) as hotel_ibv,
sum(flight_ibv) as flight_ibv,
sum(hotel_ibv) + sum(flight_ibv) as total_ibv
from
(
select
fh.dim_bookingdate_id,
dg.group_name,
'web' as device,
case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'mobile_web' else 'web' end as mobile_os,
ds.pos,
0 as hotel_sessions,
0 as flight_sessions,
0 as hotel_searches,
0 as flight_searches,
0 as hotel_details,
0 as flight_details,
0 as hotel_guest_details,
0 as flight_guest_details,
0 as hotel_payment_events,
0 as flight_payment_events,
count(distinct(case when order_type like '%hotel%' then id else null end)) as hotel_bookings,
count(distinct(case when order_type like '%flight%' then id else null end)) as flight_bookings,
sum(case when order_type like '%hotel%' then payment_amount_usd else null end) as hotel_ibv,
sum(case when order_type like '%flight%' then payment_amount_usd else null end) as flight_ibv
from
(
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, dim_language_id as language, device_category
   from tajawal_bi.fact_hotel_orders_final
   where dim_status_id != 91
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
   union
   select id, order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, dim_language as language, device_category
   from tajawal_bi.fact_flight_orders_final
   where dim_status_id != 91
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
) fh
inner join tajawal_bi.dim_groups dg on fh.dim_group_id = dg.group_id
inner join tajawal_bi.dim_store ds on fh.dim_store_id = ds.store_id
inner join 
(
  select transaction_id, max(view_id) as view_id 
  from tajawal_bi.fact_google_analytics_transactions_final 
  where view_id in ('109503410', '130709589')
  group by transaction_id
) gt on fh.order_no = gt.transaction_id
group by 
fh.dim_bookingdate_id,
dg.group_name,
case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'mobile_web' else 'web' end,
ds.pos

union all

-- payment_visits, detail_visits, traveller_visits 
select
gsap.gadate,
gav.group_name,
'web' as device,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'na'
  end
end as mobile_os,
case when lower(country) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(country) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(country) like '%baharain%' then 'Baharain' else
      case when lower(country) like '%kuwait%' then 'Kuwait' else 'other' end
    end
  end
end as pos,
0 as hotel_sessions,
0 as flight_sessions,
0 as hotel_searches,
0 as flight_searches
, sum(case when lower(pagetype) like '%detail%' and lower(product) like '%hotel%' then uniq_pageviews else 0 end) as hotel_details
, sum(case when lower(pagetype) like '%detail%' and lower(product) like '%flight%' then uniq_pageviews else 0 end) as flight_details
, sum(case when lower(pagetype) like '%detail%' and lower(product) like '%hotel%' then uniq_pageviews else 0 end) as hotel_guest_details
, sum(case when lower(pagetype) like '%detail%' and lower(product) like '%flight%' then uniq_pageviews else 0 end) as flight_guest_details
, sum(case when lower(pagetype) like '%payment%' and lower(product) like '%hotel%' then uniq_pageviews else 0 end) as hotel_payment_events
, sum(case when lower(pagetype) like '%payment%' and lower(product) like '%flight%' then uniq_pageviews else 0 end) as flight_payment_events
, 0 as hotel_bookings
, 0 as flight_bookings
, 0 as hotel_ibv
, 0 as flight_ibv
from
tajawal_bi.fact_google_analytics_web_pageviews gsap
left outer join tajawal_bi.dim_spend_ga_views gav on gsap.view_id = gav.dim_view_id
group by
gsap.gadate,
gav.group_name,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'na'
  end
end,
case when lower(country) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(country) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(country) like '%baharain%' then 'Baharain' else
      case when lower(country) like '%kuwait%' then 'Kuwait' else 'other' end
    end
  end
end

union all

-- GA unique searches
select
gsap.gadate as dim_date_id,
gav.group_name,
'web' as device,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'other'
  end
end as mobile_os,
case when lower(country) like '%saudi%' or lower(country) like '%sa%' then 'Saudi Arabia' else
  case when lower(country) like '%arab%em%' or lower(country) like '%uae%' then 'United Arab Emirates' else
    case when lower(country) like '%kuwait%' then 'Kuwait' else
      case when lower(country) like '%bahrain%' then 'Bahrain' else 'NA'
      end
    end
  end
end as pos,
0 as hotel_sessions,
0 as flight_sessions,
sum(case when lower(gsap.product) like '%hotel%' then uniqpageviews else 0 end) as hotel_searches,
sum(case when lower(gsap.product) like '%flight%' then uniqpageviews else 0 end) as flight_searches,
0 as hotel_details,
0 as flight_details,
0 as hotel_guest_details,
0 as flight_guest_details,
0 as hotel_payment_events,
0 as flight_payment_events,
0 as hotel_bookings,
0 as flight_bookings,
0 as hotel_ibv,
0 as flight_ibv
from
tajawal_bi.fact_google_analytics_search_web gsap
left outer join tajawal_bi.dim_spend_ga_views gav on gsap.view_id = gav.dim_view_id
group by
gsap.gadate,
gav.group_name,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
    else 'other'
  end
end,
case when lower(country) like '%saudi%' or lower(country) like '%sa%' then 'Saudi Arabia' else
  case when lower(country) like '%arab%em%' or lower(country) like '%uae%' then 'United Arab Emirates' else
    case when lower(country) like '%kuwait%' then 'Kuwait' else
      case when lower(country) like '%bahrain%' then 'Bahrain' else 'NA'
      end
    end
  end
end

) A
group by
dim_bookingdate_id,
group_name,
device,
mobile_os,
pos;

create table if not exists ${hiveDB}.report_tajawal_funnel_inter_inter2
-- row format delimited fields terminated by '\t' 
as
select 
dim_date, group_name, device, mobile_os, pos, product,
sum(installs) as installs,
sum(sessions) as sessions,
sum(searches) as searches,
sum(details) as details,
sum(guest_details) as guest_details,
sum(payment_events) as payment_events,
sum(bookings) as bookings,
sum(ibv) as ibv
from 
(
select 
dim_date, group_name, device, mobile_os, pos, 'hotel' as product,
0 as installs,
0 as sessions,
sum(hotel_searches) as searches,
sum(hotel_details) as details,
sum(hotel_guest_details) as guest_details,
sum(hotel_payment_events) as payment_events,
sum(hotel_bookings) as bookings,
sum(hotel_ibv) as ibv
from
${hiveDB}.report_tajawal_funnel_inter_inter
group by
dim_date, group_name, device, mobile_os, pos

union all

select 
dim_date, group_name, device, mobile_os, pos, 'flight' as product,
0 as installs,
0 as sessions,
sum(flight_searches) as searches,
sum(flight_details) as details,
sum(flight_guest_details) as guest_details,
sum(flight_payment_events) as payment_events,
sum(flight_bookings) as bookings,
sum(flight_ibv) as ibv
from
${hiveDB}.report_tajawal_funnel_inter_inter
group by
dim_date, group_name, device, mobile_os, pos

union all

select 
`date` as dim_date, b.group_name, 'app' as device, 
case when lower(a.app_name) like '%ios%' then 'ios' else 'android' end as mobile_os,
pos,
product,
sum(installs) as installs,
sum(sessions) as sessions,
0 as searches,
0 as details,
0 as guest_details,
0 as payment_events,
0 as bookings,
0 as ibv
from 
tajawal_bi.fact_adjust_data_multi_app_final a inner join tajawal_bi.dim_spend_adjust_apps b 
on a.app_token = b.dim_app_id
group by `date`, b.group_name, case when lower(a.app_name) like '%ios%' then 'ios' else 'android' end, pos, product 

union all

select
gad.gadate as dim_date, gav.group_name, 'web' as device,
case when lower(gad.device_category) like '%desktop%' then 'web' else
  case when lower(gad.device_category) like '%tablet%' or lower(gad.device_category) like '%mobile%' then 'mobile_web'
    else 'other'
  end
end as mobile_os,
pos,
product,
0 as installs,
sum(sessions) as sessions,
0 as searches,
0 as details,
0 as guest_details,
0 as payment_events,
0 as bookings,
0 as ibv
from
tajawal_bi.fact_google_analytics_ad gad
left outer join tajawal_bi.dim_spend_ga_views gav on gad.view_id = gav.dim_view_id
where view_id in ('109503410', '130709589')
group by
gad.gadate,
gav.group_name,
case when lower(gad.device_category) like '%desktop%' then 'web' else
  case when lower(gad.device_category) like '%tablet%' or lower(gad.device_category) like '%mobile%' then 'mobile_web'
    else 'other'
  end
end,
pos,
product
) B
group by dim_date, group_name, device, mobile_os, pos, product;

drop table if exists ${hiveDB}.report_tajawal_funnel;
alter table ${hiveDB}.report_tajawal_funnel_inter_inter2 rename to ${hiveDB}.report_tajawal_funnel;

drop table tajawal_bi.city_mapping_searches_hotel;

create table tajawal_bi.city_mapping_searches_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
as
select A.city as searches_city, B.city as hotel_city
from
(
select lower(rtrim(ltrim(product_detail))) as city
from
tajawal_bi.fact_google_analytics_search_app
where product like '%hotel%' and lower(rtrim(ltrim(product_detail)))!=''
group by lower(rtrim(ltrim(product_detail)))
union
select lower(rtrim(ltrim(product_detail))) as city
from
tajawal_bi.fact_google_analytics_search_web
where product like '%hotel%' and lower(rtrim(ltrim(product_detail)))!=''
group by lower(rtrim(ltrim(product_detail))) 
) A 
join
(
select lower(rtrim(ltrim(h.city))) as city,count(distinct(id)) as orders
from
tajawal_bi.dim_hotel h
inner join
tajawal_bi.fact_hotel_orders_final o
on h.dim_hotel_id=o.dim_hotel_id
where lower(rtrim(ltrim(h.city)))!=''
group by lower(rtrim(ltrim(h.city))) order by orders desc limit 1000
) B
where A.city rlike concat('[^a-z]+',B.city,'[^a-z]+');


drop table if exists ${hiveDB}.fact_per_city_orders_searches;

create table ${hiveDB}.fact_per_city_orders_searches ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
as
select u.dim_channel_id,
    case when lower(u.pos) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(u.pos) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(u.pos) like '%bahrain%' then 'Bahrain' else
      case when lower(u.pos) like '%kuwait%' then 'Kuwait' else 'other' end
    end
  end
end as pos,
u.group_name,
u.devicecategory,
u.city,
u.booking_date,
sum(u.orders) as orders,
sum(u.revenue) as revenue,
sum(u.views) as views,
sum(u.uniq_views) as uniq_views
from
(
    select 
        T5.dim_channel_id as dim_channel_id,
        T5.pos as pos,
        T5.group_name as group_name,
        T5.devicecategory as devicecategory,
        case when COALESCE(T6.cleaned_city,'')='' then T5.city else T6.cleaned_city end as city,
        T5.booking_date as booking_date,
        0 as orders,
        0 as revenue,
        T5.views as views,
        T5.uniq_views as uniq_views
from
    (
  select 
        T3.dim_channel_id as dim_channel_id,
        T3.pos as pos,
        T3.group_name as group_name,
        T3.devicecategory as devicecategory,
        case 
          when COALESCE(T4.cleaned_city,'')='' and COALESCE(latest.hotel_city,'')='' then T3.city 
          when COALESCE(T4.cleaned_city,'')!='' then T4.cleaned_city
          when COALESCE(latest.hotel_city,'')!='' then latest.hotel_city
          else T3.city end as city,
        T3.booking_date as booking_date,
        T3.views as views,
        T3.uniq_views as uniq_views
from  
(
select 
        T2.dim_channel_id as dim_channel_id,
        T2.country as pos,
        T1.group_name as group_name,
        T2.devicecategory as devicecategory,
        T2.city as city,
        T2.gadate as booking_date,
        sum(T2.views) as views,
        sum(T2.uniq_views) as uniq_views
from 
tajawal_bi.dim_spend_ga_views T1
inner join
(
    select dim_channel_id,case when devicecategory='tablet' or devicecategory='mobile' then 'mobile web'  else 'web' end as devicecategory,
    country,gadate,view_id,lower(ltrim(rtrim(product_detail))) as city ,sum(pageviews) as views,sum(uniqpageviews) as uniq_views
    from tajawal_bi.fact_google_analytics_search_web 
    where product like '%hotel%' 
    group by dim_channel_id,devicecategory,lower(ltrim(rtrim(product_detail))),gadate,view_id,country
    union
    select dim_channel_id,'native app' as devicecategory,country,gadate,view_id,lower(ltrim(rtrim(product_detail)))  as city ,sum(total_events) as views,sum(uniq_total_events) as uniq_views
    from tajawal_bi.fact_google_analytics_search_app_final 
    where product like '%hotel%' 
    group by dim_channel_id,lower(ltrim(rtrim(product_detail))),gadate,view_id,country
) T2 on T1.dim_view_id=T2.view_id 
group by T2.devicecategory,T1.mobile_os,T1.group_name,T1.device,T2.gadate,T2.city,T2.country,T2.dim_channel_id
) T3 
left outer join
( select  lower(ltrim(rtrim(search_city))) as search_city,lower(ltrim(rtrim(max(cleaned_city)))) as cleaned_city
    from googlesheets.searches_city_cleanup group by lower(ltrim(rtrim(search_city)))
) T4 
on T4.search_city=T3.city
left outer join
(
  select lower(ltrim(rtrim(searches_city))) as searches_city,lower(ltrim(rtrim(max(hotel_city)))) as hotel_city
  from tajawal_bi.city_mapping_searches_hotel
  group by lower(ltrim(rtrim(searches_city)))
) latest
on latest.searches_city=T3.city
) T5
    left outer join
    (
        select  lower(ltrim(rtrim(city_name))) as city_name,lower(ltrim(rtrim(max(cleaned_city_name)))) as cleaned_city
    from googlesheets.dim_hotel_city_cleanup group by lower(ltrim(rtrim(city_name)))
        ) T6 on T6.city_name=T5.city
union all
--orders
select 
    S1.dim_channel_id as dim_channel_id,
    S2.pos as pos,
    case when S1.dim_group_id=1 then 'tajawal' when S1.dim_group_id=2 then 'almosafer' end as group_name, 
    S1.device_category as devicecategory, 
    lower(ltrim(rtrim(h.city))) as city, 
    S1.booking_date as booking_date, 
    COALESCE(count(distinct(S1.id)),0) as orders, 
    COALESCE(sum(S1.ibv),0) as revenue,
    0 as views,
    0 as uniq_views
from (
    select
    dim_channel_id,
    order_no,
    dim_store_id,
    dim_group_id,
    device_category,
    dim_hotel_id as dim_hotel_id,
    dim_bookingdate_id as booking_date,
    id,
    ibv
    from 
    tajawal_bi.fact_hotel_orders_final 
    where dim_group_id=1 or dim_group_id=2
    -- group by 
    -- order_no,
    -- dim_store_id,dim_group_id,
    -- device_category,dim_hotel_id,dim_bookingdate_id
  ) S1
left outer join tajawal_bi.dim_hotel h on h.dim_hotel_id=S1.dim_hotel_id
left outer join tajawal_bi.dim_store S2 
on S1.dim_store_id=S2.store_id
group by S1.dim_channel_id ,S2.pos ,
    S1.dim_group_id , S1.device_category , 
    h.city, S1.booking_date    ) u 
group by u.dim_channel_id,
case when lower(u.pos) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(u.pos) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(u.pos) like '%bahrain%' then 'Bahrain' else
      case when lower(u.pos) like '%kuwait%' then 'Kuwait' else 'other' end
    end
  end
end,u.group_name,u.devicecategory,u.city,u.booking_date;


