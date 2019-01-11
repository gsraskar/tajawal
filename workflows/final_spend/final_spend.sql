set hive.mv.files.thread=0;

-- ===============
-- ===== Create new CRM Table
-- ===============

--===CRM Table data

-- Query 1, Part 1 : Include GA Transactions data into it
Drop table if exists ${hiveDB}.inter_fact_crm_spend_consolidation;
Create table if not exists 
${hiveDB}.inter_fact_crm_spend_consolidation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
Select
'notarget' as recordtype,
gv.group_name,
fh.dim_bookingdate_id as `booking_date`,
gv.device,
gv.mobile_os,
case when lower(coalesce(ds.pos, 'na')) = 'null' then 'na' else lower(ds.pos) end as pos,
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
case when lower(coalesce(ds.pos, 'na')) = 'null' then 'na' else lower(ds.pos) end,
fh.order_type,
ltrim(rtrim(fga.channel_group)),
ltrim(rtrim(fga.channel));

-- Query 1, Part 2.1 : Include Flight Adjust data into it
insert into 
${hiveDB}.inter_fact_crm_spend_consolidation
Select
'notarget' as recordtype,
B.group_name,
A.`date` as `booking_date`,
'app' as device,
case when lower(B.app_name) like 'ios' then 'ios' else 
  case when lower(B.app_name) like 'android' then 'android' else 'na'
  end
end as mobile_os,
lower(A.pos) as pos,
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
${hiveDB}.inter_fact_crm_spend_consolidation
Select
'notarget' as recordtype,
B.group_name,
A.`date` as `booking_date`,
'app' as device,
case when lower(B.app_name) like 'ios' then 'ios' else 
  case when lower(B.app_name) like 'android' then 'android' else 'na'
  end
end as mobile_os,
lower(A.pos) as pos,
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

Drop table if exists ${hiveDB}.inter2_fact_crm_spend_consolidation;
Create table ${hiveDB}.inter2_fact_crm_spend_consolidation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
select * from ${hiveDB}.inter_fact_crm_spend_consolidation 
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
From ${hiveDB}.inter_fact_crm_spend_consolidation 
Group by 
group_name,
booking_date,
product,
channel_group;
Drop table ${hiveDB}.inter_fact_crm_spend_consolidation;
Alter table ${hiveDB}.inter2_fact_crm_spend_consolidation rename to ${hiveDB}.inter_fact_crm_spend_consolidation;

--=======Query 3 : Combine Spend into final table

Drop table if exists ${hiveDB}.inter2_fact_crm_spend_consolidation;
Create table ${hiveDB}.inter2_fact_crm_spend_consolidation  -- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
as 
Select
recordtype,
group_name,
booking_date,
device,
case when coalesce(mobile_os, 'na') = 'na' then 'NA' else coalesce(mobile_os, 'NA') end as mobile_os,
case when lower(case when lower(pos) like '%united%' then 'uae' else pos end) = 'na' then 'others' else lower(case when lower(pos) like '%united%' then 'uae' else pos end) end as pos,
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
${hiveDB}.inter_fact_crm_spend_consolidation a
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
Drop table if exists ${hiveDB}.fact_crm_spend_consolidation;
Alter table ${hiveDB}.inter2_fact_crm_spend_consolidation rename to ${hiveDB}.fact_crm_spend_consolidation;



truncate table ${hiveDB}.dim_spend_daily_targets;
insert into ${hiveDB}.dim_spend_daily_targets select * from googlesheets.dim_spend_daily_targets;

 truncate table ${hiveDB}.dim_spend_monthly_targets_final; insert into ${hiveDB}.dim_spend_monthly_targets_final select * from googlesheets.dim_spend_monthly_targets_final;

drop table ${hiveDB}.fact_per_route_orders_searches;
create table ${hiveDB}.fact_per_route_orders_searches 
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
as
select case when A.dim_channel_id='\N' then NULL else A.dim_channel_id end as dim_channel_id,
case when lower(A.pos) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(A.pos) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(A.pos) like '%bahrain%' then 'Bahrain' else
      case when lower(A.pos) like '%kuwait%' then 'Kuwait' else 'others' end
    end
  end
end as pos,
A.device,A.dim_group_id,A.device_category,A.route,A.booking_date,sum(A.orders) as orders,
round(sum(A.revenue), 2) as revenue,
sum(A.views) as views,sum(A.uniq_views) as uniq_views
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
drop table if exists ${hiveDB}.report_tajawal_funnel_inter;
create table if not exists ${hiveDB}.report_tajawal_funnel_inter row format delimited fields terminated by '\t'
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
-- sum(hotelsearch_events) as hotel_searches,
0 as hotel_searches,
-- sum(flightsearch_events) as flight_searches,
0 as flight_searches,
-- sum(hotelsearch_events) + sum(flightsearch_events) as totalsearch_events,
0 as totalsearch_events,
-- sum(hotelpage_events) as hotel_details,
0 as hotel_details,
-- sum(flightdetails_events) as flight_details,
0 as flight_details,
-- sum(hotelpage_events) + sum(flightdetails_events) as total_details,
0 as total_details,
-- sum(hotelguestdetails_events) as hotel_guest_details,
0 as hotel_guest_details,
-- sum(flightpax_events) as flight_guest_details,
0 as flight_guest_details,
-- sum(hotelguestdetails_events) + sum(flightpax_events) as total_guest_details,
0 as total_guest_details,
-- sum(hotelpayment_events) as hotel_payment_events,
0 as hotel_payment_events,
-- sum(flightpayment_events) as flight_payment_events,
0 as flight_payment_events,
-- sum(hotelpayment_events) + sum(flightpayment_events) as total_payment_events,
0 as total_payment_events,
-- sum(hotel_events) as hotel_bookings,
0 as hotel_bookings,
-- sum(revenue_events) - sum(flight_events) as hotel_bookings,
-- sum(flight_events) as flight_bookings,
0 as flight_bookings,
-- sum(hotel_events) + sum(flight_events) as total_bookings,
-- sum(revenue_events) as total_bookings,
0 as total_bookings,
-- sum(hotel_revenue) as hotel_ibv,
-- sum(revenue) - sum(flight_revenue) as hotel_ibv,
0 as hotel_ibv,
-- sum(flight_revenue) as flight_ibv,
0 as flight_ibv,
-- sum(hotel_revenue) + sum(flight_revenue) as total_ibv
-- sum(revenue) as total_ibv
0 as total_ibv
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
gv.device as device,
-- case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'app' else lower(fh.device_category) end as mobile_os,
coalesce(gv.mobile_os, 'web') as mobile_os,
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
count(distinct(case when order_type like '%hotel%' then order_no else null end)) as hotel_bookings,
count(distinct(case when order_type like '%flight%' then order_no else null end)) as flight_bookings,
sum(case when order_type like '%hotel%' then payment_amount_usd else null end) as hotel_ibv,
sum(case when order_type like '%flight%' then payment_amount_usd else null end) as flight_ibv
from
(
   select order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, dim_language_id as language, device_category
   from tajawal_bi.fact_hotel_orders_final
   where dim_status_id != 91
   -- group by order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type
   union
   select order_no, dim_group_id, dim_store_id, dim_bookingdate_id,order_type, ibv as payment_amount_usd, dim_language as language, device_category
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
  -- where view_id in ('109503410', '130709589')
  group by transaction_id
) gt on fh.order_no = gt.transaction_id
inner join tajawal_bi.dim_spend_ga_views gv on gt.view_id = gv.dim_view_id
group by 
fh.dim_bookingdate_id,
dg.group_name,
gv.device,
-- case when lower(fh.device_category) like '%mobile%web%' or lower(fh.device_category) like '%native%app%' then 'app' else  lower(fh.device_category) end,
coalesce(gv.mobile_os, 'web'),
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
-- coalesce(gav.mobile_os, 'web') as mobile_os,
case when lower(country) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(country) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(country) like '%baharain%' then 'Baharain' else
      case when lower(country) like '%kuwait%' then 'Kuwait' else 'others' end
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
-- coalesce(gav.mobile_os, 'web'),
case when lower(country) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(country) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(country) like '%baharain%' then 'Baharain' else
      case when lower(country) like '%kuwait%' then 'Kuwait' else 'others' end
    end
  end
end

union all

-- payment_visits, detail_visits, traveller_visits 
select
gsap.gadate,
gav.group_name,
'app' as device,
-- case when lower(gsap.devicecategory) like '%desktop%' then 'mobile_web' else
--  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile'
--    else 'others'
--  end
-- end as mobile_os,
coalesce(gav.mobile_os, 'web') as mobile_os,
case when lower(country) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(country) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(country) like '%baharain%' then 'Baharain' else
      case when lower(country) like '%kuwait%' then 'Kuwait' else 'others' end
    end
  end
end as pos,
0 as hotel_sessions,
0 as flight_sessions,
0 as hotel_searches,
0 as flight_searches
, sum(case when lower(pagetype) like 'details%' and lower(product) like '%hotel%' then uniq_screen_views else 0 end) as hotel_details
, sum(case when lower(pagetype) like 'details%' and lower(product) like '%flight%' then uniq_screen_views else 0 end) as flight_details
, sum(case when lower(pagetype) like 'guest%details' and lower(product) like '%hotel%' then uniq_screen_views else 0 end) as hotel_guest_details
, sum(case when lower(pagetype) like 'guest%details' and lower(product) like '%flight%' then uniq_screen_views else 0 end) as flight_guest_details
, sum(case when lower(pagetype) like '%payment%' and lower(product) like '%hotel%' then uniq_screen_views else 0 end) as hotel_payment_events
, sum(case when lower(pagetype) like '%payment%' and lower(product) like '%flight%' then uniq_screen_views else 0 end) as flight_payment_events
, 0 as hotel_bookings
, 0 as flight_bookings
, 0 as hotel_ibv
, 0 as flight_ibv
from
tajawal_bi.fact_google_analytics_app_pageviews gsap
left outer join tajawal_bi.dim_spend_ga_views gav on gsap.view_id = gav.dim_view_id
group by
gsap.gadate,
gav.group_name,
--case when lower(gsap.devicecategory) like '%desktop%' then 'mobile_web' else
--  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile'
--    else 'others'
--  end
--end,
coalesce(gav.mobile_os, 'web'),
case when lower(country) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(country) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(country) like '%baharain%' then 'Baharain' else
      case when lower(country) like '%kuwait%' then 'Kuwait' else 'others' end
    end
  end
end


union all

-- GA Web unique searches
select
gsap.gadate as dim_date_id,
gav.group_name,
'web' as device,
case when lower(gsap.devicecategory) like '%desktop%' then 'web' else
 case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile_web'
   else 'others'
 end
end as mobile_os,
-- coalesce(gav.mobile_os, 'web'),
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
    else 'others'
  end
end,
-- coalesce(gav.mobile_os, 'web'),
case when lower(country) like '%saudi%' or lower(country) like '%sa%' then 'Saudi Arabia' else
  case when lower(country) like '%arab%em%' or lower(country) like '%uae%' then 'United Arab Emirates' else
    case when lower(country) like '%kuwait%' then 'Kuwait' else
      case when lower(country) like '%bahrain%' then 'Bahrain' else 'NA'
      end
    end
  end
end

union all

-- GA App unique searches
select
gsap.gadate as dim_date_id,
gav.group_name,
'app' as device,
-- case when lower(gsap.devicecategory) like '%desktop%' then 'mobile_web' else
--  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile'
--    else 'others'
--  end
-- end as mobile_os,
coalesce(gav.mobile_os, 'web') as mobile_os,
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
sum(case when lower(gsap.product) like '%hotel%' then uniq_total_events else 0 end) as hotel_searches,
sum(case when lower(gsap.product) like '%flight%' then uniq_total_events else 0 end) as flight_searches,
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
tajawal_bi.fact_google_analytics_search_app_final gsap
left outer join tajawal_bi.dim_spend_ga_views gav on gsap.view_id = gav.dim_view_id
group by
gsap.gadate,
gav.group_name,
-- case when lower(gsap.devicecategory) like '%desktop%' then 'mobile_web' else
--  case when lower(gsap.devicecategory) like '%tablet%' or lower(gsap.devicecategory) like '%mobile%' then 'mobile'
--    else 'others'
--  end
-- end,
coalesce(gav.mobile_os, 'web'),
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



create table if not exists ${hiveDB}.report_tajawal_funnel_inter2
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
tajawal_bi.report_tajawal_funnel_inter
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
tajawal_bi.report_tajawal_funnel_inter
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
    else 'others'
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
    else 'others'
  end
end,
pos,
product
) B
group by dim_date, group_name, device, mobile_os, pos, product;

drop table if exists ${hiveDB}.report_tajawal_funnel;
alter table ${hiveDB}.report_tajawal_funnel_inter2 rename to ${hiveDB}.report_tajawal_funnel;


drop table if exists ${hiveDB}.fact_per_city_orders_searches;

create table ${hiveDB}.fact_per_city_orders_searches --ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
as
select u.dim_channel_id,
    case when lower(u.pos) like '%united%arab%emirates' then 'United Arab Emirates' else
  case when lower(u.pos) like '%saudi%arabia%' then 'Saudi Arabia' else
    case when lower(u.pos) like '%bahrain%' then 'Bahrain' else
      case when lower(u.pos) like '%kuwait%' then 'Kuwait' else 'others' end
    end
  end
end as pos,
u.group_name,
u.devicecategory,
u.city,
u.booking_date,
sum(u.orders) as orders,
round(sum(u.revenue), 2) as revenue,
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
        case when COALESCE(T4.cleaned_city,'')='' then T3.city else T4.cleaned_city end as city,
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
    select dim_channel_id,case when devicecategory='tablet' or devicecategory='mobile' then 'mobile_web'  else 'web' end as devicecategory,
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
      case when lower(u.pos) like '%kuwait%' then 'Kuwait' else 'others' end
    end
  end
end,u.group_name,u.devicecategory,u.city,u.booking_date;

