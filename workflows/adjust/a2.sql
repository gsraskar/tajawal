drop table if exists tajawal_uat_bi.fact_adjust_data_multi_app_final_brand;
create table tajawal_uat_bi.fact_adjust_data_multi_app_final_brand like tajawal_bi.fact_adjust_data_multi_app_final;
insert into tajawal_uat_bi.fact_adjust_data_multi_app_final_brand 
select
tracker_token,network,campaign,adgroup,creative,`date`,app_token,app_name,country,clicks,impressions,installs,click_conversion_rate,ctr,impression_conversion_rate,reattributions,sessions,revenue_events,revenue,daus,waus,maus,hotel_revenue,hotel_events,hotel_revenue_per_event,flight_revenue,flight_events,flight_revenue_per_event,hotelpage_events,flightdetails_events,hotelguestdetails_events,flightpax_events,hotelpayment_events,flightpayment_events,hotelsearch_events,flightsearch_events,holidays_revenue,holidays_revenue_events,holidays_package_details_events,holidays_guest_details_events,holidays_payment_events,product,pos,mobile_os,language,cost_metric_name,cost_metric_value,cost,hotel_spend,flight_spend,package_spend,a.dim_channel_id,
case when lower(ltrim(rtrim(channel_group))) like '%sem%' and lower(ltrim(rtrim(campaign))) like '%-brand-%' then 'brand'
       when lower(ltrim(rtrim(channel_group))) like '%crm%' or lower(ltrim(rtrim(channel_group))) like  '%display%' then 'brand'
       else 'Non Brand'
  end as brand_nonbrand,
objective
from tajawal_bi.fact_adjust_data_multi_app_final a left outer join tajawal_bi.dim_spend_channels ds on a.dim_channel_id = ds.dim_channel_id;
