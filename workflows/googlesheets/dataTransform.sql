Drop table if exists googlesheets.tmp_dim_google_cpc_sourcemedium_mapping;
create table googlesheets.tmp_dim_google_cpc_sourcemedium_mapping  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
select * from googlesheets.dim_google_cpc_sourcemedium_mapping where  lower(campaign_name) not like "%campaignname%" ;
Drop table googlesheets.dim_google_cpc_sourcemedium_mapping;
alter table googlesheets.tmp_dim_google_cpc_sourcemedium_mapping rename to googlesheets.dim_google_cpc_sourcemedium_mapping;


--Daily Spend data 
truncate table  googlesheets.`dim_spend_monthly_targets_final` ;
 Insert into table googlesheets.`dim_spend_monthly_targets_final`
 select  
  ltrim(rtrim(`brand_id`)) as brand_id ,
   ltrim(rtrim(brand)) as brand ,                                                                                                         
   ltrim(rtrim(`channel`)) as channel,                                                                                                       
   ltrim(rtrim(`sub_channel`)) as sub_channel,                                                                                                       
   ltrim(rtrim(`month`)) as `month`,                                                                                                         
   ltrim(rtrim(`year`)) as `year`,                                                                                                          
   case when lower(product) like '%hotel%' then 'hotel' else case when lower(product) like '%flight%' then 'flight' else lower(product) end end as product,                                                                                                       
   coalesce(cast(regexp_replace(`target_spend`, ',', '')  as double), 0.0),                                                                                                
   coalesce(cast(regexp_replace(`bookings_target`, ',', '') as double), 0.0),                                                                                               
   coalesce(cast(regexp_replace(`ibv`, ',', '') as double), 0.0),                                                                                                           
   coalesce(cast(regexp_replace(`gbv_target`, ',', '') as double), 0.0),                                                                                                    
   coalesce(cast(regexp_replace(`cpo_target`, ',', '') as double), 0.0),                                                                                                    
   coalesce(cast(regexp_replace(`crr_target`, ',', '') as double), 0.0),                                                                                                    
   coalesce(cast(regexp_replace(`revenue_share_target`, ',', '') as double), 0.0)
   from googlesheets.`dim_spend_monthly_targets` where ltrim(rtrim(lower(brand)))!="brand";


truncate table googlesheets.dim_spend_daily_targets;
insert into table googlesheets.dim_spend_daily_targets  
select 
a.brand_id  as brand_id ,         
a.brand  as brand,               
a.channel as channel,            
a.sub_channel as sub_channel,            
a.month  as month,             
a.year   as year,             
a.product as product,    
c.day_of_month,       
round((a.target_spend/c.day_of_month),2)   as target_spend,      
round((a.bookings_target/c.day_of_month),2)   as bookings_target,     
round((a.ibv/c.day_of_month),2)   as IBV ,                 
round((a.gbv_target /c.day_of_month),2)   as gbv_target,          
a.cpo_target as cpo_target,         
a.crr_target as crr_target,         
a.revenue_share_target as revenue_share_target,
b.dim_date_id as `dim_date`         
from googlesheets.dim_spend_monthly_targets_final a inner join 
(select * from tajawal_bi.dim_date) b 
on lower(ltrim(rtrim(a.month)))=lower(ltrim(rtrim(b.month_name))) and ltrim(rtrim(lower(a.year)))=ltrim(rtrim(lower(b.year_number)))
inner join (select year_number, month_name, max(day_of_month) as day_of_month from tajawal_bi.dim_date group by year_number, month_name order by year_number, month_name)c on ltrim(rtrim(lower(a.month)))=ltrim(rtrim(lower(c.month_name))) and ltrim(rtrim(lower(a.year)))=ltrim(rtrim(lower(c.year_number)));





Truncate table googlesheets.`spend_affiliate_push_final`;
 Insert into table googlesheets.spend_affiliate_push_final 
 select  
 `date`, 
 regexp_replace(regexp_replace(`spend`,'$',''),'#REF','0.0') as spend , 
 regexp_replace(regexp_replace(`bookings`,'$',''),'#REF','0.0') as  bookings, 
 regexp_replace(regexp_replace(`revenue`,'$',''),'#REF','0.0') as revenue, 
 regexp_replace(regexp_replace(`cpa`,'$',''),'#DIV/0!','0.0') as cpa, 
 regexp_replace(regexp_replace(`aov`,'$',''),'#DIV/0!','0.0') as aov, 
 regexp_replace(regexp_replace(`crr`,'%',''),'#DIV/0!','0.0') as crr, 
 `brand`, 
 `type` 
 from googlesheets.spend_affiliate_push
  where `date`!= '' and `date`!= 'Date';
 

Truncate table googlesheets.almosafer_offline_data_final;
   insert into googlesheets.almosafer_offline_data_final  select
 `date` ,
 regexp_replace(regexp_replace(salesibv_daily_total_sar,',',''),'#REF','0.0') as salesibv_daily_total_sar,
 regexp_replace(regexp_replace(salesibv_call_centre,',',''),'#REF','0.0') as salesibv_call_centre, 
 regexp_replace(regexp_replace(salesibv_follow_up,',',''),'#REF','0.0') as salesibv_follow_up,
 regexp_replace(regexp_replace(salesibv_vip,',',''),'#REF','0.0') as salesibv_vip,
 regexp_replace(regexp_replace(salesibv_retail,',',''),'#REF','0.0') as salesibv_retail ,	
 regexp_replace(regexp_replace(salesibv_monthly_rolling_total,',',''),'#REF','0.0') as salesibv_monthly_rolling_total,
 regexp_replace(regexp_replace(number_of_transactions_daily_total,',',''),'#REF','0.0') as number_of_transactions_daily_total,
 regexp_replace(regexp_replace(number_of_transactions_call_centre ,',',''),'#REF','0.0') as number_of_transactions_call_centre,
 regexp_replace(regexp_replace(number_of_transactions_follow_up,',',''),'#REF','0.0') as number_of_transactions_follow_up,
 regexp_replace(regexp_replace(number_of_transactions_vip,',',''),'#REF','0.0') as number_of_transactions_vip,
 regexp_replace(regexp_replace(number_of_transactions_retail,',',''),'#REF','0.0') as number_of_transactions_retail,
 regexp_replace(regexp_replace(number_of_transactions_monthly_rolling_total,',',''),'#REF','0.0') as number_of_transactions_monthly_rolling_total
 from   googlesheets.almosafer_offline_data;


Truncate table googlesheets.summary_sheet_tajawal_flights_final;
 Insert into table googlesheets.summary_sheet_tajawal_flights_final   
 select  
 `date` ,
  regexp_replace(regexp_replace(sm_spend,'$',''),'#REF','0.0') as sm_spend ,
  regexp_replace(regexp_replace(sm_bookings,'$',''),'#REF','0.0') as sm_bookings  ,
  regexp_replace(regexp_replace(sm_revenue,'$',''),'#REF','0.0') as sm_revenue   ,
  regexp_replace(regexp_replace(sm_spend,'$',''),'#REF','0.0')  as sm_app_spend   ,
  regexp_replace(regexp_replace(sm_spend,'$',''),'#REF','0.0') as sm_app_bookings  ,
  regexp_replace(regexp_replace(sm_app_revenue,'$',''),'#REF','0.0')as sm_app_revenue ,
  regexp_replace(regexp_replace(sm_spend,'$',''),'#REF','0.0') as sem_spend   ,
  regexp_replace(regexp_replace(sem_bookings,'$',''),'#REF','0.0') as sem_bookings  ,
  regexp_replace(regexp_replace(sem_revenue,'$',''),'#REF','0.0') as sem_revenue   ,
  regexp_replace(regexp_replace(direct_bookings,'$',''),'#REF','0.0')as direct_bookings  ,
  regexp_replace(regexp_replace(direct_revenue,'$',''),'#REF','0.0') as direct_revenue   ,
  regexp_replace(regexp_replace(affiliates_spend,'$',''),'#REF','0.0') as affiliates_spend   ,
  regexp_replace(regexp_replace(affiliates_bookings,'$',''),'#REF','0.0')as affiliates_bookings   ,
  regexp_replace(regexp_replace(affiliates_revenue,'$',''),'#REF','0.0') as affiliates_revenue   ,
  regexp_replace(regexp_replace(mobilenetworks_spend,'$',''),'#REF','0.0') as mobilenetworks_spend   ,
  regexp_replace(regexp_replace(mobilenetworks_bookings,'$',''),'#REF','0.0') as mobilenetworks_bookings  ,
  regexp_replace(regexp_replace(mobilenetworks_revenue,'$',''),'#REF','0.0')as mobilenetworks_revenue   ,
  regexp_replace(regexp_replace(retargettingweb_spend,'$',''),'#REF','0.0') as retargettingweb_spend   ,
  regexp_replace(regexp_replace(retargettingweb_bookings,'$',''),'#REF','0.0') as retargettingweb_bookings  ,
  regexp_replace(regexp_replace(retargettingweb_revenue,'$',''),'#REF','0.0') as retargettingweb_revenue   ,
  regexp_replace(regexp_replace(meta_spend,'$',''),'#REF','0.0') as meta_spend  , 
  regexp_replace(regexp_replace(meta_bookings,'$',''),'#REF','0.0') as meta_bookings  ,
  regexp_replace(regexp_replace(meta_revenue,'$',''),'#REF','0.0') as meta_revenue   ,
  regexp_replace(regexp_replace(sessions_sem,'$',''),'#REF','0.0') as sessions_sem  , 
  regexp_replace(regexp_replace(sessions_seo,'$',''),'#REF','0.0') as sessions_seo  , 
  regexp_replace(regexp_replace(retargetting_app_spend,'$',''),'#REF','0.0') as retargetting_app_spend   ,
  regexp_replace(regexp_replace(retargetting_app_bookings,'$',''),'#REF','0.0') as retargetting_app_bookings  ,
  regexp_replace(regexp_replace(retargetting_app_revenue,'$',''),'#REF','0.0') as retargetting_app_revenue   ,
  regexp_replace(regexp_replace(influencers_spend,'$',''),'#REF','0.0') as influencers_spend   ,
  regexp_replace(regexp_replace(influencers_bookings,'$',''),'#REF','0.0') as influencers_bookings  ,
  regexp_replace(regexp_replace(influencers_revenue,'$',''),'#REF','0.0') as influencers_revenue   ,
  regexp_replace(regexp_replace(uac_spend,'$',''),'#REF','0.0') as uac_spend   ,
  regexp_replace(regexp_replace(uac_bookings,'$',''),'#REF','0.0') as uac_bookings  ,
  regexp_replace(regexp_replace(uac_revenue,'$',''),'#REF','0.0') as uac_revenue   ,
  regexp_replace(regexp_replace(totalspend,'$',''),'#REF','0.0') as totalspend   ,
  regexp_replace(regexp_replace(rollingspend,'$',''),'#REF','0.0') as rollingspend   
 from googlesheets.summary_sheet_tajawal_flights
 where `date`!= '' and `date`!= 'Date';

Truncate table googlesheets.summary_sheet_almosafer_flights_final;
 insert into googlesheets.summary_sheet_almosafer_flights_final  select 
 `date` ,
  regexp_replace(regexp_replace(sm_spend,'$',''),'#REF','0.0') as sm_spend   ,
  regexp_replace(regexp_replace(sm_bookings,'$',''),'#REF','0.0') as sm_bookings  ,
  regexp_replace(regexp_replace(sm_revenue,'$',''),'#REF','0.0') as sm_revenue   ,
  regexp_replace(regexp_replace(sm_app_spend,'$',''),'#REF','0.0') as sm_app_spend   ,
  regexp_replace(regexp_replace(sm_app_bookings,'$',''),'#REF','0.0') as sm_app_bookings  ,
  regexp_replace(regexp_replace(sm_app_revenue,'$',''),'#REF','0.0') as sm_app_revenue   ,
  regexp_replace(regexp_replace(sem_spend,'$',''),'#REF','0.0') as sem_spend   ,
  regexp_replace(regexp_replace(sem_bookings,'$',''),'#REF','0.0') as sem_bookings  ,
  regexp_replace(regexp_replace(sem_revenue,'$',''),'#REF','0.0') as sem_revenue   ,
  regexp_replace(regexp_replace(sessions_sem,'$',''),'#REF','0.0') as sessions_sem   ,
  regexp_replace(regexp_replace(sessions_seo,'$',''),'#REF','0.0') as sessions_seo   ,
  regexp_replace(regexp_replace(meta_spend,'$',''),'#REF','0.0') as meta_spend   ,
  regexp_replace(regexp_replace(meta_bookings,'$',''),'#REF','0.0') as meta_bookings  ,
  regexp_replace(regexp_replace(meta_revenue,'$',''),'#REF','0.0') as meta_revenue   ,
  regexp_replace(regexp_replace(total_spend,'$',''),'#REF','0.0') as total_spend   ,
  regexp_replace(regexp_replace(rolling_spend,'$',''),'#REF','0.0') as rolling_spend   ,
  regexp_replace(regexp_replace(retargettingair_spend,'$',''),'#REF','0.0') as retargettingair_spend   ,
  regexp_replace(regexp_replace(retargettingair_bookings,'$',''),'#REF','0.0') as retargettingair_bookings  ,
  regexp_replace(regexp_replace(retargettingair_revenue,'$',''),'#REF','0.0') as retargettingair_revenue   ,
  regexp_replace(regexp_replace(affiliates_spend,'$',''),'#REF','0.0') as affiliates_spend   ,
  regexp_replace(regexp_replace(affiliates_bookings,'$',''),'#REF','0.0') as affiliates_bookings  ,
  regexp_replace(regexp_replace(affiliates_revenue,'$',''),'#REF','0.0') as affiliates_revenue   ,
  regexp_replace(regexp_replace(mobilenetwork_spend,'$',''),'#REF','0.0') as mobilenetwork_spend   ,
  regexp_replace(regexp_replace(mobilenetwork_bookings,'$',''),'#REF','0.0') as mobilenetwork_bookings  ,
  regexp_replace(regexp_replace(mobilenetwork_revenue,'$',''),'#REF','0.0') as mobilenetwork_revenue  
 from googlesheets.summary_sheet_almosafer_flights
 where `date`!= '' and `date`!='Date';

Truncate table  googlesheets.summary_sheet_tajawal_hotel_final;
insert into table googlesheets.summary_sheet_tajawal_hotel_final
 select
  `date` ,
  regexp_replace(regexp_replace(sm_spend,'$',''),'#REF','0.0') as sm_spend   ,
  regexp_replace(regexp_replace(sm_bookings,'$',''),'#REF','0.0') as sm_bookings  ,
  regexp_replace(regexp_replace(sm_revenue,'$',''),'#REF','0.0') as sm_revenue   ,
  regexp_replace(regexp_replace(sm_app_spend,'$',''),'#REF','0.0') as sm_app_spend   ,
  regexp_replace(regexp_replace(sm_app_bookings,'$',''),'#REF','0.0') as sm_app_bookings   ,
  regexp_replace(regexp_replace(sm_app_revenue,'$',''),'#REF','0.0') as sm_app_revenue   ,
  regexp_replace(regexp_replace(sem_spend,'$',''),'#REF','0.0') as sem_spend   ,
  regexp_replace(regexp_replace(sem_bookings,'$',''),'#REF','0.0') as sem_bookings   ,
  regexp_replace(regexp_replace(sem_revenue,'$',''),'#REF','0.0') as sem_revenue   ,
  regexp_replace(regexp_replace(direct_bookings,'$',''),'#REF','0.0') as direct_bookings  ,
  regexp_replace(regexp_replace(direct_revenue,'$',''),'#REF','0.0') as direct_revenue   ,
  regexp_replace(regexp_replace(sessions_sem,'$',''),'#REF','0.0') as sessions_sem   ,
  regexp_replace(regexp_replace(blank,'$',''),'#REF','0.0') as blank   ,
  regexp_replace(regexp_replace(affiliates_spend,'$',''),'#REF','0.0') as affiliates_spend   ,
  regexp_replace(regexp_replace(affiliates_bookings,'$',''),'#REF','0.0') as affiliates_bookings  ,
  regexp_replace(regexp_replace(affiliates_revenue,'$',''),'#REF','0.0') as affiliates_revenue   ,
  regexp_replace(regexp_replace(mobile_spend,'$',''),'#REF','0.0') as mobile_spend   ,
  regexp_replace(regexp_replace(mobile_bookings,'$',''),'#REF','0.0') as mobile_bookings  ,
  regexp_replace(regexp_replace(mobile_revenue,'$',''),'#REF','0.0') as mobile_revenue   ,
  regexp_replace(regexp_replace(retargeting_spend,'$',''),'#REF','0.0') as retargeting_spend   ,
  regexp_replace(regexp_replace(retargeting_bookings,'$',''),'#REF','0.0') as retargeting_bookings  ,
  regexp_replace(regexp_replace(retargeting_revenue,'$',''),'#REF','0.0') as retargeting_revenue   ,
  regexp_replace(regexp_replace(seo_totalspend,'$',''),'#REF','0.0') as seo_totalspend   ,
  regexp_replace(regexp_replace(seo_rollingspend,'$',''),'#REF','0.0') as seo_rollingspend   
from googlesheets.summary_sheet_tajawal_hotel
 where `date`!= ''  and `date`!='Date';



Truncate table googlesheets.summary_sheet_almosafer_hotel_final;
insert into  table googlesheets.summary_sheet_almosafer_hotel_final 
 select 
  `date` string  ,
  regexp_replace(regexp_replace(sm_spend,'$',''),'#REF','0.0') as sm_spend   ,
  regexp_replace(regexp_replace(sm_bookings,'$',''),'#REF','0.0') as sm_bookings  ,
  regexp_replace(regexp_replace(sm_revenue,'$',''),'#REF','0.0') as sm_revenue   ,
  regexp_replace(regexp_replace(sm_app_spend,'$',''),'#REF','0.0') as sm_app_spend   ,
  regexp_replace(regexp_replace(sm_app_bookings,'$',''),'#REF','0.0') as sm_app_bookings   ,
  regexp_replace(regexp_replace(sm_app_revenue,'$',''),'#REF','0.0') as sm_app_revenue   ,
  regexp_replace(regexp_replace(sem_spend,'$',''),'#REF','0.0') as sem_spend   ,
  regexp_replace(regexp_replace(sem_bookings,'$',''),'#REF','0.0') as sem_bookings   ,
  regexp_replace(regexp_replace(sem_revenue,'$',''),'#REF','0.0') as sem_revenue   ,
  regexp_replace(regexp_replace(mobilenetworks_spend,'$',''),'#REF','0.0') as mobilenetworks_spend   ,
  regexp_replace(regexp_replace(mobilenetworks_bookings,'$',''),'#REF','0.0') as mobilenetworks_bookings  ,
  regexp_replace(regexp_replace(mobilenetworks_revenue,'$',''),'#REF','0.0') as mobilenetworks_revenue   ,
  regexp_replace(regexp_replace(sessions_sem,'$',''),'#REF','0.0') as sessions_sem   ,
  regexp_replace(regexp_replace(affiliates_spend,'$',''),'#REF','0.0') as affiliates_spend   ,
  regexp_replace(regexp_replace(affiliates_bookings,'$',''),'#REF','0.0') as affiliates_bookings   ,
  regexp_replace(regexp_replace(affiliates_revenue,'$',''),'#REF','0.0') as affiliates_revenue   ,
  regexp_replace(regexp_replace(retargeting_app_spend,'$',''),'#REF','0.0') as retargeting_app_spend   ,
  regexp_replace(regexp_replace(retargeting_app_bookings,'$',''),'#REF','0.0') as retargeting_app_bookings  ,
  regexp_replace(regexp_replace(retargeting_app_revenue,'$',''),'#REF','0.0') as retargeting_app_revenue   ,
  regexp_replace(regexp_replace(retargeting_app_revenue,'$',''),'#REF','0.0') as uac_spend   ,
  regexp_replace(regexp_replace(uac_bookings,'$',''),'#REF','0.0') as uac_bookings  ,
  regexp_replace(regexp_replace(uac_revenue,'$',''),'#REF','0.0') as uac_revenue   ,
  regexp_replace(regexp_replace(retargeting_spend,'$',''),'#REF','0.0') as retargeting_spend   ,
  regexp_replace(regexp_replace(retargeting_bookings,'$',''),'#REF','0.0') as retargeting_bookings  ,
  regexp_replace(regexp_replace(retargeting_revenue,'$',''),'#REF','0.0') as retargeting_revenue   ,
  regexp_replace(regexp_replace(total_spend,'$',''),'#REF','0.0') as total_spend   ,
  regexp_replace(regexp_replace(rolling_spend,'$',''),'#REF','0.0') as rolling_spend   

 from googlesheets.summary_sheet_almosafer_hotel
 where `date`!='' and `date`!='Date';

alter table googlesheets.dim_promo_grouping rename to googlesheets.dim_promo_grouping_inter;
create table googlesheets.dim_promo_grouping like googlesheets.dim_promo_grouping_inter;
insert into googlesheets.dim_promo_grouping
select * from googlesheets.dim_promo_grouping_inter
 where lower(`promo_id`)!='promo_id';
drop table googlesheets.dim_promo_grouping_inter;

drop table if exists googlesheets.mobile_adnetwork_final;
create table googlesheets.mobile_adnetwork_final row format delimited fields terminated by '\t' as select * from googlesheets.mobile_adnetwork where ltrim(rtrim(lower(value))) != 'value';

--Drop table if exists googlesheets.summary_sheet_almosafer_hotel;
--Drop table if exists googlesheets.summary_sheet_tajawal_hotel;
--Drop table if exists googlesheets.summary_sheet_tajawal_flights;
--Drop table if exists googlesheets.summary_sheet_almosafer_flights;

--Alter table googlesheets.summary_sheet_almosafer_hotel_intermediate1 rename To googlesheets.summary_sheet_almosafer_hotel;
--Alter table googlesheets.summary_sheet_tajawal_hotel_intermediate1 rename TO  googlesheets.summary_sheet_tajawal_hotel;
--Alter table googlesheets.summary_sheet_tajawal_flights_intermediate1 rename TO googlesheets.summary_sheet_tajawal_flights;
--Alter  table  googlesheets.summary_sheet_almosafer_flights_intermediate1 rename TO googlesheets.summary_sheet_almosafer_flights;

-- Remove trailing spaces in dim_adjust_network
drop table if exists googlesheets.dim_adjust_networks_inter;
create table if not exists googlesheets.dim_adjust_networks_inter like googlesheets.dim_adjust_networks;
insert into googlesheets.dim_adjust_networks_inter
select ltrim(rtrim(adjust_network_name)) as adjust_network_name, ltrim(rtrim(channel_group)) as channel_group, ltrim(rtrim(channel)) as channel, ltrim(rtrim(lower(paid_unpaid))) as paid_unpaid
from googlesheets.dim_adjust_networks;
drop table if exists googlesheets.dim_adjust_networks;
alter table googlesheets.dim_adjust_networks_inter rename to googlesheets.dim_adjust_networks;

-- Remove trailing spaces from dim_ga_sourcemedium_channels
drop table if exists googlesheets.dim_ga_sourcemedium_channels_inter;
create table if not exists googlesheets.dim_ga_sourcemedium_channels_inter like googlesheets.dim_ga_sourcemedium_channels;
insert into googlesheets.dim_ga_sourcemedium_channels_inter
select
ltrim(rtrim(ga_channel_grouping)) as ga_channel_grouping,
ltrim(rtrim(ga_sourcemedium)) as ga_sourcemedium,
ltrim(rtrim(ga_source)) as ga_source,
ltrim(rtrim(channel_group)) as channel_group,
ltrim(rtrim(channel)) as channel,
ltrim(rtrim(lower(paid_unpaid))) as paid_unpaid
from googlesheets.dim_ga_sourcemedium_channels;
drop table if exists googlesheets.dim_ga_sourcemedium_channels;
alter table googlesheets.dim_ga_sourcemedium_channels_inter rename to googlesheets.dim_ga_sourcemedium_channels;

-- Remove trailing spaces from dim_google_cpc_sourcemedium_mapping
drop table if exists googlesheets.dim_google_cpc_sourcemedium_mapping_inter;
create table if not exists googlesheets.dim_google_cpc_sourcemedium_mapping_inter like googlesheets.dim_google_cpc_sourcemedium_mapping;
insert into googlesheets.dim_google_cpc_sourcemedium_mapping_inter
select
ltrim(rtrim(campaign_name)) as campaign_name,
ltrim(rtrim(source_medium)) as source_medium,
ltrim(rtrim(channel_group)) as channel_group,
ltrim(rtrim(channel)) as channel,
ltrim(rtrim(paid_unpaid)) as paid_unpaid
from googlesheets.dim_google_cpc_sourcemedium_mapping;

drop table if exists googlesheets.dim_google_cpc_sourcemedium_mapping;
alter table googlesheets.dim_google_cpc_sourcemedium_mapping_inter rename to googlesheets.dim_google_cpc_sourcemedium_mapping;


