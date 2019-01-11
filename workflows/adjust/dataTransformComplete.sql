set hive.mv.files.thread=0;

drop table if exists tajawal_bi.tmp_mobile_adnetwork;
create table if not exists tajawal_bi.tmp_mobile_adnetwork as
select lower(adnetwork_name) as adnetwork_name, regexp_replace(`date`, "\/\/", "\/") as `date`, max(metric_name) as metric_name,  brand, max(type) as type, 
case when coalesce(product,'') = '' then 'all' else lower(ltrim(rtrim(product))) end as product, max(value) as value, 
case when coalesce(mobile_os,'') = '' then 'all' else lower(ltrim(rtrim(mobile_os))) end as mobile_os
from googlesheets.mobile_adnetwork
where ltrim(rtrim(`date`)) != ''
group by lower(adnetwork_name), regexp_replace(`date`, "\/\/", "\/"), brand,
case when coalesce(product,'') = '' then 'all' else lower(ltrim(rtrim(product))) end, 
case when coalesce(mobile_os,'') = '' then 'all' else lower(ltrim(rtrim(mobile_os))) end
;
-- coalesce(mobile_os,'all'),coalesce(product,'all');

drop table tajawal_bi.tmp_mobile_adnetwork_inter;
create table tajawal_bi.tmp_mobile_adnetwork_inter as select * from tajawal_bi.tmp_mobile_adnetwork;
-- Include data for mobile_os = 'all'

insert into tajawal_bi.tmp_mobile_adnetwork_inter
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type, q4.product,coalesce(q4.value,q3.value) as value,'all' as mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.product,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, product, count(*) 
    from tajawal_bi.tmp_mobile_adnetwork where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, product
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, product,max(value) as value
    from tajawal_bi.tmp_mobile_adnetwork where mobile_os='all' and not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, product
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.product=q2.product
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, product, avg(value) as value
  from tajawal_bi.tmp_mobile_adnetwork where mobile_os != 'all' and not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, product
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.product=q4.product
;

drop table tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork;
create table tajawal_bi.tmp_mobile_adnetwork_inter as select * from tajawal_bi.tmp_mobile_adnetwork;
insert into tajawal_bi.tmp_mobile_adnetwork_inter
-- union
-- Include data for mobile_os = 'ios'
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type, q4.product,coalesce(q4.value,q3.value) as value,'ios' as mobile_os
from
( select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.product,q2.value
    from(select adnetwork_name, `date`, metric_name, brand, type, product, count(*) 
from tajawal_bi.tmp_mobile_adnetwork where not `date` like '%date%'
group by adnetwork_name, `date`, metric_name, brand, type, product) q1
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product,max(value) as value
from tajawal_bi.tmp_mobile_adnetwork where mobile_os='ios' and not `date` like '%date%'
group by adnetwork_name, `date`, metric_name, brand, type, product) q2
on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
and q1.brand=q2.brand and q1.type=q2.type and q1.product=q2.product
) q4
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product, value
from tajawal_bi.tmp_mobile_adnetwork where mobile_os = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.product=q4.product
;

-- union
-- Include data for mobile_os = 'android'
drop table tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork;
create table tajawal_bi.tmp_mobile_adnetwork_inter as select * from tajawal_bi.tmp_mobile_adnetwork;
insert into tajawal_bi.tmp_mobile_adnetwork_inter
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type, q4.product,coalesce(q4.value,q3.value) as value,'android' as mobile_os
from
( select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.product,q2.value
    from(select adnetwork_name, `date`, metric_name, brand, type, product, count(*) 
from tajawal_bi.tmp_mobile_adnetwork where not `date` like '%date%'
group by adnetwork_name, `date`, metric_name, brand, type, product) q1
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product, value
from tajawal_bi.tmp_mobile_adnetwork where mobile_os='android' and not `date` like '%date%'
) q2
on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
and q1.brand=q2.brand and q1.type=q2.type and q1.product=q2.product
) q4
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product, value
from tajawal_bi.tmp_mobile_adnetwork where mobile_os = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.product=q4.product;

drop table tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork;
-- exit;
create table tajawal_bi.tmp_mobile_adnetwork_inter as 

select * from tajawal_bi.tmp_mobile_adnetwork
-- insert into tajawal_bi.tmp_mobile_adnetwork_inter
union
-- create table tajawal_bi.tmp_mobile_adnetwork_inter as
-- Include data for product = 'all'
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'all' as product,coalesce(q4.value,q3.value) as value,q4.mobile_os
from
(
select q1.adnetwork_name, q1.`date`, q1.metric_name, q1.brand, q1.type, q1.mobile_os, q2.value
from
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
  from tajawal_bi.tmp_mobile_adnetwork where not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
) q1
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os,max(value) as value
  from tajawal_bi.tmp_mobile_adnetwork where product='all' and not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
) q2
on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os,avg(value) as value
  from tajawal_bi.tmp_mobile_adnetwork where product != 'all' and not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os
;

--union
-- Include data for product = 'package'

drop table tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork;
-- exit;
create table tajawal_bi.tmp_mobile_adnetwork_inter as 

select * from tajawal_bi.tmp_mobile_adnetwork
--insert into tajawal_bi.tmp_mobile_adnetwork_inter

union

select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'package' as product,coalesce(q4.value,q3.value) as value, q4.mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.mobile_os,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
    from tajawal_bi.tmp_mobile_adnetwork where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os,value
    from tajawal_bi.tmp_mobile_adnetwork where product='package' and not `date` like '%date%'
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
  from tajawal_bi.tmp_mobile_adnetwork where product = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os
;
-- union
-- Include data for product = 'hotel'
drop table tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork;
create table tajawal_bi.tmp_mobile_adnetwork_inter as 

select * from tajawal_bi.tmp_mobile_adnetwork

union
-- insert into tajawal_bi.tmp_mobile_adnetwork_inter

select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'hotel' as product,coalesce(q4.value,q3.value) as value, q4.mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.mobile_os,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
    from tajawal_bi.tmp_mobile_adnetwork where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os,value
    from tajawal_bi.tmp_mobile_adnetwork where product='hotel' and not `date` like '%date%'
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
  from tajawal_bi.tmp_mobile_adnetwork where product = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os
;
-- union
-- Include data for product = 'flight'
drop table tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork;
create table tajawal_bi.tmp_mobile_adnetwork_inter as 

select * from tajawal_bi.tmp_mobile_adnetwork

union
-- insert into tajawal_bi.tmp_mobile_adnetwork_inter

select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'flight' as product,coalesce(q4.value,q3.value) as value, q4.mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.mobile_os,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
    from tajawal_bi.tmp_mobile_adnetwork where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
    from tajawal_bi.tmp_mobile_adnetwork where product='flight' and not `date` like '%date%'
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
  from tajawal_bi.tmp_mobile_adnetwork where product = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os;

drop table tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork;

create table tajawal_bi.tmp_mobile_adnetwork_inter as

select adnetwork_name, `date`, metric_name, brand, type, mobile_os,
sum(case when lower(product) like '%hotel%' then value else 0 end) as hotel_cost,
sum(case when lower(product) like '%flight%' then value else 0 end) as flight_cost,
sum(case when lower(product) like '%package%' then value else 0 end) as package_cost,
sum(case when lower(product) like '%all%' then value else 0 end) as all_cost
from tajawal_bi.tmp_mobile_adnetwork
group by  adnetwork_name, `date`, metric_name, brand, type, mobile_os;

drop table tajawal_bi.tmp_mobile_adnetwork;
alter table  tajawal_bi.tmp_mobile_adnetwork_inter rename to  tajawal_bi.tmp_mobile_adnetwork;

drop table if exists tajawal_bi.tmp_mobile_adnetwork_cost;
create table if not exists tajawal_bi.tmp_mobile_adnetwork_cost as
select A.adnetwork_name,A.mobile_os, A.cost_date as start_date, 
LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name,mobile_os ORDER BY cost_date) as end_date,
metric_name, hotel_cost,flight_cost,package_cost,all_cost, brand, type
from
(
select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, hotel_cost,flight_cost,package_cost,all_cost, brand, type ,mobile_os
from 
(
    select adnetwork_name, `date`, metric_name, hotel_cost,flight_cost,package_cost,all_cost, brand,type,mobile_os from tajawal_bi.tmp_mobile_adnetwork where `date` != 'date'
    union
    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, 0 as hotel_cost,0 as flight_cost, 0 as package_cost, 0 as all_cost, brand, type,mobile_os from
    (select adnetwork_name, brand, type,mobile_os, count(*) from tajawal_bi.tmp_mobile_adnetwork where `date` != 'date' group by adnetwork_name, brand, type,mobile_os) T
) T1 
where lower(brand) like '%almosafer%'
order by adnetwork_name, cost_date asc
) A;

insert into tajawal_bi.tmp_mobile_adnetwork_cost
select A.adnetwork_name,A.mobile_os,A.cost_date as start_date,
LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name,mobile_os ORDER BY cost_date) as end_date,
metric_name, hotel_cost,flight_cost,package_cost,all_cost, brand, type
from
(
select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, hotel_cost,flight_cost,package_cost,all_cost, brand, type,mobile_os
from
(
    select adnetwork_name, `date`, metric_name, hotel_cost,flight_cost,package_cost,all_cost, brand, type,mobile_os from tajawal_bi.tmp_mobile_adnetwork where `date` != 'date'
    union
    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, 0 as hotel_cost,0 as flight_cost,0 as package_cost,0 as all_cost, brand, type,mobile_os from
    (select adnetwork_name, brand, type,mobile_os,count(*) from tajawal_bi.tmp_mobile_adnetwork where `date` != 'date' group by adnetwork_name, brand, type,mobile_os) T
) T1
where lower(brand) like '%tajawal%'
order by adnetwork_name, cost_date asc
) A;

-- exit;

-- drop table if exists tajawal_bi.fact_adjust_data_multi_app_inter;
drop table if exists tajawal_bi.fact_adjust_data_multi_app_final_reprocessed;
-- alter table tajawal_bi.fact_adjust_data_multi_app rename to tajawal_bi.fact_adjust_data_multi_app_inter;
--Drop table if exists tajawal_bi.fact_adjust_data;
Create table if not exists tajawal_bi.fact_adjust_data_multi_app_final_reprocessed ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
--tblproperties("skip.header.line.count"="1")
AS
select
  `tracker_token` , 
  `network` , 
  `campaign` , 
  `adgroup` , 
  `creative` , 
  regexp_replace(`date`, '-', '') as `date` , 
  `app_token`,
  f.`app_name`,
  country,
   COALESCE(`clicks`,0.0) as clicks , 
   COALESCE(`impressions`,0.0) as impressions, 
  COALESCE(`installs` ,0.0) as installs, 
  COALESCE(`click_conversion_rate` ,0.0) as click_conversion_rate, 
  COALESCE(`ctr` ,0.0) as ctr, 
  COALESCE(`impression_conversion_rate` ,0.0) as impression_conversion_rate, 
  COALESCE(`reattributions` ,0.0) as reattributions, 
  COALESCE(`sessions` ,0.0) as sessions, 
  COALESCE(`revenue_events` ,0.0) as revenue_events, 
  COALESCE(`revenue` ,0.0) as revenue, 
  COALESCE(`daus` ,0.0) as daus, 
  COALESCE(`waus` ,0.0) as waus, 
  COALESCE(`maus` ,0.0) as maus, 
  COALESCE(`hotel_revenue` ,0.0) as hotel_revenue , 
  COALESCE(`hotel_events` ,0.0) as hotel_events, 
  COALESCE(`hotel_revenue_per_event` ,0.0) as hotel_revenue_per_event, 
  COALESCE(`flight_revenue` ,0.0) as flight_revenue, 
  COALESCE(`flight_events` ,0.0) as flight_events, 
  COALESCE(`flight_revenue_per_event` ,0.0) as flight_revenue_per_event,
coalesce(hotelpage_events, 0.0) as hotelpage_events,
coalesce(flightdetails_events, 0.0) as flightdetails_events,
coalesce(hotelguestdetails_events, 0.0) as hotelguestdetails_events,
coalesce(flightpax_events, 0.0) as flightpax_events,
coalesce(hotelpayment_events, 0.0) as hotelpayment_events,
coalesce(flightpayment_events, 0.0) as flightpayment_events,
coalesce(hotelsearch_events, 0.0) as hotelsearch_events,
coalesce(flightsearch_events, 0.0) as flightsearch_events,
coalesce(holidays_revenue,0.0) as holidays_revenue,
coalesce(holidays_revenue_events,0.0) as holidays_revenue_events,
coalesce(holidays_package_details_events,0.0) as holidays_package_details_events,
coalesce(holidays_guest_details_events,0.0) as holidays_guest_details_events,
coalesce(holidays_payment_events,0.0) as holidays_payment_events,
-- (case when (lower(campaign) like '%hotel%' and lower(campaign) like '%flight%') or lower(campaign) like '%both%' then 'both' 
--   else case when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' then 'flight' 
--   else case when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' then 'hotel' else 'NA' end end end) as product
 -- , case when lower(campaign) like '%ksa\_%' or lower(campaign) like '%sa\_%' or lower(campaign) like '%kas\_%' then 'Saudi Arabia' 
--    else
--      case when lower(campaign) like '%ae\_%' or lower(campaign) like '%uae\_%' then 'UAE'
--      else
--        case when lower(campaign) like '%kw\_%' then 'Kuwait'
--        else
--    case when lower(campaign) like '%bh\_%' then 'Bahrain' else 'NA' end
--        end
--      end
--    end as pos
--  , case when lower(campaign) like '%android%' then 'android' else case when lower(campaign) like '%ios\_%' or lower(campaign) like '%\_ios%' then 'ios' else 'NA' end end as mobile_os
--  , case when lower(campaign) like '%ar\_%' or lower(campaign) like '%\_ar%' then 'ar' else case when lower(campaign) like '%en\_%' or lower(campaign) like '%\_en%' then 'en' else 'NA' end end as language

-- For criteo consider campaign name from criteo mapping else consider regular campaign name
case when lower(ds.channel) like '%criteo%' then
  case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
  else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
  end
  end
else
(
case when campaign like '%hotel%' and  campaign like '%flight%' 
     then 'both'
     when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
     then 'both'
     when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
     then 'flight' 
     when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
     then 'hotel' 
     when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'  
     then 'both' 
     when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
     then 'package'
     else 'NA' 
     end)                              
end
 as product,
-- (case when lower(campaign) like '%ksa%' or  lower(campaign) like '%sa\_%' or  lower(campaign) like '%kas\_%' then 'Saudi Arabia' else case when lower(campaign) like '%ae%' or  lower(campaign) like '%uae%' then 'UAE' else case when lower(campaign) like '%kw\_%' or lower(campaign) like '%kuw%' then 'kuwait' else case when lower(campaign) like '%bh\_%' or lower(campaign) like '%bah%' then 'Baharain' else 'NA' end end end end) as pos,

(
  case when lower(campaign) like '%\_sa%' or lower(campaign) like '%\-sa\-%' or lower(campaign) like '%\_ks%' or  lower(campaign) like '%ksa%' or (lower(campaign) like '%sa\_%' and lower(campaign) not like '%usa\_%') or  lower(campaign) like '%kas\_%' or ( lower(campaign) like '%192%' and lower(ds.channel) like '%revx%') 
    then 'Saudi Arabia' else 
      case when lower(campaign) like '%ae%' or  lower(campaign) like '%uae%' or ( lower(campaign) like '%226%' and lower(ds.channel) like '%revx%' )
      then 'UAE' else 
        case when lower(campaign) like '%kw\_%' or lower(campaign) like '%kuw%' or ( lower(campaign) like '%117%' and lower(ds.channel) like '%revx%' )
        then 'Kuwait' else 
          case when lower(campaign) like '%bh\_%' or lower(campaign) like '%bah%' or ( lower(campaign) like '%20\_%' and lower(campaign) not like '%220%' and lower(ds.channel) like '%revx%' )
          then 'Baharain' else 'others' 
          end 
        end 
      end 
    end
  ) as pos,
aa.mobile_os as mobile_os,
(case when lower(campaign) like '%ar\_%' or lower(campaign) like '%\_ar%' then 'ar' else case when lower(campaign) like '%en\_%' or lower(campaign) like '%\_en%' then 'en' else 'NA' end end) as language,


   c.metric_name as cost_metric_name
  ,-- coalesce(c.value, 0.0) as cost_metric_value
  coalesce(case when (  
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='hotel' 
       then c.hotel_cost
       when (  
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='flight' 
        then c.flight_cost
       when (  
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='package'
        then c.package_cost
        else c.all_cost
        end,0) as cost_metric_value, 
    --coalesce(case when lower(ds.channel_group) like '%meta%' or lower(ltrim(rtrim(c.metric_name))) = 'crr' then coalesce(revenue, 0.0) * 0.02 else
    --case when lower(ltrim(rtrim(c.metric_name))) = 'cpi' then coalesce(installs, 0) * coalesce(c.value, 0) else 
    --  case when lower(ltrim(rtrim(c.metric_name))) = 'cpa' then coalesce(revenue_events, 0) * coalesce(c.value, 0) else
    --    case when lower(ltrim(rtrim(c.metric_name))) = 'cpc' then coalesce(clicks, 0) * coalesce(c.value, 0) else 0
    --    end
    --  end
    --end
    --end, 0.0) as cost
  --,
  coalesce(case when (  
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='hotel' 
        then case lower(rtrim(ltrim(c.metric_name))) when 'cpi'
                                                     then coalesce(installs, 0) * hotel_cost
                                                     when 'cpa'
                                                     then coalesce(revenue_events, 0) *hotel_cost 
                                                     when 'crr'
                                                     then coalesce(revenue,0)/100 * hotel_cost
             end
        when (
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='flight'
        then case lower(rtrim(ltrim(c.metric_name))) when 'cpi'
                                                     then coalesce(installs, 0) * flight_cost
                                                     when 'cpa'
                                                     then coalesce(revenue_events, 0) *flight_cost
                                                     when 'crr'
                                                     then coalesce(revenue,0)/100 * flight_cost
             end 
         when (
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='package'
         then case lower(rtrim(ltrim(c.metric_name))) when 'cpi'
                                                     then coalesce(installs, 0) * package_cost
                                                     when 'cpa'
                                                     then coalesce(revenue_events, 0) *package_cost
                                                     when 'crr'
                                                     then coalesce(revenue,0)/100 * package_cost
             end


         when (
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='NA'
         then case lower(rtrim(ltrim(c.metric_name))) when 'cpi'
                                                     then coalesce(installs, 0) * all_cost
                                                     when 'cpa'
                                                     then coalesce(revenue_events, 0) *all_cost
                                                     when 'crr'
                                                     then coalesce(revenue,0)/100 * all_cost
             end
        -- CASE FOR 'both'
         when (
       case when lower(ds.channel) like '%criteo%' then
       case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%hotel%' then 'hotel'
          else case when lower(coalesce(ccm.criteo_campaign, campaign)) like '%flight%' then 'flight' else 'NA'
       end
       end
          else
             (
             case when campaign like '%hotel%' and  campaign like '%flight%'
                  then 'both'
                  when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
                  then 'both'
                  when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%' or lower(campaign) like '%airline%' or upper(campaign) like '%-FL' or upper(campaign) like '%\_FL'
                  then 'flight'
                  when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
                  then 'hotel'
                  when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\_fh\_%'
                  then 'both'
                  when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
                  then 'package'
                  else 'NA'
             end)
        end)='both'
         then case lower(rtrim(ltrim(c.metric_name))) when 'cpi'
                                                     then coalesce(installs, 0) * all_cost
                                                     when 'cpa'
                                                     then coalesce(revenue_events, 0) *all_cost
                                                     when 'crr'
                                                     then coalesce(revenue,0)/100 * all_cost
             end
         end,0) as cost,

  coalesce(case when lower(ltrim(rtrim(c.metric_name))) = 'cpi' 
       -- then installs * c.hotel_cost/2
       -- Rule here is : All spend for tajawal for CPI networks goes to flight and not hotel, for other than tajawal it is split half into hotel and flight
       then case when lower(f.app_name) like '%tajawal%' then 0 else installs * c.hotel_cost/2 end 
       when lower(ltrim(rtrim(c.metric_name))) = 'cpa'
       then hotel_events * c.hotel_cost
       when lower(ltrim(rtrim(c.metric_name))) = 'crr'
       then hotel_revenue * c.hotel_cost / 100 
       end,0) as hotel_spend, 
  coalesce(case when lower(ltrim(rtrim(c.metric_name))) = 'cpi'
       -- then installs * c.flight_cost / 2
       -- Rule here is : All spend of tajawal for CPI networks goes to flight and not hotels
       then case when lower(f.app_name) like '%tajawal%' then installs * c.flight_cost else installs * c.flight_cost / 2 end 
       when lower(ltrim(rtrim(c.metric_name))) = 'cpa'
       then flight_events * c.flight_cost
       when lower(ltrim(rtrim(c.metric_name))) = 'crr'
       then flight_revenue * c.flight_cost / 100
       end,0) as flight_spend,
  coalesce(case when lower(ltrim(rtrim(c.metric_name))) = 'cpi'
       then 0
       when lower(ltrim(rtrim(c.metric_name))) = 'cpa'
       then holidays_revenue_events * c.package_cost
       when lower(ltrim(rtrim(c.metric_name))) = 'crr'
       then holidays_revenue * c.package_cost / 100
       end,0) as package_spend,
  ds.dim_channel_id,
  case when lower(ltrim(rtrim(channel_group))) like '%sem%' and lower(ltrim(rtrim(campaign))) like '%brand%' then 'brand'
       when lower(ltrim(rtrim(channel_group))) like '%crm%' or lower(ltrim(rtrim(channel_group))) like  '%display%' then 'brand'
       else 'Non Brand'
  end as brand_nonbrand,
  case when lower(ltrim(rtrim(channel_group))) like '%social media%' and lower(ltrim(rtrim(campaign))) like '%nc%' then 'Aquisition'
       when lower(ltrim(rtrim(channel_group))) like '%social media%' then 'Retargeting'
       when lower(ltrim(rtrim(channel_group))) like '%retargeting%' or lower(ltrim(rtrim(channel_group))) like  '%crm%' then 'Retargeting'
       when lower(ltrim(rtrim(channel_group))) like '%direct%' then 'NA' else 'Aquisition'
  end as objective
  from tajawal_bi.fact_adjust_data_multi_app f

  -- Criteo campaign names contain correct product name so including that here
  left outer join ( select lower(ltrim(rtrim(ga_adjust_campaign))) as ga_adjust_campaign, max(criteo_campaign) as criteo_campaign 
  from tajawal_bi.criteo_campaign_mapping group by lower(ltrim(rtrim(ga_adjust_campaign))) ) ccm 
  on lower(ltrim(rtrim(f.campaign))) = ccm.ga_adjust_campaign
  inner join tajawal_bi.dim_spend_adjust_apps aa on f.app_token = aa.dim_app_id 
  left outer join (select lower(regexp_replace(adjust_network_name, ' ', '')) as adjust_network_name, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_adjust_network_channels group by lower(regexp_replace(adjust_network_name, ' ', ''))) an on lower(regexp_replace(f.network, ' ', '')) = lower(regexp_replace(an.adjust_network_name, ' ', ''))
  left outer join tajawal_bi.dim_spend_channels ds on an.dim_channel_id = ds.dim_channel_id
  left outer join tajawal_bi.tmp_mobile_adnetwork_cost c on lower(rtrim(ltrim(regexp_replace(f.network, ' ', '')))) = lower(rtrim(ltrim(regexp_replace(c.adnetwork_name, ' ', '')))) and ltrim(rtrim(lower(aa.mobile_os)))=ltrim(rtrim(lower(c.mobile_os))) and
   ltrim(rtrim(aa.group_name)) = ltrim(rtrim(c.brand))
  where
  ( 
    (c.start_date is null
    or (regexp_replace(`date`, '-', '') >= c.start_date and regexp_replace(`date`, '-', '') < c.end_date)) 
  )
  and tracker_token != 'tracker_token';

-- drop table if exists tajawal_bi.fact_adjust_data_multi_app_inter;
exit;

-- ==== Step 1 : Merge new channels seen in googlesheets.dim_adjust_networks into dim_spend_channels table ==== --
-- Create table dim_spend_channels if not exists
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_channels (
    dim_channel_id string,
    channel_group string,
    channel string,
    paid_unpaid string
);
-- Populate table with new channels
DROP TABLE IF EXISTS tajawal_bi.dim_spend_channels_inter;
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_channels_inter row format delimited fields terminated by ',' as
-- SELECT *
SELECT dim_channel_id,
case when max(coalesce(channel_group, -1)) = '-1' then null else max(coalesce(channel_group, -1)) end as channel_group,
case when max(coalesce(channel, -1)) = '-1' then null else max(coalesce(channel, -1)) end as channel,
case when max(coalesce(paid_unpaid, -1)) = '-1' then null else max(coalesce(paid_unpaid, -1)) end as paid_unpaid
FROM
(
    -- select * from
    -- tajawal_bi.dim_spend_channels
    -- union
    select * from
    (
        select md5(lower(regexp_replace(concat(A.channel_group, A.channel), ' ', ''))) as dim_channel_id, lower(ltrim(rtrim(A.channel_group))) as channel_group, lower(ltrim(rtrim(A.channel))) as channel, A.paid_unpaid
        from
        (
            select channel_group, channel, paid_unpaid, count(*)
            from googlesheets.dim_ga_sourcemedium_channels
            group by channel_group, channel, paid_unpaid
            union 
            select channel_group, channel, paid_unpaid, count(*) 
            from googlesheets.dim_adjust_networks 
            group by channel_group, channel, paid_unpaid
            union
            select channel_group, channel, paid_unpaid, count(*)
            from googlesheets.dim_google_cpc_sourcemedium_mapping
            group by channel_group, channel, paid_unpaid
        ) A 
        -- left outer join tajawal_bi.dim_spend_channels ds 
        -- ON A.channel_group = ds.channel_group and A.channel = ds.channel
        -- where ds.channel is null
    ) T2
) T group by dim_channel_id;
DROP TABLE tajawal_bi.dim_spend_channels;
ALTER TABLE tajawal_bi.dim_spend_channels_inter RENAME TO tajawal_bi.dim_spend_channels;

-- ==== Step 2 : Merge new network=>channel mapping into dim_adjust_network table ==== --
-- Create table dim_spend_adjust_network_channels
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_adjust_network_channels (
    dim_network string,
    dim_channel_id string
);
-- POPULATE table with new channels.
DROP TABLE IF EXISTS tajawal_bi.dim_spend_adjust_network_channels_inter;
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_adjust_network_channels_inter row format delimited fields terminated by ',' as
SELECT *
FROM
(
--    select * from
--    (
--        select * from tajawal_bi.dim_spend_adjust_network_channels
--    ) T1
--    union
    select T2.adjust_network_name, T2.dim_channel_id from
    (
        select da.adjust_network_name, da.channel_group, da.channel, ds.dim_channel_id
        from googlesheets.dim_adjust_networks da inner join tajawal_bi.dim_spend_channels ds 
	on lower(regexp_replace(concat(da.channel_group, da.channel), ' ', '')) = lower(regexp_replace(concat(ds.channel_group, ds.channel), ' ', ''))
        -- on da.channel_group = ds.channel_group and da.channel = ds.channel
    ) T2 
--    left outer join tajawal_bi.dim_spend_adjust_network_channels dsa on T2.dim_channel_id = dsa.dim_channel_id
--    where dsa.dim_channel_id is null
) T;
DROP TABLE tajawal_bi.dim_spend_adjust_network_channels;
ALTER TABLE tajawal_bi.dim_spend_adjust_network_channels_inter RENAME TO tajawal_bi.dim_spend_adjust_network_channels;

Drop table if exists tajawal_bi.fact_mobilenetwork_data_detailed_reprocessed;
Create table if not exists tajawal_bi.fact_mobilenetwork_data_detailed_reprocessed row format delimited fields terminated by ','  as
select C.channel_group, C.channel, B.group_name, A.*
from tajawal_bi.fact_adjust_data_multi_app_final A inner join tajawal_bi.dim_spend_adjust_apps B on A.app_token = B.dim_app_id
inner join
(
    select B.channel_group, B.channel, A.dim_channel_id, A.adjust_network_name, count(*) 
    from tajawal_bi.dim_spend_adjust_network_channels A inner join tajawal_bi.dim_spend_channels B 
    on A.dim_channel_id = B.dim_channel_id 
    where lower(B.channel_group) like '%mobile%network%'
    group by B.channel_group, B.channel, A.dim_channel_id, A.adjust_network_name
) C on regexp_replace(A.network, ' ', '') = regexp_replace(C.adjust_network_name, ' ', '');


Drop table if exists tajawal_bi.fact_mobilenetwork_data_incr;
Create table if not exists tajawal_bi.fact_mobilenetwork_data_incr row format delimited fields terminated by ',' as
select C.channel_group, C.channel, B.group_name, 
A.network,
A.campaign,
A.`date`,
A.app_token,
A.app_name,
A.product,
A.pos,
-- coalesce(B.mobile_os, 'na') as mobile_os,
A.mobile_os,
A.language,
round(sum(A.clicks),2) as clicks,
round(sum(A.impressions),2) as impressions,
round(sum(A.installs),2) as installs,
round(sum(A.sessions),2) as sessions,
round(sum(A.revenue_events),2) as bookings,
round(sum(A.revenue),2) as revenue,
round(sum(A.hotelsearch_events),2) as hotelsearch_events,
round(sum(A.flightsearch_events),2) as flightsearch_events,
round(sum(revenue - flight_revenue),2) as hotel_revenue,
round(sum(revenue_events - flight_events),2) as hotel_events,
round(sum(hotel_revenue_per_event),2) as hotel_revenue_per_event,
round(sum(flight_revenue),2) as flight_revenue,
round(sum(flight_events),2) as flight_events,
round(sum(flight_revenue_per_event),2) as flight_revenue_per_event,
round(sum(hotelpage_events),2) as hotelpage_events,
round(sum(flightdetails_events),2) as flightdetails_events,
round(sum(hotelguestdetails_events),2) as hotelguestdetails_events,
round(sum(flightpax_events),2) as flightpax_events,
round(sum(hotelpayment_events),2) as hotelpayment_events,
round(sum(flightpayment_events),2) as flightpayment_events,
round(sum(cost),2) as spend
-- select A.network, sum(clicks) as clicks, sum(impressions), sum(installs), sum(sessions), sum(revenue_events), sum(revenue), sum(cost)
from tajawal_bi.fact_adjust_data_multi_app_final_reprocessed A inner join tajawal_bi.dim_spend_adjust_apps B on A.app_token = B.dim_app_id
inner join
(
    select B.channel_group, B.channel, A.dim_channel_id, A.adjust_network_name, count(*) 
    from tajawal_bi.dim_spend_adjust_network_channels A inner join tajawal_bi.dim_spend_channels B 
    on A.dim_channel_id = B.dim_channel_id 
    where lower(B.channel_group) like '%mobile%network%'
    group by A.dim_channel_id, A.adjust_network_name, B.channel_group, B.channel
) C on ltrim(rtrim(lower(regexp_replace(A.network, ' ', '')))) = ltrim(rtrim(lower(regexp_replace(C.adjust_network_name, ' ', ''))))
group by C.channel_group, C.channel, B.group_name, 
A.network,
A.campaign,
A.`date`,
A.app_token,
A.app_name,
A.product,
A.pos,
A.mobile_os,
A.language;


drop table if exists  tajawal_bi.fact_adjust_active_users_incr;
create table if not exists tajawal_bi.fact_adjust_active_users_incr ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
as
select 
apps.app_name,
channels.channel_group,
channels.channel,
adjust.product,
adjust.pos,
adjust.`date`,
sum(adjust.daus) as daus,
sum(adjust.maus) as maus,
sum(adjust.waus) as waus
from
tajawal_bi.fact_adjust_data_multi_app_final_incr adjust 
left outer join tajawal_bi.dim_spend_adjust_apps apps 
on apps.dim_app_id=adjust.app_token
left outer join tajawal_bi.dim_spend_channels channels
on adjust.dim_channel_id=channels.dim_channel_id 
group by apps.app_name,channels.channel_group,channels.channel,adjust.product,adjust.pos,adjust.`date`;