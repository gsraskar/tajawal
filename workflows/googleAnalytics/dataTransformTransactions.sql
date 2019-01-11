

drop table if exists ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra;
create table if not exists ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra as
select adnetwork_name, `date`, max(metric_name) as metric_name, brand, max(type) as type,
case when coalesce(product,'') = '' then 'all' else ltrim(rtrim(lower(product))) end as product,
max(coalesce(cast(value as double), 0.0)) as value,
case when coalesce(mobile_os,'') = '' then 'all' else ltrim(rtrim(lower(mobile_os))) end as mobile_os
from googlesheets.mobile_adnetwork
where lower(type) like '%affiliate%' and lower(adnetwork_name) != 'adnetwork_name'
group by 
adnetwork_name, 
`date`, 
brand,
case when coalesce(product,'') = '' then 'all' else ltrim(rtrim(lower(product))) end,
case when coalesce(mobile_os,'') = '' then 'all' else ltrim(rtrim(lower(mobile_os))) end
;

drop table if exists ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra rename to ${hiveDB}.tmp_mobile_adnetwork_gatra;
drop table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter;
create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as select * from ${hiveDB}.tmp_mobile_adnetwork_gatra;
-- Include data for mobile_os = 'all'

insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_inter
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type, q4.product,coalesce(q4.value,q3.value) as value,'all' as mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.product,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, product, count(*) 
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, product
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, product,max(value) as value
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where mobile_os='all' and not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, product
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.product=q2.product
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, product, avg(value) as value
  from ${hiveDB}.tmp_mobile_adnetwork_gatra where mobile_os != 'all' and not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, product
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.product=q4.product
;

drop table ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter rename to ${hiveDB}.tmp_mobile_adnetwork_gatra;
create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as select * from ${hiveDB}.tmp_mobile_adnetwork_gatra;
insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_inter
-- union
-- Include data for mobile_os = 'ios'
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type, q4.product,coalesce(q4.value,q3.value) as value,'ios' as mobile_os
from
( select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.product,q2.value
    from(select adnetwork_name, `date`, metric_name, brand, type, product, count(*) 
from ${hiveDB}.tmp_mobile_adnetwork_gatra where not `date` like '%date%'
group by adnetwork_name, `date`, metric_name, brand, type, product) q1
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product,max(value) as value
from ${hiveDB}.tmp_mobile_adnetwork_gatra where mobile_os='ios' and not `date` like '%date%'
group by adnetwork_name, `date`, metric_name, brand, type, product) q2
on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
and q1.brand=q2.brand and q1.type=q2.type and q1.product=q2.product
) q4
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product, value
from ${hiveDB}.tmp_mobile_adnetwork_gatra where mobile_os = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.product=q4.product
;

-- union
-- Include data for mobile_os = 'android'
drop table ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter rename to ${hiveDB}.tmp_mobile_adnetwork_gatra;
create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as select * from ${hiveDB}.tmp_mobile_adnetwork_gatra;
insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_inter
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type, q4.product,coalesce(q4.value,q3.value) as value,'android' as mobile_os
from
( select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.product,q2.value
    from(select adnetwork_name, `date`, metric_name, brand, type, product, count(*) 
from ${hiveDB}.tmp_mobile_adnetwork_gatra where not `date` like '%date%'
group by adnetwork_name, `date`, metric_name, brand, type, product) q1
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product, value
from ${hiveDB}.tmp_mobile_adnetwork_gatra where mobile_os='android' and not `date` like '%date%'
) q2
on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
and q1.brand=q2.brand and q1.type=q2.type and q1.product=q2.product
) q4
left outer join
(select adnetwork_name, `date`, metric_name, brand, type, product, value
from ${hiveDB}.tmp_mobile_adnetwork_gatra where mobile_os = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.product=q4.product;

drop table ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter rename to ${hiveDB}.tmp_mobile_adnetwork_gatra;
-- exit;
create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as 

select * from ${hiveDB}.tmp_mobile_adnetwork_gatra
-- insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_inter
union
-- create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as
-- Include data for product = 'all'
select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'all' as product,coalesce(q4.value,q3.value) as value,q4.mobile_os
from
(
select q1.adnetwork_name, q1.`date`, q1.metric_name, q1.brand, q1.type, q1.mobile_os, q2.value
from
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
  from ${hiveDB}.tmp_mobile_adnetwork_gatra where not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
) q1
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os,max(value) as value
  from ${hiveDB}.tmp_mobile_adnetwork_gatra where product='all' and not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
) q2
on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os,avg(value) as value
  from ${hiveDB}.tmp_mobile_adnetwork_gatra where product != 'all' and not `date` like '%date%'
  group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os
;

--union
-- Include data for product = 'package'

drop table ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter rename to ${hiveDB}.tmp_mobile_adnetwork_gatra;
-- exit;
create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as 

select * from ${hiveDB}.tmp_mobile_adnetwork_gatra
--insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_inter

union

select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'package' as product,coalesce(q4.value,q3.value) as value, q4.mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.mobile_os,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os,value
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where product='package' and not `date` like '%date%'
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
  from ${hiveDB}.tmp_mobile_adnetwork_gatra where product = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os
;
-- union
-- Include data for product = 'hotel'
drop table ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter rename to ${hiveDB}.tmp_mobile_adnetwork_gatra;
create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as 

select * from ${hiveDB}.tmp_mobile_adnetwork_gatra

union
-- insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_inter

select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'hotel' as product,coalesce(q4.value,q3.value) as value, q4.mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.mobile_os,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os,value
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where product='hotel' and not `date` like '%date%'
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
  from ${hiveDB}.tmp_mobile_adnetwork_gatra where product = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os
;
-- union
-- Include data for product = 'flight'
drop table ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter rename to ${hiveDB}.tmp_mobile_adnetwork_gatra;
create table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter as 

select * from ${hiveDB}.tmp_mobile_adnetwork_gatra

union
-- insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_inter

select q4.adnetwork_name, q4.`date`, q4.metric_name, q4.brand, q4.type,'flight' as product,coalesce(q4.value,q3.value) as value, q4.mobile_os
from
( 
  select q1.adnetwork_name,q1.`date`,q1.metric_name,q1.brand,q1.type,q1.mobile_os,q2.value
  from
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, count(*) 
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where not `date` like '%date%'
    group by adnetwork_name, `date`, metric_name, brand, type, mobile_os
  ) q1
  left outer join
  (
    select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
    from ${hiveDB}.tmp_mobile_adnetwork_gatra where product='flight' and not `date` like '%date%'
  ) q2
  on q1.adnetwork_name=q2.adnetwork_name and q1.`date`=q2.`date` and q1.metric_name=q2.metric_name 
  and q1.brand=q2.brand and q1.type=q2.type and q1.mobile_os=q2.mobile_os
) q4
left outer join
(
  select adnetwork_name, `date`, metric_name, brand, type, mobile_os, value
  from ${hiveDB}.tmp_mobile_adnetwork_gatra where product = 'all' and not `date` like '%date%'
) q3
on q3.adnetwork_name=q4.adnetwork_name and q3.`date`=q4.`date` and q3.metric_name=q4.metric_name 
and q3.brand=q4.brand and q3.type=q4.type and q3.mobile_os=q4.mobile_os;

drop table ${hiveDB}.tmp_mobile_adnetwork_gatra;
alter table ${hiveDB}.tmp_mobile_adnetwork_gatra_inter rename to ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra;


drop table if exists ${hiveDB}.tmp_mobile_adnetwork_gatra_cost_affiliate;
create table if not exists ${hiveDB}.tmp_mobile_adnetwork_gatra_cost_affiliate as
select A.adnetwork_name, A.cost_date as start_date,
LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name,mobile_os,product ORDER BY cost_date) as end_date,metric_name, value, brand, type,mobile_os,product
from
(
select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, value, brand, type,mobile_os,product
from
(
    select adnetwork_name, `date`, metric_name, value, brand, type,mobile_os,product from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra where `date` != 'date'
    union
    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, 0.0 as value, brand, type,mobile_os,product from
    (select adnetwork_name, brand, type,mobile_os,product,count(*) from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra where `date` != 'date' group by adnetwork_name, brand, type,mobile_os,product) T
) T1
where lower(brand) like '%almosafer%'
order by adnetwork_name, cost_date asc
) A;

insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_cost_affiliate
select A.adnetwork_name, A.cost_date as start_date,
LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name,mobile_os,product ORDER BY cost_date) as end_date,metric_name, value, brand, type,mobile_os,product
from
(
select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, value, brand, type,mobile_os,product
from
(
    select adnetwork_name, `date`, metric_name, value, brand, type,mobile_os,product from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra where `date` != 'date'
    union
    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, 0.0 as value, brand, type,mobile_os,product from
    (select adnetwork_name, brand, type,mobile_os,product,count(*) from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra where `date` != 'date' group by adnetwork_name, brand, type,mobile_os,product) T
) T1
where lower(brand) like '%tajawal%'
order by adnetwork_name, cost_date asc
) A;

--drop table if exists ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra_gatra;
--create table if not exists ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra_gatra as
--select adnetwork_name, `date`, max(metric_name) as metric_name, max(value) as value, brand, max(type) as type
--from googlesheets.mobile_adnetwork
--where lower(type) like '%affiliate%' and lower(adnetwork_name) != 'adnetwork_name'
--group by adnetwork_name, `date`, brand;
--
--drop table if exists ${hiveDB}.tmp_mobile_adnetwork_gatra_cost_affiliate_gatra;
--create table if not exists ${hiveDB}.tmp_mobile_adnetwork_gatra_cost_affiliate_gatra as
--select A.adnetwork_name, A.cost_date as start_date,
--LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name ORDER BY cost_date) as end_date,
--metric_name, value, brand, type
--from
--(
--select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, value, brand, type
--from
--(
--    select adnetwork_name, `date`, metric_name, value, brand, type from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra_gatra where `date` != 'date'
--    union
--    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, '0' as value, brand, type from
--    (select adnetwork_name, brand, type, count(*) from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra_gatra where `date` != 'date' group by adnetwork_name, brand, type) T
--) T1
--where lower(brand) like '%almosafer%'
--order by adnetwork_name, cost_date asc
--) A;
--
--insert into ${hiveDB}.tmp_mobile_adnetwork_gatra_cost_affiliate_gatra
--select A.adnetwork_name, A.cost_date as start_date,
--LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name ORDER BY cost_date) as end_date,
--metric_name, value, brand, type
--from
--(
--select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, value, brand, type
--from
--(
--    select adnetwork_name, `date`, metric_name, value, brand, type from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra_gatra where `date` != 'date'
--    union
--    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, '0' as value, brand, type from
--    (select adnetwork_name, brand, type, count(*) from ${hiveDB}.tmp_mobile_adnetwork_gatra_affiliate_gatra_gatra where `date` != 'date' group by adnetwork_name, brand, type) T
--) T1
--where lower(brand) like '%tajawal%'
--order by adnetwork_name, cost_date asc
--) A;


drop table if exists ${hiveDB}.fact_google_analytics_transactions_final_incr;

create table if not exists ${hiveDB}.fact_google_analytics_transactions_final_incr row format delimited fields terminated by '|'
AS
-- select C.*, B.dim_channel_id as dim_channel_id, d.metric_name, d.value
select C.sourcemedium, C.gadate, C.transaction_id, C.transactions, C.view_id, B.dim_channel_id as dim_channel_id, 
case when lower(B.channel_group) like 'meta' then 'CRR' else d.metric_name end as metric_name, 
case when lower(B.channel_group) like 'meta' then 2.0 else coalesce(d.value, 0.0) end as value
from
${hiveDB}.fact_google_analytics_transactions_incr C
-- tajawal_bi.fact_google_analytics_transactions_final C
left outer join 
(
  select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id 
  from ${hiveDB}.dim_spend_ga_sourcemedium_channels 
  group by lower(regexp_replace(ga_sourcemedium, ' ', ''))
) A on lower(regexp_replace(sourcemedium, ' ', '')) = lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
left outer join 
(
  select dim_channel_id, max(channel_group) as channel_group 
  from ${hiveDB}.dim_spend_channels group by dim_channel_id 
) B on A.dim_channel_id = B.dim_channel_id 
left outer join ${hiveDB}.dim_spend_ga_views on C.view_id = dim_spend_ga_views.dim_view_id
left outer join ${hiveDB}.tmp_mobile_adnetwork_gatra_cost_affiliate d 
on 
lower(rtrim(ltrim(regexp_replace(C.sourcemedium, ' ', '')))) = lower(rtrim(ltrim(regexp_replace(d.adnetwork_name, ' ', '')))) 
and lower(ltrim(rtrim(coalesce(dim_spend_ga_views.group_name, 'na')))) = lower(ltrim(rtrim(coalesce(d.brand, 'na'))))
and ltrim(rtrim(lower(case when coalesce(dim_spend_ga_views.mobile_os, 'NULL') = 'NULL' then 'all' else dim_spend_ga_views.mobile_os end))) = ltrim(rtrim(lower(case when coalesce(d.mobile_os, '') = '' then 'all' else d.mobile_os end)))
and case when lower(transaction_id) like 'h%' then 'hotel' else case when lower(transaction_id) like 'a%' then 'flight' else 'all' end end = ltrim(rtrim(lower(d.product)))
where
  (
    d.start_date is null
    or (regexp_replace(gadate, '-', '') >= d.start_date and regexp_replace(gadate, '-', '') < d.end_date)
  )
;

