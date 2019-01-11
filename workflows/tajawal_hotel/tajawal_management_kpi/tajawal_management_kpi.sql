set hive.mv.files.thread=0;

-- STEP 1 : Create an intermediate table which will host data from both flight and hotel orders.
drop table if exists ${hiveDB}.intermediate_dashboard_management_kpi_input;
create table if not exists ${hiveDB}.intermediate_dashboard_management_kpi_input as
select id as order_no, dim_group_id, order_type, dim_bookingdate_id, dim_bookingpaid_id, ibv, gbv
from tajawal_bi.fact_hotel_orders_final
where dim_status_id != 91-- and dim_group_id != '5'
union all
select id as order_no, dim_group_id, order_type, dim_bookingdate_id, dim_bookingpaid_id, ibv, gbv
from tajawal_bi.fact_flight_orders_final
where dim_status_id != 91
union all
select id as order_no, '1' as dim_group_id, 'package' as order_type, 
coalesce(case when dim_bookingdate_id = 'null' then null else dim_bookingdate_id end, dim_date_id) as dim_bookingdate_id, 
coalesce(case when dim_bookingpaid_id = 'null' then null else dim_bookingpaid_id end, dim_date_id) as dim_bookingpaid_id, 
coalesce(iov, 0) as iov, 
case when dim_bookingpaid_id = 'null' then 0 else coalesce(gbv, 0) end as gbv
from
( select * from tajawal_bi.fact_package_orders_final where dim_status_id != 91 and dim_group_id = '1') A
right outer join
(
  select distinct(dim_date_id) as dim_date_id from dim_date where dim_date_id >= '20170601' and dim_date_id <= from_unixtime(unix_timestamp(current_timestamp, 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd') order by dim_date_id asc
) as dim_date on dim_bookingdate_id = dim_date_id
-- where dim_status_id != 91 and dim_group_id = '1'
union all
select id as order_no, '2' as dim_group_id, 'package' as order_type, 
coalesce(case when dim_bookingdate_id = 'null' then null else dim_bookingdate_id end, dim_date_id) as dim_bookingdate_id, 
coalesce(case when dim_bookingpaid_id = 'null' then null else dim_bookingpaid_id end, dim_date_id) as dim_bookingpaid_id, 
coalesce(iov, 0) as iov, 
case when dim_bookingpaid_id = 'null' then 0 else coalesce(gbv, 0) end as gbv
from
(select * from tajawal_bi.fact_package_orders_final where dim_status_id != 91 and dim_group_id = '2') A
right outer join
(
  select distinct(dim_date_id) as dim_date_id from dim_date where dim_date_id >= '20170601' and dim_date_id <= from_unixtime(unix_timestamp(current_timestamp, 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd') order by dim_date_id asc
) as dim_date on dim_bookingdate_id = dim_date_id
-- where dim_status_id != 91 and dim_group_id = '2'
;

-- STEP 1.1 : Calculate Spend at required level
drop table if exists ${hiveDB}.intermediate_spend_table;
create table if not exists ${hiveDB}.intermediate_spend_table as
select substr(dim_date_id, 1, 6) as dim_date_month, dim_date_id, case when lower(ltrim(rtrim(group_name))) = 'tajawal' then 1 else case when lower(ltrim(rtrim(group_name))) = 'almosafer' then 2 else -1 end end as group_name, lower(ltrim(rtrim(product))) as product, sum(spend) as total_spend 
from tajawal_bi.fact_spend_data_channelwise 
group by substr(dim_date_id, 1, 6), dim_date_id, lower(ltrim(rtrim(group_name))), lower(ltrim(rtrim(product)));


-- STEP 2 : Calculate IOV
DROP TABLE IF EXISTS ${hiveDB}.intermediate2_dashboard_management_kpi_input;
CREATE TABLE IF NOT EXISTS ${hiveDB}.intermediate2_dashboard_management_kpi_input as
select * from
(
  select A.booking_date, A.Brand , A.Product , A.KPI , A.Value ,
  SUM(A.Value) OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date)
  from
  (
   select
   dim_bookingdate_id as booking_date, 
   from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyyMM') as booking_month,
   dim_group_id as Brand,
   order_type as Product,
   'IBV ($)'  as KPI,
   SUM(ibv) as Value
   from ${hiveDB}.intermediate_dashboard_management_kpi_input as HO
   --where year(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))= 2017  and month(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))>2
   group by dim_bookingdate_id,dim_group_id,order_type order by dim_bookingdate_id asc
  ) A
  --order by A.booking_date, A.Brand , A.Product , A.KPI asc
  
  UNION ALL
  
  -- STEP 3 : Calculate Transaction Count
  select A.booking_date, A.Brand , A.Product , A.KPI , A.Value ,
  SUM(A.Value) OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date)
  from
  (
   select
   dim_bookingdate_id as booking_date, 
   from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyyMM') as booking_month,
   dim_group_id as Brand,
   order_type as Product,
   '# of Transactions'  as KPI,
   count(distinct(order_no)) as Value
   from ${hiveDB}.intermediate_dashboard_management_kpi_input as HO
   --where year(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))= 2017  and month(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))>2
   group by dim_bookingdate_id,dim_group_id,order_type order by dim_bookingdate_id asc
  ) A
  --order by A.booking_date, A.Brand , A.Product , A.KPI asc

  UNION ALL

-- STEP 4 : Calcualte GBV
  select A.booking_date, A.Brand , A.Product , A.KPI , A.Value ,
  SUM(A.Value) OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date)
  from  
  (
   select
   dim_bookingpaid_id as booking_date, 
   from_unixtime(unix_timestamp(dim_bookingpaid_id, 'yyyyMMdd'), 'yyyyMM') as booking_month,
   dim_group_id as Brand,
   order_type as Product,
   'GBV ($)'  as KPI,
   SUM(GBV) as Value
   from ${hiveDB}.intermediate_dashboard_management_kpi_input as HO
   --where year(from_unixtime(unix_timestamp(dim_bookingpaid_id, 'yyyyMMdd'), 'yyyy-MM-dd'))= 2017  and month(from_unixtime(unix_timestamp(dim_bookingpaid_id, 'yyyyMMdd'), 'yyyy-MM-dd'))>2
   group by dim_bookingpaid_id,dim_group_id,order_type order by dim_bookingpaid_id asc
  ) A
  --order by A.booking_date, A.Brand , A.Product , A.KPI asc

  UNION ALL

  -- STEP 5 : AOV
  select A.booking_date, A.Brand , A.Product , A.KPI , A.IBV ,
  SUM(A.Value)  OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date) /
  SUM(A.distinct_count) OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date)
  from  
  (
   select
   dim_bookingdate_id as booking_date, 
   from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyyMM') as booking_month,
   dim_group_id as Brand,
   order_type as Product,
   'AOV ($)'  as KPI,
   SUM(IBV) as Value,
   SUM(IBV) / count(distinct(order_no)) as IBV,
   count(distinct(order_no)) as distinct_count
   from ${hiveDB}.intermediate_dashboard_management_kpi_input as HO
   --where year(from_unixtime(unix_timestamp(dim_bookingpaid_id, 'yyyyMMdd'), 'yyyy-MM-dd'))= 2017  and month(from_unixtime(unix_timestamp(dim_bookingpaid_id, 'yyyyMMdd'), 'yyyy-MM-dd'))>2
   group by dim_bookingdate_id,dim_group_id,order_type order by dim_bookingdate_id asc
  ) A
  --order by A.booking_date, A.Brand , A.Product , A.KPI asc

  UNION ALL

  -- STEP 6 : CRR
  select A.booking_date, A.Brand , A.Product , A.KPI , A.Value, -- B.total_spend / A.Value ,
  -- SUM(B.total_spend) OVER(PARTITION BY B.group_name, B.product, B.dim_date_month order by B.dim_date_id) / 
  SUM(A.Value) OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date)
  from  
  (
   select
   dim_bookingdate_id as booking_date, 
   from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyyMM') as booking_month,
   dim_group_id as Brand,
   order_type as Product,
   'CRR ($)'  as KPI,
   SUM(IBV) as Value
   from ${hiveDB}.intermediate_dashboard_management_kpi_input as HO
   --where year(from_unixtime(unix_timestamp(dim_bookingpaid_id, 'yyyyMMdd'), 'yyyy-MM-dd'))= 2017  and month(from_unixtime(unix_timestamp(dim_bookingpaid_id, 'yyyyMMdd'), 'yyyy-MM-dd'))>2
   group by dim_bookingdate_id,dim_group_id,order_type order by dim_bookingdate_id asc
  ) A left outer join ${hiveDB}.intermediate_spend_table B 
  on A.booking_date = B.dim_date_id and A.booking_month = B.dim_date_month and ltrim(rtrim(lower(A.Brand))) = ltrim(rtrim(lower(B.group_name))) and ltrim(rtrim(lower(A.product))) = ltrim(rtrim(lower(B.product))) 
  --order by A.booking_date, A.Brand , A.Product , A.KPI asc

  UNION ALL

  -- STEP 7 : CPO
  select A.booking_date, A.Brand , A.Product , A.KPI , A.Value, -- B.total_spend / A.Value ,
  -- SUM(B.total_spend) OVER(PARTITION BY B.group_name, B.product, B.dim_date_month order by B.dim_date_id) /
  SUM(A.Value) OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date)
  from
  (
   select
   dim_bookingdate_id as booking_date, 
   from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyyMM') as booking_month,
   dim_group_id as Brand,
   order_type as Product,
   'CPO ($)'  as KPI,
   count(distinct(order_no)) as Value
   from ${hiveDB}.intermediate_dashboard_management_kpi_input as HO
   --where year(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))= 2017  and month(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))>2
   group by dim_bookingdate_id,dim_group_id,order_type order by dim_bookingdate_id asc
  ) A 
  -- left outer join ${hiveDB}.intermediate_spend_table B 
  -- on A.booking_date = B.dim_date_id and A.booking_month = B.dim_date_month and ltrim(rtrim(lower(A.Brand))) = ltrim(rtrim(lower(B.group_name))) and ltrim(rtrim(lower(A.product))) = ltrim(rtrim(lower(B.product)))

  UNION ALL

  -- STEP 8 : Spend
  select A.booking_date, A.Brand , A.Product , A.KPI , B.total_spend ,
  SUM(B.total_spend) OVER(PARTITION BY B.group_name, B.product, B.dim_date_month order by B.dim_date_id) -- SUM(A.Value) OVER (PARTITION BY A.Brand, A.Product, A.booking_month order by A.booking_date)
  from
  (
   select
   dim_bookingdate_id as booking_date, 
   from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyyMM') as booking_month,
   dim_group_id as Brand,
   order_type as Product,
   'Spend ($)'  as KPI,
   sum(0) as Value
   from ${hiveDB}.intermediate_dashboard_management_kpi_input as HO
   --where year(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))= 2017  and month(from_unixtime(unix_timestamp(dim_bookingdate_id, 'yyyyMMdd'), 'yyyy-MM-dd'))>2
   group by dim_bookingdate_id,dim_group_id,order_type order by dim_bookingdate_id asc
  ) A left outer join ${hiveDB}.intermediate_spend_table B 
  on A.booking_date = B.dim_date_id and A.booking_month = B.dim_date_month and ltrim(rtrim(lower(A.Brand))) = ltrim(rtrim(lower(B.group_name))) and ltrim(rtrim(lower(A.product))) = ltrim(rtrim(lower(B.product)))

  --order by A.booking_date, A.Brand , A.Product , A.KPI asc
) B;

DROP TABLE IF EXISTS ${hiveDB}.dashboard_management_kpi_input;
drop table if exists ${hiveDB}.tmp11_dashboard_management_kpi_input;

ALTER TABLE ${hiveDB}.intermediate2_dashboard_management_kpi_input RENAME TO ${hiveDB}.tmp11_dashboard_management_kpi_input;
CREATE TABLE if not exists ${hiveDB}.dashboard_management_kpi_input as 
Select 
booking_date,
case when brand = '5' then 'offline' else 'online' end as business_type,
brand, product, kpi, value, sum_window_0 
from ${hiveDB}.tmp11_dashboard_management_kpi_input;

--drop table if exists ${hiveDB}.tmp_dashboard_management_kpi_input;
drop table if exists ${hiveDB}.intermediate_dashboard_management_kpi_input;
drop table if exists ${hiveDB}.intermediate2_dashboard_management_kpi_input;

-- ======= Create data for almosafer offline flight records
drop table if exists ${hiveDB}.dashboard_management_kpi_input_inter;
create table ${hiveDB}.dashboard_management_kpi_input_inter like ${hiveDB}.dashboard_management_kpi_input;

insert into ${hiveDB}.dashboard_management_kpi_input_inter

select * from (

select hotel_gbv.booking_date as day, 'offline' as business_type, '5' as brand, 'flight' as product, 'GBV ($)' as kpi, 
case when all_gbv.value > 0 then (all_gbv.value - case when hotel_gbv.value is null then 0 else hotel_gbv.value end) else 0 end as value, 
case when all_gbv.mtd_value > 0 then (all_gbv.mtd_value - case when hotel_gbv.mtd is null then 0 else hotel_gbv.mtd end) else 0 end as mtd
from
(
select booking_date, value, sum_window_0 as mtd from ${hiveDB}.dashboard_management_kpi_input where brand = 5 and kpi = 'GBV ($)' 
) hotel_gbv 
right outer join 
(
select 
from_unixtime(unix_timestamp(`date`, 'MM/dd/yyyy'), 'yyyyMMdd') as `date`, 
cast(regexp_replace(salesibv_daily_total_SAR, ",", "") as double) * 0.27 as value,
cast(regexp_replace(salesibv_monthly_rolling_total, ",", "") as double) * 0.27 as mtd_value
from googlesheets.almosafer_offline_data 
) all_gbv
ON hotel_gbv.booking_date = all_gbv.`date`

union all 

select hotel_gbv.booking_date as day, 'offline' as business_type, '5' as brand, 'flight' as product, 'IBV ($)' as kpi, 
case when all_gbv.value > 0 then (all_gbv.value - case when hotel_gbv.value is null then 0 else hotel_gbv.value end) else 0 end as value, 
case when all_gbv.mtd_value > 0 then (all_gbv.mtd_value - case when hotel_gbv.mtd is null then 0 else hotel_gbv.mtd end) else 0 end as mtd
from
(
select booking_date, value, sum_window_0 as mtd from ${hiveDB}.dashboard_management_kpi_input where brand = 5 and kpi = 'IBV ($)' 
) hotel_gbv 
right outer join 
(
select 
from_unixtime(unix_timestamp(`date`, 'MM/dd/yyyy'), 'yyyyMMdd') as `date`, 
cast(regexp_replace(salesibv_daily_total_SAR, ",", "") as double) * 0.27 as value,
cast(regexp_replace(salesibv_monthly_rolling_total, ",", "") as double) * 0.27 as mtd_value
from googlesheets.almosafer_offline_data 
) all_gbv
ON hotel_gbv.booking_date = all_gbv.`date`

union all

select hotel_gbv.booking_date as day, 'offline' as business_type, '5' as brand, 'flight' as product, 'CRR ($)' as kpi, 
(all_gbv.value - case when hotel_gbv.value is null then 0 else hotel_gbv.value end) as value, (all_gbv.mtd_value - case when hotel_gbv.mtd is null then 0 else hotel_gbv.mtd end) as mtd
from
(
select booking_date, value, sum_window_0 as mtd from ${hiveDB}.dashboard_management_kpi_input where brand = 5 and kpi = 'CRR ($)' 
) hotel_gbv 
right outer join 
(
select 
from_unixtime(unix_timestamp(`date`, 'MM/dd/yyyy'), 'yyyyMMdd') as `date`, 
cast(cast(regexp_replace(salesibv_daily_total_SAR, ",", "") as double) as double) * 0.27 as value,
cast(cast(regexp_replace(salesibv_monthly_rolling_total, ",", "") as double) as double) * 0.27 as mtd_value
from googlesheets.almosafer_offline_data 
) all_gbv
ON hotel_gbv.booking_date = all_gbv.`date`

union all

select hotel_gbv.booking_date as day, 'offline' as business_type, '5' as brand, 'flight' as product, '# of Transactions' as kpi, 
case when all_gbv.value > 0 then (all_gbv.value - case when hotel_gbv.value is null then 0 else hotel_gbv.value end) else 0 end as value, 
case when all_gbv.mtd_value > 0 then (all_gbv.mtd_value - case when hotel_gbv.mtd is null then 0 else hotel_gbv.mtd end) else 0 end as mtd
from
(
select booking_date, value, sum_window_0 as mtd from ${hiveDB}.dashboard_management_kpi_input where brand = 5 and kpi = '# of Transactions' 
) hotel_gbv 
right outer join 
(
select 
from_unixtime(unix_timestamp(`date`, 'MM/dd/yyyy'), 'yyyyMMdd') as `date`, 
number_of_transactions_daily_total as value,
number_of_transactions_monthly_rolling_total as mtd_value
from googlesheets.almosafer_offline_data 
) all_gbv
ON hotel_gbv.booking_date = all_gbv.`date`

union all

select hotel_gbv.booking_date as day, 'offline' as business_type, '5' as brand, 'flight' as product, 'CPO ($)' as kpi, 
(all_gbv.value - case when hotel_gbv.value is null then 0 else hotel_gbv.value end) as value, (all_gbv.mtd_value - case when hotel_gbv.mtd is null then 0 else hotel_gbv.mtd end) as mtd
from
(
select booking_date, value, sum_window_0 as mtd from ${hiveDB}.dashboard_management_kpi_input where brand = 5 and kpi = 'CPO ($)' 
) hotel_gbv 
right outer join 
(
select 
from_unixtime(unix_timestamp(`date`, 'MM/dd/yyyy'), 'yyyyMMdd') as `date`, 
number_of_transactions_daily_total as value,
number_of_transactions_monthly_rolling_total as mtd_value
from googlesheets.almosafer_offline_data 
) all_gbv
ON hotel_gbv.booking_date = all_gbv.`date`

union all

select hotel_gbv.booking_date as day, 'offline' as business_type, '5' as brand, 'flight' as product, 'Spend ($)' as kpi, 
0 as value, 0 as mtd
from
(
select booking_date, value, sum_window_0 as mtd from ${hiveDB}.dashboard_management_kpi_input where brand = 5 and kpi = 'Spend ($)' 
) hotel_gbv 

) A;

insert into ${hiveDB}.dashboard_management_kpi_input select * from ${hiveDB}.dashboard_management_kpi_input_inter;


