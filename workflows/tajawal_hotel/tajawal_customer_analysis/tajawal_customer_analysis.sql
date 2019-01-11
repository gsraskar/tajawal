--set hiveDB=${hiveDB}.
set hive.mv.files.thread=0;
--Drop table if exists ${hiveDB}.dim_customer_almosafer;
--create table ${hiveDB}.dim_customer_almosafer as select * from ${hiveDB}.dim_customer where first_order_group =2;

--Drop table if exists ${hiveDB}.dim_customer_tajawal;
--create table ${hiveDB}.dim_customer_tajawal as select * from ${hiveDB}.dim_customer where first_order_group =1;


drop table if exists ${hiveDB}.inter2_all_customer_orders;
create table ${hiveDB}.inter2_all_customer_orders
as
select fd.dim_bookingdate_id, dg.group_name, dc.first_product as order_type, ds.type, ds.pos, ds.country, ds.mobile_os as mobile_os_dc, fd.dim_customer_id,fd.id as order_no, ds1.type as type_order, ds1.pos as pos_order, ds1.country as country_order, ds1.mobile_os, dg1.group_name as group_name_order, fd.order_type as order_type_order
, dc.first_order_date
from
--${hiveDB}.fact_flight_orders_final fd 
${hiveDB}.fact_flight_orders_final fd 
--inner join ${hiveDB}.dim_customer dc on fd.dim_customer_id = dc.dim_customer_id
inner join (select * from ${hiveDB}.dim_customer_almosafer union all select * from ${hiveDB}.dim_customer_tajawal) dc on fd.dim_customer_id = dc.dim_customer_id
left outer join ${hiveDB}.dim_store ds on dc.first_store_id = ds.store_id
inner join ${hiveDB}.dim_groups dg on dc.first_order_group = dg.group_id
left outer join ${hiveDB}.dim_store ds1 on fd.dim_store_id = ds1.store_id
inner join ${hiveDB}.dim_groups dg1 on fd.dim_group_id = dg1.group_id
where fd.dim_status_id != 91 
--and fd.dim_status_id != 94 
and fd.dim_group_id != 5
union all
select fd.dim_bookingdate_id, dg.group_name, dc.first_product as order_type, ds.type, ds.pos, ds.country, ds.mobile_os as mobile_os_dc, fd.dim_customer_id, fd.id as order_no, ds1.type as type_order, ds1.pos as pos_order, ds1.country as country_order, ds1.mobile_os, dg1.group_name as group_name_order, fd.order_type as order_type_order
, dc.first_order_date
from
--${hiveDB}.fact_hotel_orders_final fd 
${hiveDB}.fact_hotel_orders_final fd 
--inner join ${hiveDB}.dim_customer dc on fd.dim_customer_id = dc.dim_customer_id
inner join (select * from ${hiveDB}.dim_customer_almosafer union all select * from ${hiveDB}.dim_customer_tajawal) dc on fd.dim_customer_id = dc.dim_customer_id
left outer join ${hiveDB}.dim_store ds on dc.first_store_id = ds.store_id
inner join ${hiveDB}.dim_groups dg on dc.first_order_group = dg.group_id
left outer join ${hiveDB}.dim_store ds1 on fd.dim_store_id = ds1.store_id
inner join ${hiveDB}.dim_groups dg1 on fd.dim_group_id = dg1.group_id
--left outer join ${hiveDB}.dim_store ds on fd.dim_store_id = ds.store_id
--inner join ${hiveDB}.dim_groups dg on fd.dim_group_id = dg.group_id
where fd.dim_status_id != 91 
--and fd.dim_status_id != 94 
and fd.dim_group_id != 5
union all
select fd.dim_bookingdate_id, dg.group_name, dc.first_product as order_type, ds.type, ds.pos, ds.country, ds.mobile_os as mobile_os_dc, fd.dim_customer_id, fd.id as order_no, ds1.type as type_order, ds1.pos as pos_order, ds1.country as country_order, ds1.mobile_os, dg1.group_name as group_name_order, fd.order_type as order_type_order
, dc.first_order_date
from
--${hiveDB}.fact_hotel_orders_final fd 
${hiveDB}.fact_package_orders_final fd 
--inner join ${hiveDB}.dim_customer dc on fd.dim_customer_id = dc.dim_customer_id
inner join (select * from ${hiveDB}.dim_customer_almosafer union all select * from ${hiveDB}.dim_customer_tajawal) dc on fd.dim_customer_id = dc.dim_customer_id
left outer join ${hiveDB}.dim_store ds on dc.first_store_id = ds.store_id
inner join ${hiveDB}.dim_groups dg on dc.first_order_group = dg.group_id
left outer join ${hiveDB}.dim_store ds1 on fd.dim_store_id = ds1.store_id
inner join ${hiveDB}.dim_groups dg1 on fd.dim_group_id = dg1.group_id
--left outer join ${hiveDB}.dim_store ds on fd.dim_store_id = ds.store_id
--inner join ${hiveDB}.dim_groups dg on fd.dim_group_id = dg.group_id
where fd.dim_status_id != 91 
--and fd.dim_status_id != 94 
and fd.dim_group_id != 5

union all

select fd.dim_bookingdate_id, 'all' as group_name, dc.first_product as order_type, ds.type, ds.pos, ds.country, ds.mobile_os as mobile_os_dc, fd.dim_customer_id,fd.id as order_no, ds1.type as type_order, ds1.pos as pos_order, ds1.country as country_order, ds1.mobile_os, 'all' as  group_name_order, fd.order_type as order_type_order
, dc.first_order_date
from
--${hiveDB}.fact_flight_orders_final fd 
${hiveDB}.fact_flight_orders_final fd 
inner join ${hiveDB}.dim_customer dc on fd.dim_customer_id = dc.dim_customer_id
--inner join (select * from ${hiveDB}.dim_customer_almosafer union all select * from ${hiveDB}.dim_customer_tajawal) dc on fd.dim_customer_id = dc.dim_customer_id
left outer join ${hiveDB}.dim_store ds on dc.first_store_id = ds.store_id
inner join ${hiveDB}.dim_groups dg on dc.first_order_group = dg.group_id
left outer join ${hiveDB}.dim_store ds1 on fd.dim_store_id = ds1.store_id
inner join ${hiveDB}.dim_groups dg1 on fd.dim_group_id = dg1.group_id
where fd.dim_status_id != 91 
--and fd.dim_status_id != 94 
and fd.dim_group_id != 5
union all
select fd.dim_bookingdate_id, 'all' as group_name, dc.first_product as order_type, ds.type, ds.pos, ds.country, ds.mobile_os as mobile_os_dc, fd.dim_customer_id, fd.id as order_no, ds1.type as type_order, ds1.pos as pos_order, ds1.country as country_order, ds1.mobile_os, 'all' as group_name_order, fd.order_type as order_type_order
, dc.first_order_date
from
--${hiveDB}.fact_hotel_orders_final fd 
${hiveDB}.fact_hotel_orders_final fd 
inner join ${hiveDB}.dim_customer dc on fd.dim_customer_id = dc.dim_customer_id
--inner join (select * from ${hiveDB}.dim_customer_almosafer union all select * from ${hiveDB}.dim_customer_tajawal) dc on fd.dim_customer_id = dc.dim_customer_id
left outer join ${hiveDB}.dim_store ds on dc.first_store_id = ds.store_id
inner join ${hiveDB}.dim_groups dg on dc.first_order_group = dg.group_id
left outer join ${hiveDB}.dim_store ds1 on fd.dim_store_id = ds1.store_id
inner join ${hiveDB}.dim_groups dg1 on fd.dim_group_id = dg1.group_id
--left outer join ${hiveDB}.dim_store ds on fd.dim_store_id = ds.store_id
--inner join ${hiveDB}.dim_groups dg on fd.dim_group_id = dg.group_id
where fd.dim_status_id != 91 
--and fd.dim_status_id != 94 
and fd.dim_group_id != 5
union all
select fd.dim_bookingdate_id, 'all' as group_name, dc.first_product as order_type, ds.type, ds.pos, ds.country, ds.mobile_os as mobile_os_dc, fd.dim_customer_id, fd.id as order_no, ds1.type as type_order, ds1.pos as pos_order, ds1.country as country_order, ds1.mobile_os, 'all' as group_name_order, fd.order_type as order_type_order
, dc.first_order_date
from
--${hiveDB}.fact_hotel_orders_final fd 
${hiveDB}.fact_package_orders_final fd 
inner join ${hiveDB}.dim_customer dc on fd.dim_customer_id = dc.dim_customer_id
--inner join (select * from ${hiveDB}.dim_customer_almosafer union all select * from ${hiveDB}.dim_customer_tajawal) dc on fd.dim_customer_id = dc.dim_customer_id
left outer join ${hiveDB}.dim_store ds on dc.first_store_id = ds.store_id
inner join ${hiveDB}.dim_groups dg on dc.first_order_group = dg.group_id
left outer join ${hiveDB}.dim_store ds1 on fd.dim_store_id = ds1.store_id
inner join ${hiveDB}.dim_groups dg1 on fd.dim_group_id = dg1.group_id
--left outer join ${hiveDB}.dim_store ds on fd.dim_store_id = ds.store_id
--inner join ${hiveDB}.dim_groups dg on fd.dim_group_id = dg.group_id
where fd.dim_status_id != 91 
--and fd.dim_status_id != 94 
and fd.dim_group_id != 5
;

-- Query 1 : Insert all records
drop table if exists ${hiveDB}.inter_all_customer_orders;
create table ${hiveDB}.inter_all_customer_orders as select * from ${hiveDB}.inter2_all_customer_orders;
-- Query 2 : Insert records where pos and country are 'ALL' 
insert into ${hiveDB}.inter_all_customer_orders select dim_bookingdate_id, group_name, order_type, type, 'all' as pos, 'all' as country, mobile_os_dc, dim_customer_id, order_no, type_order, 'all' as pos_order, 'all' as country_order, mobile_os, group_name_order, order_type_order, first_order_date from ${hiveDB}.inter2_all_customer_orders;
drop table if exists ${hiveDB}.inter2_all_customer_orders;
alter table ${hiveDB}.inter_all_customer_orders rename to ${hiveDB}.inter2_all_customer_orders;
-- Query 3 : Insert records where type is 'ALL'
drop table if exists ${hiveDB}.inter_all_customer_orders;
create table ${hiveDB}.inter_all_customer_orders as select * from ${hiveDB}.inter2_all_customer_orders;
insert into ${hiveDB}.inter_all_customer_orders select dim_bookingdate_id, group_name, order_type, 'all' as type, pos, country, mobile_os_dc, dim_customer_id, order_no, 'all' as type_order, pos_order, country_order, mobile_os, group_name_order, order_type_order, first_order_date from ${hiveDB}.inter2_all_customer_orders;
drop table if exists ${hiveDB}.inter2_all_customer_orders;
alter table ${hiveDB}.inter_all_customer_orders rename to ${hiveDB}.inter2_all_customer_orders;
-- Query 3.1 : Insert records where mobile_os is 'ALL'
drop table if exists ${hiveDB}.inter_all_customer_orders;
create table ${hiveDB}.inter_all_customer_orders as select * from ${hiveDB}.inter2_all_customer_orders;
insert into ${hiveDB}.inter_all_customer_orders select dim_bookingdate_id, group_name, order_type, type, pos, country, 'all' as mobile_os_dc, dim_customer_id, order_no, type_order, pos_order, country_order, 'all' as mobile_os, group_name_order, order_type_order, first_order_date from ${hiveDB}.inter2_all_customer_orders;
drop table if exists ${hiveDB}.inter2_all_customer_orders;
alter table ${hiveDB}.inter_all_customer_orders rename to ${hiveDB}.inter2_all_customer_orders;
-- Query 4 : Insert records where order_type is 'ALL'
-- Query 4 : Insert records where order_type is 'ALL'
drop table if exists ${hiveDB}.inter_all_customer_orders;
create table ${hiveDB}.inter_all_customer_orders as select * from ${hiveDB}.inter2_all_customer_orders;
insert into ${hiveDB}.inter_all_customer_orders select dim_bookingdate_id, group_name, 'all' as order_type, type, pos, country, mobile_os_dc, dim_customer_id, order_no, type_order, pos_order, country_order, mobile_os, group_name_order, 'all' as order_type_order, first_order_date from ${hiveDB}.inter2_all_customer_orders;
drop table if exists ${hiveDB}.inter2_all_customer_orders;
alter table ${hiveDB}.inter_all_customer_orders rename to ${hiveDB}.inter2_all_customer_orders;
-- Query 5 : Insert records where group_name is 'ALL'
--drop table if exists ${hiveDB}.inter_all_customer_orders;
--create table ${hiveDB}.inter_all_customer_orders as select * from ${hiveDB}.inter2_all_customer_orders;
--insert into ${hiveDB}.inter_all_customer_orders select dim_bookingdate_id, 'all' as group_name, order_type, type, pos, country, mobile_os_dc, dim_customer_id, order_no, type_order, pos_order, country_order, mobile_os, 'all' as group_name_order, order_type_order, first_order_date from ${hiveDB}.inter2_all_customer_orders;
--drop table if exists ${hiveDB}.inter2_all_customer_orders;
--alter table ${hiveDB}.inter_all_customer_orders rename to ${hiveDB}.inter2_all_customer_orders;

-- Final Query : To calculate result at pre aggregated levels
drop table if exists ${hiveDB}.tajawal_new_customer_analysis_intermediate;
--create table ${hiveDB}.tajawal_new_customer_analysis_intermediate row format delimited fields terminated by '\t' as
create table ${hiveDB}.tajawal_new_customer_analysis_intermediate as
select year_number, month_name, group_name_order, order_type_order, type_order, pos_order, country, mobile_os, (existing_customers + new_customers) as total_customers, (existing_bookings + new_customer_bookings) as total_bookings,
new_customers, new_customer_bookings
from (
select dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, pos_order, pos_order as country, mobile_os,
count(distinct(case when group_name_order = group_name and dd.mmyyyy != dc.mmyyyy then a.dim_customer_id  else null end)) existing_customers,
count(distinct(case when group_name_order = group_name and dd.mmyyyy != dc.mmyyyy then order_no else null end)) as existing_bookings,
count(distinct(case when dd.mmyyyy = dc.mmyyyy and group_name_order = group_name and order_type_order = order_type and type = type_order and mobile_os_dc = mobile_os and pos_order = pos then a.dim_customer_id else null end)) new_customers,
count(distinct(case when dd.mmyyyy = dc.mmyyyy and group_name_order = group_name and order_type_order = order_type and type = type_order and mobile_os_dc = mobile_os and pos_order = pos then order_no else null end)) new_customer_bookings
from ${hiveDB}.inter2_all_customer_orders a
inner join ${hiveDB}.dim_date dd on a.dim_bookingdate_id = dd.dim_date_id
--inner join ${hiveDB}.dim_customer ddc on a.dim_customer_id = ddc.dim_customer_id
inner join ${hiveDB}.dim_date dc on a.first_order_date = dc.dim_date_id
group by dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order) A;
drop table if exists ${hiveDB}.report_tajawal_new_customer_analysis;
alter table ${hiveDB}.tajawal_new_customer_analysis_intermediate rename to ${hiveDB}.report_tajawal_new_customer_analysis;

-- Final Query : To calculate cohort data result
drop table if exists ${hiveDB}.tajawal_cohort_customer_analysis_intermediate;
--create table ${hiveDB}.tajawal_cohort_customer_analysis_intermediate row format delimited fields terminated by '\t' as
create table ${hiveDB}.tajawal_cohort_customer_analysis_intermediate as
select dc.year_number as first_order_year, dc.month_name as first_month_name, dd.year_number as transaction_year, dd.month_name as transaction_month, group_name_order, order_type_order, type_order, mobile_os, pos_order, pos_order as country,
count(distinct(CASE WHEN group_name = group_name_order then a.dim_customer_id else null end)) as customer_count, count(distinct(CASE WHEN group_name = group_name_order then order_no else null end)) as total_bookings
from ${hiveDB}.inter2_all_customer_orders a
inner join ${hiveDB}.dim_date dd on a.dim_bookingdate_id = dd.dim_date_id
--inner join ${hiveDB}.dim_customer ddc on a.dim_customer_id = ddc.dim_customer_id
inner join ${hiveDB}.dim_date dc on a.first_order_date = dc.dim_date_id
group by dc.year_number, dc.month_name, dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order;
drop table if exists ${hiveDB}.tajawal_cohort_customer_analysis_intermediate_1;
create table ${hiveDB}.tajawal_cohort_customer_analysis_intermediate_1 as select * from ${hiveDB}.tajawal_cohort_customer_analysis_intermediate where total_bookings != 0;
drop table if exists ${hiveDB}.report_tajawal_cohort_customer_analysis;
alter table ${hiveDB}.tajawal_cohort_customer_analysis_intermediate_1 rename to ${hiveDB}.report_tajawal_cohort_customer_analysis;

-- Final Query : To calculate 
drop table if exists ${hiveDB}.tajawal_brandwise_existing_customer_analysis_intermediate;
--create table ${hiveDB}.tajawal_brandwise_existing_customer_analysis_intermediate row format delimited fields terminated by '\t' as
create table ${hiveDB}.tajawal_brandwise_existing_customer_analysis_intermediate as
select year_number, month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, country, first_brand, first_product, (new_customers + existing_customers) as total_customers, existing_customers  from (
-- Case to handle existing customers for same brand
select dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, pos_order as country,
--ddc.first_order_group as first_brand, ddc.first_product as first_product,
case when lower(group_name) = 'tajawal' then '1' else case when lower(group_name) = 'almosafer' then '2' else null end end as first_brand, order_type as first_product,
count(distinct(a.dim_customer_id)) total_customers,
count(distinct(case when dd.mmyyyy = dc.mmyyyy and group_name_order = group_name and order_type_order = order_type and type = type_order and mobile_os_dc = mobile_os and pos_order = pos then a.dim_customer_id else null end)) new_customers,
count(distinct(case when dd.mmyyyy != dc.mmyyyy and group_name_order = group_name then a.dim_customer_id else null end)) existing_customers
from ${hiveDB}.inter2_all_customer_orders a
inner join ${hiveDB}.dim_date dd on a.dim_bookingdate_id = dd.dim_date_id
--inner join ${hiveDB}.dim_customer ddc on a.dim_customer_id = ddc.dim_customer_id
inner join ${hiveDB}.dim_date dc on a.first_order_date = dc.dim_date_id
where group_name_order = group_name 
--group by dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, ddc.first_order_group, ddc.first_product
group by dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, group_name, order_type

union all
-- Case to handle existing customers cross brand
select dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, pos_order as country,
--ddc.first_order_group as first_brand, ddc.first_product as first_product,
case when lower(group_name) = 'tajawal' then '1' else case when lower(group_name) = 'almosafer' then '2' else null end end as first_brand, order_type as first_product,
count(distinct(a.dim_customer_id)) total_customers,
count(distinct(case when dd.mmyyyy = dc.mmyyyy and group_name_order = group_name and order_type_order = order_type and type = type_order and mobile_os_dc = mobile_os and pos_order = pos then a.dim_customer_id else null end)) new_customers,
count(distinct(case when dd.mmyyyy != dc.mmyyyy and group_name_order != group_name then a.dim_customer_id else null end)) existing_customers
from ${hiveDB}.inter2_all_customer_orders a
inner join ${hiveDB}.dim_date dd on a.dim_bookingdate_id = dd.dim_date_id
--inner join ${hiveDB}.dim_customer ddc on a.dim_customer_id = ddc.dim_customer_id
inner join ${hiveDB}.dim_date dc on a.first_order_date = dc.dim_date_id
where group_name_order != group_name 
--group by dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, ddc.first_order_group, ddc.first_product
group by dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, group_name, order_type

union all

select dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, a.country_order,
'all' as first_brand, 'all' as first_product,
count(distinct(a.dim_customer_id)) total_customers,
count(distinct(case when dd.mmyyyy = dc.mmyyyy and group_name_order = group_name and order_type_order = order_type and type = type_order and mobile_os_dc = mobile_os and pos_order = pos then a.dim_customer_id else null end)) new_customers,
count(distinct(case when dd.mmyyyy != dc.mmyyyy and group_name_order = group_name then a.dim_customer_id else null end)) existing_customers
from ${hiveDB}.inter2_all_customer_orders a
inner join ${hiveDB}.dim_date dd on a.dim_bookingdate_id = dd.dim_date_id
--inner join ${hiveDB}.dim_customer ddc on a.dim_customer_id = ddc.dim_customer_id
inner join ${hiveDB}.dim_date dc on a.first_order_date = dc.dim_date_id
group by dd.year_number, dd.month_name, group_name_order, order_type_order, type_order, mobile_os, pos_order, a.country_order
) Z 
;
drop table if exists ${hiveDB}.report_tajawal_brandwise_existing_customer_analysis;
alter table ${hiveDB}.tajawal_brandwise_existing_customer_analysis_intermediate rename to ${hiveDB}.report_tajawal_brandwise_existing_customer_analysis;


-- ================================
-- ======== Queries for dim_promo_id
-- ================================
set hive.mv.files.thread=0;

DROP TABLE IF EXISTS ${hiveDB}.intermediate_promo_stats;
CREATE TABLE IF NOT EXISTS ${hiveDB}.intermediate_promo_stats as -- row format delimited fields terminated by '\t' as
select T1.dim_bookingdate_id, T1.dim_bookingpaid_id, dd.month_name, dd.year_number, T1.dim_group_id, T1.order_type,
ds.type, ds.pos, ds.country, ds.mobile_os, T1.dim_promo_id,
T1.order_no, T1.first_order_date, T1.first_promo_id, df.month_name as first_month_name, df.year_number as first_year_number, T1.customer_id, T1.discount_amount_usd, T1.discount_amount_ibv,
T1.discount_gbv -- , T3.total_ibv
FROM
(
    select T.dim_bookingdate_id, coalesce(T2.dim_bookingpaid_id, T.dim_bookingpaid_id) as dim_bookingpaid_id, T.dim_group_id, T.order_type, T.dim_store_id, T.dim_promo_id, T.order_no,
    T.first_order_date, T.first_promo_id, T.customer_id, T.discount_amount_usd, T.discount_amount_ibv, T2.discount_gbv from
    (
        -- Query 1 : Calculate fields 1
        SELECT fd.dim_bookingdate_id, fd.dim_bookingpaid_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, fd.dim_promo_id,
        fd.order_no AS order_no, 
        ddc.first_order_date, ddc.first_promo_id,
        fd.dim_customer_id AS customer_id,
        SUM(-1 * fd.discount_amount_usd) AS discount_amount_usd,
        SUM(fd.payment_amount_usd) AS discount_amount_ibv
        FROM
        (
        SELECT dim_bookingdate_id, dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, ibv as payment_amount_usd
        FROM ${hiveDB}.fact_hotel_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
        union all
        SELECT dim_bookingdate_id, dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, ibv as payment_amount_usd
        FROM ${hiveDB}.fact_flight_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
        union all
        SELECT dim_bookingdate_id, dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, iov as payment_amount_usd
        FROM ${hiveDB}.fact_package_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
        ) fd
        INNER JOIN (
        select dim_customer_id, first_order_group, first_order_date, first_promo_id from ${hiveDB}.dim_customer_almosafer
        union all
        select dim_customer_id, first_order_group, first_order_date, first_promo_id from ${hiveDB}.dim_customer_tajawal 
        ) ddc ON fd.dim_customer_id = ddc.dim_customer_id and fd.dim_group_id = ddc.first_order_group
        GROUP BY fd.dim_bookingdate_id, fd.dim_bookingpaid_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, 
        fd.dim_promo_id, fd.order_no, fd.dim_customer_id, ddc.first_order_date, ddc.first_promo_id
    ) T
    left outer join
    (
        -- Query 2 : Calculate fields 2
        SELECT fd.dim_bookingpaid_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, fd.dim_promo_id,
        fd.order_no AS order_no,
        fd.dim_customer_id AS customer_id,
        sum(payment_amount_usd) as discount_gbv
        FROM
        (
        SELECT dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, gbv as payment_amount_usd
        FROM ${hiveDB}.fact_hotel_orders_final
        WHERE dim_status_id != 91 and dim_group_id != 5 and dim_status_id != 94 and dim_state_id != 'Fraud'
        union all
        SELECT dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, gbv as payment_amount_usd
        FROM ${hiveDB}.fact_flight_orders_final 
        WHERE dim_status_id != 91 and dim_group_id != 5 and dim_status_id != 94 and dim_state_id != 'Fraud'
        union all
        SELECT dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, gbv as payment_amount_usd
        FROM ${hiveDB}.fact_package_orders_final 
        WHERE dim_status_id != 91 and dim_group_id != 5 and dim_status_id != 94
        ) fd
        GROUP BY fd.dim_bookingpaid_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, fd.dim_promo_id,
        fd.order_no, fd.dim_customer_id
    ) T2 on T.dim_bookingdate_id = T2.dim_bookingpaid_id and T.dim_group_id = T2.dim_group_id and T.order_type = T2.order_type
    and T.dim_store_id = T2.dim_store_id and T.dim_promo_id = T2.dim_promo_id and T.order_no = T2.order_no
    and T.customer_id = T2.customer_id

    union

    select T.dim_bookingdate_id, T2.dim_bookingpaid_id, T2.dim_group_id, T2.order_type, T2.dim_store_id, T2.dim_promo_id, T.order_no,
    T.first_order_date, T.first_promo_id, T.customer_id, T.discount_amount_usd, T.discount_amount_ibv, T2.discount_gbv from
    (
        -- Query 1 : Calculate fields 1
        SELECT fd.dim_bookingdate_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, fd.dim_promo_id,
        fd.order_no AS order_no, 
        ddc.first_order_date, ddc.first_promo_id,
        fd.dim_customer_id AS customer_id,
        SUM(-1 * fd.discount_amount_usd) AS discount_amount_usd,
        SUM(fd.payment_amount_usd) AS discount_amount_ibv
        FROM
        (
        SELECT dim_bookingdate_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, ibv as payment_amount_usd
        FROM ${hiveDB}.fact_hotel_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
        union all
        SELECT dim_bookingdate_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, ibv as payment_amount_usd
        FROM ${hiveDB}.fact_flight_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
        union all
        SELECT dim_bookingdate_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, iov as payment_amount_usd
        FROM ${hiveDB}.fact_package_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
        ) fd
        INNER JOIN (
        select dim_customer_id, first_order_group, first_order_date, first_promo_id from ${hiveDB}.dim_customer_almosafer
        union all
        select dim_customer_id, first_order_group, first_order_date, first_promo_id from ${hiveDB}.dim_customer_tajawal 
        ) ddc ON fd.dim_customer_id = ddc.dim_customer_id and fd.dim_group_id = ddc.first_order_group
        GROUP BY fd.dim_bookingdate_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, 
        fd.dim_promo_id, fd.order_no, fd.dim_customer_id, ddc.first_order_date, ddc.first_promo_id
    ) T
    right outer join
    (
        -- Query 2 : Calculate fields 2
        SELECT fd.dim_bookingpaid_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, fd.dim_promo_id,
        fd.order_no AS order_no,
        fd.dim_customer_id AS customer_id,
        sum(payment_amount_usd) as discount_gbv
        FROM
        (
        SELECT dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, gbv as payment_amount_usd
        FROM ${hiveDB}.fact_hotel_orders_final 
        WHERE dim_status_id != 91 and dim_group_id != 5 and dim_status_id != 94 and dim_state_id != 'Fraud'
        union all
        SELECT dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, gbv as payment_amount_usd
        FROM ${hiveDB}.fact_flight_orders_final 
        WHERE dim_status_id != 91 and dim_group_id != 5 and dim_status_id != 94 and dim_state_id != 'Fraud'
        union all
        SELECT dim_bookingpaid_id, dim_group_id, order_type, dim_store_id, dim_promo_id,
        dim_customer_id, id as order_no, -1 * discount_amount_usd as discount_amount_usd, gbv as payment_amount_usd
        FROM ${hiveDB}.fact_package_orders_final 
        WHERE dim_status_id != 91 and dim_group_id != 5 and dim_status_id != 94 
        ) fd
        GROUP BY fd.dim_bookingpaid_id, fd.dim_group_id, fd.order_type, fd.dim_store_id, fd.dim_promo_id,
        fd.order_no, fd.dim_customer_id
    ) T2 on T.dim_bookingdate_id = T2.dim_bookingpaid_id and T.dim_group_id = T2.dim_group_id and T.order_type = T2.order_type
    and T.dim_store_id = T2.dim_store_id and T.dim_promo_id = T2.dim_promo_id and T.order_no = T2.order_no
    and T.customer_id = T2.customer_id
    where T.dim_bookingdate_id is null
) T1
inner join ${hiveDB}.dim_date dd on COALESCE(T1.dim_bookingdate_id, T1.dim_bookingpaid_id) = dd.dim_date_id 
left outer join ${hiveDB}.dim_date df on T1.first_order_date = df.dim_date_id
inner join ${hiveDB}.dim_groups dg on T1.dim_group_id = dg.group_id 
inner join ${hiveDB}.dim_store ds on T1.dim_store_id = ds.store_id;

DROP TABLE IF EXISTS ${hiveDB}.intermediate_promo_total_stats;
CREATE TABLE IF NOT EXISTS ${hiveDB}.intermediate_promo_total_stats as -- row format delimited fields terminated by '\t' as
select A.dim_bookingdate_id, dd.month_name, dd.year_number, A.dim_group_id, dg.group_name,
A.order_type, ds.type, ds.pos, ds.mobile_os, ds.country, A.total_orders, A.total_customers, A.total_ibv
FROM
(
SELECT fd.dim_bookingdate_id, fd.dim_group_id, fd.order_type, fd.dim_store_id,
    count(distinct(order_no)) as total_orders, count(distinct(fd.dim_customer_id)) AS total_customers,
    sum(payment_amount_usd) as total_ibv
    FROM
    (
    SELECT dim_bookingdate_id, dim_group_id, order_type, dim_store_id, dim_promo_id, dim_status_id, dim_state_id,
    dim_customer_id, order_no, ibv as payment_amount_usd
    FROM ${hiveDB}.fact_flight_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
    union all
    SELECT dim_bookingdate_id, dim_group_id, order_type, dim_store_id, dim_promo_id, dim_status_id, dim_state_id,
    dim_customer_id, order_no, ibv as payment_amount_usd
    FROM ${hiveDB}.fact_hotel_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
    union all
    SELECT dim_bookingdate_id, dim_group_id, order_type, dim_store_id, dim_promo_id, dim_status_id, 'null' as dim_state_id,
    dim_customer_id, order_no, iov as payment_amount_usd
    FROM ${hiveDB}.fact_package_orders_final WHERE dim_status_id != 91 and dim_group_id != 5
    ) fd
    group by fd.dim_bookingdate_id, fd.dim_group_id, fd.order_type, fd.dim_store_id
) A inner join ${hiveDB}.dim_store ds on A.dim_store_id = ds.store_id
inner join ${hiveDB}.dim_groups dg on A.dim_group_id = dg.group_id
inner join ${hiveDB}.dim_date dd on A.dim_bookingdate_id = dd.dim_date_id;

-- ============ PRE CALCULATION FOR 'ALL' COMBINATIONS ============== --
-- ==== STEP 1 : PRE CALCULATE CUSTOMER COUNT AT ALL POSSIBLE LEVELS

drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 1 : All
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats
as select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
order_no, first_order_date, first_promo_id, customer_id, discount_amount_usd, discount_amount_ibv, discount_gbv
from ${hiveDB}.intermediate_promo_stats;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 2 : mobile_os 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, pos, 'all' as mobile_os,
order_no, first_order_date, first_promo_id, customer_id, discount_amount_usd, discount_amount_ibv, discount_gbv
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 3 : pos 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, 'all' pos, mobile_os,
order_no, first_order_date, first_promo_id, customer_id, discount_amount_usd, discount_amount_ibv, discount_gbv
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 4 : type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, 'all' type, pos, mobile_os,
order_no, first_order_date, first_promo_id, customer_id, discount_amount_usd, discount_amount_ibv, discount_gbv
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 5 : order_type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, 'all' order_type, type, pos, mobile_os,
order_no, first_order_date, first_promo_id, customer_id, discount_amount_usd, discount_amount_ibv, discount_gbv
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 6 : order_type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, 'all' dim_group_id, order_type, type, pos, mobile_os,
order_no, first_order_date, first_promo_id, customer_id, discount_amount_usd, discount_amount_ibv, discount_gbv
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 6 : order_type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, 'all' as month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
order_no, first_order_date, first_promo_id, customer_id, discount_amount_usd, discount_amount_ibv, discount_gbv
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;

-- Consolidated table to host pre aggregated customer counts
drop table if exists ${hiveDB}.report_dim_promo_performance_inter1;
create table if not exists ${hiveDB}.report_dim_promo_performance_inter1 as
select dim_promo_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
-- count(distinct(case when substr(dim_bookingdate_id, 1, 6) = substr(first_order_date, 1, 6) then customer_id else null end)) as discount_new_customers,
count(distinct(case when dim_bookingdate_id = first_order_date and dim_promo_id = first_promo_id then customer_id else null end)) as discount_new_customers,
count(distinct(customer_id)) as discount_total_customers
from ${hiveDB}.intermediate_precal_promo_customer_stats
group by dim_promo_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os;

-- === STEP 2 : PRE CALCULATE ALL OTHER METRICES TOGETHER (other than customer counts) === --
DROP TABLE IF EXISTS ${hiveDB}.intermediate_promo_stats_1_1;
CREATE TABLE IF NOT EXISTS ${hiveDB}.intermediate_promo_stats_1_1 as
select A.dim_promo_id, A.dim_bookingdate_id, A.dim_bookingpaid_id, A.month_name, A.year_number, A.dim_group_id, A.order_type, A.type,
A.pos, A.mobile_os, A.discount_orders, A.discount_amount_usd, A.discount_amount_ibv, A.discount_gbv, B.total_orders, B.total_ibv,
A.date_start, A.date_end
FROM
(
    select dim_promo_id, dim_bookingdate_id, dim_bookingpaid_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
    count(distinct(order_no)) as discount_orders, sum(discount_amount_usd) as discount_amount_usd,
    sum(discount_amount_ibv) as discount_amount_ibv, sum(discount_gbv) as discount_gbv,
    min(dim_bookingdate_id) as date_start, max(dim_bookingdate_id) as date_end
    from ${hiveDB}.intermediate_promo_stats
    group by dim_promo_id, dim_bookingdate_id, dim_bookingpaid_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os 
) A
left outer join
(
    select dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
    sum(total_orders) as total_orders, sum(total_ibv) as total_ibv
    from ${hiveDB}.intermediate_promo_total_stats
    group by dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os
) B  ON A.dim_bookingdate_id = B.dim_bookingdate_id and A.month_name = B.month_name and A.year_number = B.year_number
and A.dim_group_id = B.dim_group_id and A.order_type = B.order_type and A.type = B.type and A.pos = B.pos
and A.mobile_os = B.mobile_os;

DROP TABLE IF EXISTS ${hiveDB}.intermediate_promo_stats_1;
CREATE TABLE IF NOT EXISTS ${hiveDB}.intermediate_promo_stats_1 as
SELECT 
A.dim_promo_id, COALESCE(A.dim_bookingdate_id, A.dim_bookingpaid_id) as dim_bookingdate_id,
A.month_name, A.year_number, A.dim_group_id, A.order_type, A.type,
A.pos, A.mobile_os, 
sum(A.discount_orders) as discount_orders, sum(A.discount_amount_usd) as discount_amount_usd, sum(A.discount_amount_ibv) as discount_amount_ibv, 
sum(A.discount_gbv) as discount_gbv, max(A.total_orders) as total_orders, max(A.total_ibv) as total_ibv, min(A.date_start) as date_start, 
max(A.date_end) as date_end
FROM
${hiveDB}.intermediate_promo_stats_1_1 A
group by 
A.dim_promo_id, COALESCE(A.dim_bookingdate_id, A.dim_bookingpaid_id),
A.month_name, A.year_number, A.dim_group_id, A.order_type, A.type,
A.pos, A.mobile_os;

-- Create pre-aggr data for consolidated table
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 1 : All
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats
as select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
discount_orders, discount_amount_usd, discount_amount_ibv, discount_gbv, total_orders, total_ibv, date_start, date_end
from ${hiveDB}.intermediate_promo_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 2 : mobile_os 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, pos, 'all' as mobile_os,
discount_orders, discount_amount_usd, discount_amount_ibv, discount_gbv, total_orders, total_ibv, date_start, date_end
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 3 : pos 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, type, 'all' pos, mobile_os,
discount_orders, discount_amount_usd, discount_amount_ibv, discount_gbv, total_orders, total_ibv, date_start, date_end
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 4 : type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, order_type, 'all' type, pos, mobile_os,
discount_orders, discount_amount_usd, discount_amount_ibv, discount_gbv, total_orders, total_ibv, date_start, date_end
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 5 : order_type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, dim_group_id, 'all' order_type, type, pos, mobile_os,
discount_orders, discount_amount_usd, discount_amount_ibv, discount_gbv, total_orders, total_ibv, date_start, date_end
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 6 : order_type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, month_name, year_number, 'all' dim_group_id, order_type, type, pos, mobile_os,
discount_orders, discount_amount_usd, discount_amount_ibv, discount_gbv, total_orders, total_ibv, date_start, date_end
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
alter table ${hiveDB}.intermediate_precal_promo_customer_stats rename to ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- CASE 6 : order_type 'all'
create table if not exists ${hiveDB}.intermediate_precal_promo_customer_stats as select * from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
insert into ${hiveDB}.intermediate_precal_promo_customer_stats
select 
dim_promo_id, dim_bookingdate_id, 'all' month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
discount_orders, discount_amount_usd, discount_amount_ibv, discount_gbv, total_orders, total_ibv, date_start, date_end
from ${hiveDB}.intermediate_precal_promo_customer_stats_1;
drop table if exists ${hiveDB}.intermediate_precal_promo_customer_stats_1;
-- Consolidated table to host all metrices other than customer count
drop table if exists ${hiveDB}.report_dim_promo_performance_inter2;
create table if not exists ${hiveDB}.report_dim_promo_performance_inter2 as
select dim_promo_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os,
sum(discount_orders) as discount_orders,
sum(discount_amount_usd) as discount_amount_usd,
sum(discount_amount_ibv) as discount_amount_ibv,
sum(discount_gbv) as discount_gbv,
sum(total_orders) as total_orders,
sum(total_ibv) as total_ibv,
min(date_start) as date_start,
max(date_end) as date_end
from ${hiveDB}.intermediate_precal_promo_customer_stats
group by dim_promo_id, month_name, year_number, dim_group_id, order_type, type, pos, mobile_os;

-- === STEP 3 : CREATE FINAL PRE-AGGR TABLE === --
drop table if exists ${hiveDB}.report_dim_promo_performance;
create table if not exists ${hiveDB}.report_dim_promo_performance as
select A.dim_promo_id, A.month_name, A.year_number, A.dim_group_id, A.order_type, A.type, A.pos, A.mobile_os,
A.discount_orders, A.total_orders, B.discount_new_customers, B.discount_total_customers, A.discount_amount_usd,
A.discount_amount_ibv, A.total_ibv, A.discount_gbv, A.date_start, A.date_end
from 
${hiveDB}.report_dim_promo_performance_inter1 B inner join 
${hiveDB}.report_dim_promo_performance_inter2 A
on A.dim_promo_id = B.dim_promo_id and A.month_name = B.month_name and A.year_number = B.year_number 
and A.dim_group_id = B.dim_group_id
and A.order_type = B.order_type and A.type = B.type and A.pos = B.pos and A.mobile_os = B.mobile_os;

drop table if exists ${hiveDB}.report_promo_daywise_details;
create table if not exists ${hiveDB}.report_promo_daywise_details as -- row format delimited fields terminated by '\t' as
SELECT
report_promo_stats_detailed.dim_promo_id  AS PromoCode,
report_promo_stats_detailed.first_promo_id as first_promo_id,
report_promo_stats_detailed.`month_name` AS MONTH,
dd.month_number AS Month_num,
report_promo_stats_detailed.`year_number` AS  YEAR,
dg.group_name AS Brand,
report_promo_stats_detailed.`order_type` AS Product,
report_promo_stats_detailed.`type` AS Device,
report_promo_stats_detailed.pos AS POS,
report_promo_stats_detailed.mobile_os,
from_unixtime(unix_timestamp(report_promo_stats_detailed.dim_bookingdate_id, "yyyyMMdd" ), 'dd-MM-yyyy') AS dim_bookingdate_id,
from_unixtime(unix_timestamp(report_promo_stats_detailed.dim_bookingpaid_id, "yyyyMMdd" ), 'dd-MM-yyyy') AS dim_bookingpaid_id,
from_unixtime(unix_timestamp(first_order_date, "yyyyMMdd" ), 'dd-MM-yyyy') AS first_order_date,
order_no AS  discount_bookings,
customer_id AS discount_total_customers,
SUM(discount_amount_usd) AS discount_amount_usd,
SUM(discount_amount_ibv) AS payment_amount_usd,
SUM(discount_gbv) AS gbv,
MIN(report_promo_stats_detailed.dim_bookingdate_id) AS start_date,
MAX(report_promo_stats_detailed.dim_bookingdate_id) AS end_date,
(total_ibv) / SUM(B.records_count) AS total_ibv,
(total_bookings) / SUM(B.records_count) AS total_bookings
FROM 
${hiveDB}.intermediate_promo_stats as report_promo_stats_detailed
INNER JOIN ${hiveDB}.`dim_groups` AS dg ON dg.`group_id`=report_promo_stats_detailed.dim_group_id
INNER JOIN ${hiveDB}.`dim_date` AS dd ON dd.`dim_date_id`= coalesce(report_promo_stats_detailed.dim_bookingdate_id, report_promo_stats_detailed.dim_bookingpaid_id)
LEFT JOIN
(
SELECT 
report_promo_total_stats.month_name AS MONTH,
dd.month_number AS Month_num,
report_promo_total_stats.year_number AS  YEAR,
dg.group_name AS Brand,
`order_type` AS Product,
`type` AS Device,
`pos` AS POS,
`mobile_os`,
dim_bookingdate_id,
SUM(total_ibv)  AS total_ibv,
SUM(`total_orders`) AS total_bookings
FROM ${hiveDB}.intermediate_promo_total_stats as report_promo_total_stats
INNER JOIN ${hiveDB}.`dim_groups` AS dg ON dg.`group_id`=report_promo_total_stats.dim_group_id
INNER JOIN ${hiveDB}.`dim_date` AS dd ON dd.`dim_date_id`=report_promo_total_stats.dim_bookingdate_id
GROUP BY report_promo_total_stats.month_name,report_promo_total_stats.year_number,dd.month_number ,dg.group_name,
`order_type`,`type`,`pos`,`mobile_os`,report_promo_total_stats.dim_bookingdate_id
) AS Pramocode_IBV
ON  Pramocode_IBV.Month=report_promo_stats_detailed.`month_name`
AND Pramocode_IBV.YEAR=report_promo_stats_detailed.`year_number`
AND Pramocode_IBV.Brand=dg.group_name
AND Pramocode_IBV.Product=report_promo_stats_detailed.order_type
AND Pramocode_IBV.Device=report_promo_stats_detailed.type
AND Pramocode_IBV.mobile_os=report_promo_stats_detailed.mobile_os
AND Pramocode_IBV.pos=report_promo_stats_detailed.pos
AND Pramocode_IBV.dim_bookingdate_id=coalesce(report_promo_stats_detailed.dim_bookingdate_id, report_promo_stats_detailed.dim_bookingpaid_id)
LEFT OUTER JOIN (
SELECT  report_promo_stats_detailed.dim_promo_id,
  report_promo_stats_detailed.month_name,
  report_promo_stats_detailed.year_number,
  report_promo_stats_detailed.dim_group_id,
  report_promo_stats_detailed.order_type,
  report_promo_stats_detailed.type,
  report_promo_stats_detailed.pos,
  report_promo_stats_detailed.mobile_os,
  report_promo_stats_detailed.dim_bookingdate_id,
  COUNT(*) AS records_count
  FROM ${hiveDB}.intermediate_promo_stats as report_promo_stats_detailed
  INNER JOIN ${hiveDB}.`dim_groups` AS dg ON dg.`group_id`=report_promo_stats_detailed.dim_group_id
  INNER JOIN ${hiveDB}.`dim_date` AS dd ON dd.`dim_date_id`=coalesce(report_promo_stats_detailed.dim_bookingdate_id, report_promo_stats_detailed.dim_bookingpaid_id)

  GROUP BY report_promo_stats_detailed.dim_promo_id,
  report_promo_stats_detailed.month_name,
  report_promo_stats_detailed.year_number,
  report_promo_stats_detailed.dim_group_id,
  report_promo_stats_detailed.order_type,
  report_promo_stats_detailed.type,
  report_promo_stats_detailed.pos,
  report_promo_stats_detailed.mobile_os,
  report_promo_stats_detailed.dim_bookingdate_id
) B ON
report_promo_stats_detailed.dim_promo_id = B.dim_promo_id AND report_promo_stats_detailed.month_name = B.month_name
AND report_promo_stats_detailed.year_number = B.year_number AND report_promo_stats_detailed.dim_group_id = B.dim_group_id
AND report_promo_stats_detailed.order_type = B.order_type AND report_promo_stats_detailed.type = B.type
AND report_promo_stats_detailed.pos = B.pos AND report_promo_stats_detailed.mobile_os = B.mobile_os
AND report_promo_stats_detailed.dim_bookingdate_id = B.dim_bookingdate_id
GROUP BY report_promo_stats_detailed.dim_promo_id, report_promo_stats_detailed.first_promo_id, report_promo_stats_detailed.`month_name`,report_promo_stats_detailed.`year_number`,
dg.group_name,report_promo_stats_detailed.`order_type`,report_promo_stats_detailed.`type`,report_promo_stats_detailed.`pos`,report_promo_stats_detailed.mobile_os,dd.month_number,
order_no, customer_id, report_promo_stats_detailed.dim_bookingdate_id, report_promo_stats_detailed.dim_bookingpaid_id, first_order_date,total_ibv,total_bookings;

-- ==== Pre aggregated table for daily marketing performance report udpate
DROP TABLE IF EXISTS tajawal_bi.inter_marketing_performance_daily;
CREATE TABLE IF NOT EXISTS tajawal_bi.inter_marketing_performance_daily row format delimited fields terminated by "\t" AS
-- SELECT  
-- A.MONTH, A.YEAR, A.Brand,A.Product,A.Device,A.POS,A.IBV,A.Booking,A.Month_num,
-- fsc.spend AS Spend, 
-- A.dim_bookingdate_id AS dim_bookingdate_id,
-- dt.IBV_target  AS ibv_target ,
-- dt.`bookings_target` AS booking_target,
-- dt.`target_spend` AS spend_target,
-- dt.`CPO_target` AS CPO_target,
-- dt.`CRR_target` AS CRR_target
-- FROM  
-- (
--   SELECT  A.month_name AS MONTH,
--   A.`month_number` AS Month_num,
--   A.year_number AS YEAR,
--   A.dim_bookingdate_id AS dim_bookingdate_id,
--   gr.group_name AS Brand ,
--   A.product_type AS Product,
--   ds.type AS Device,
--   ds.pos AS POS,
--   SUM(A.ibv) AS IBV,
--   SUM(A.bookings_count) AS Booking
--   FROM
--   (
--   SELECT dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id, 
--   COUNT(DISTINCT(id)) bookings_count, SUM(ibv) ibv
--   FROM tajawal_bi.fact_hotel_orders_final INNER JOIN tajawal_bi.dim_date dd ON dim_bookingdate_id = dim_date_id
--   WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (1,2)
--   GROUP BY dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id
-- 
--   UNION ALL
-- 
--   SELECT dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id, 
--   COUNT(DISTINCT(id)) bookings_count, SUM(ibv) ibv
--   FROM tajawal_bi.fact_flight_orders_final INNER JOIN tajawal_bi.dim_date dd ON dim_bookingdate_id = dim_date_id
--   WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (1,2)  
--   GROUP BY dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id
-- 
--   UNION ALL
-- 
--   SELECT dd.month_name, dd.month_number, dd.year_number, CAST(dd.dim_date_id as string) AS dim_bookingdate_id,
--   CASE WHEN  P.products_type IS NULL THEN 'package' ELSE products_type END AS product_type ,
--   CASE WHEN  P.dim_group_id IS NULL THEN '1' ELSE dim_group_id END AS dim_group_id,
--   CASE WHEN  P.dim_store_id IS NULL THEN '1' ELSE dim_store_id END AS dim_store_id,
--   CASE WHEN COUNT(DISTINCT(id))  IS NULL THEN 0 ELSE COUNT(DISTINCT(id)) END AS bookings_count,
--   CASE WHEN SUM(iov)  IS NULL THEN 0 ELSE SUM(iov) END AS ibv
--   FROM   tajawal_bi.dim_date  dd
--   LEFT  JOIN (
--     SELECT * FROM tajawal_bi.fact_package_orders_final  
--     WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (1) 
--   ) AS P ON P.dim_bookingdate_id = dd.dim_date_id
--   WHERE  dd.dim_date_id <= date_format(current_timestamp(), 'yyyyMMdd')
--   GROUP BY dd.month_name, dd.month_number, dd.year_number,dd.dim_date_id,P.products_type, P.dim_group_id, P.dim_store_id
-- 
--   UNION ALL
-- 
--   SELECT dd.month_name, dd.month_number, dd.year_number, CAST(dd.dim_date_id as string) AS dim_bookingdate_id,
--   CASE WHEN  P.products_type IS NULL THEN 'package' ELSE products_type END AS product_type ,
--   CASE WHEN  P.dim_group_id IS NULL THEN '2' ELSE dim_group_id END AS dim_group_id,
--   CASE WHEN  P.dim_store_id IS NULL THEN '1' ELSE dim_store_id END AS dim_store_id,
--   CASE WHEN COUNT(DISTINCT(id))  IS NULL THEN 0 ELSE COUNT(DISTINCT(id)) END AS bookings_count,
--   CASE WHEN SUM(iov)  IS NULL THEN 0 ELSE SUM(iov) END AS ibv
--   FROM   tajawal_bi.dim_date  dd
--   LEFT  JOIN (
--     SELECT * FROM tajawal_bi.fact_package_orders_final 
--     WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (1) 
--   ) AS P ON P.dim_bookingdate_id = dd.dim_date_id
--   WHERE  dd.dim_date_id <= date_format(current_timestamp(), 'yyyyMMdd')
--   GROUP BY dd.month_name, dd.month_number, dd.year_number,dd.dim_date_id,P.products_type, P.dim_group_id, P.dim_store_id                
--   ) A
--   LEFT OUTER JOIN tajawal_bi.dim_store ds ON A.dim_store_id = ds.store_id
--   LEFT OUTER JOIN tajawal_bi.dim_groups gr ON A.dim_group_id = gr.group_id
--   GROUP BY A.month_name , A.`month_number`, A.year_number ,
--   A.dim_bookingdate_id , gr.group_name ,A.product_type ,ds.type ,ds.pos
-- ) A
-- LEFT OUTER JOIN  
-- (
--   SELECT SUM(spend),
--   `dim_date_id` ,
--   `group_name` AS Brand,
--   `product` AS Product,
--   device AS Device,
--   POS AS POS,SUM(spend)AS spend FROM  tajawal_bi.fact_spend_data_channelwise
--   GROUP BY `dim_date_id`,group_name,product,device,POS
-- ) fsc ON fsc.Brand = A.Brand AND fsc.dim_date_id=A.dim_bookingdate_id  AND fsc.product= A.product AND fsc.POS=A.POS AND fsc.Device=A.device
-- LEFT OUTER JOIN
-- (
--   SELECT
--   product,brand ,SUM(target_spend) AS target_spend,SUM(bookings_target) AS bookings_target,SUM(IBV ) AS IBV_target,AVG(CPO_target) AS CPO_target,AVG(CRR_target) AS CRR_target ,
--   `dim_date`
--   FROM tajawal_bi.dim_spend_daily_targets  WHERE product !='Total' AND channel !='Performance Total'
--   GROUP BY   product,brand ,dim_date
-- ) dt ON dt.brand = A.Brand AND dt.dim_date=A.dim_bookingdate_id  AND dt.product= A.product

SELECT  A.MONTH ,A.YEAR,A.Brand,A.Product,A.Device,A.POS,A.IBV,A.Booking,A.Month_num,
A.spend AS Spend ,A.dim_bookingdate_id AS dim_bookingdate_id,
dt.IBV_target  AS ibv_target ,
dt.`bookings_target` AS booking_target,
dt.`target_spend` AS spend_target,
dt.`CPO_target` AS CPO_target,
dt.`CRR_target` AS CRR_target
FROM  (
  SELECT  A.month_name AS MONTH,
  A.`month_number` AS Month_num,
  A.year_number AS YEAR,
  A.dim_bookingdate_id AS dim_bookingdate_id,
  gr.group_name AS Brand ,
  A.product_type AS Product,
  ds.type AS Device,
  ds.pos AS POS,
  SUM(A.ibv) AS IBV,
  SUM(A.bookings_count) AS Booking,
  0 as spend
  FROM
  (
    SELECT dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id, COUNT(DISTINCT(id)) bookings_count, SUM(ibv) ibv
    FROM tajawal_bi.fact_hotel_orders_final INNER JOIN tajawal_bi.dim_date dd ON dim_bookingdate_id = dim_date_id
    WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (1,2)
    GROUP BY dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id
    
    UNION ALL
    
    SELECT dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id, COUNT(DISTINCT(id)) bookings_count, SUM(ibv) ibv
    FROM tajawal_bi.fact_flight_orders_final INNER JOIN tajawal_bi.dim_date dd ON dim_bookingdate_id = dim_date_id
    WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (1,2)  
    GROUP BY dd.month_name, dd.month_number, dd.year_number, dim_bookingdate_id, product_type, dim_group_id, dim_store_id
    
    UNION ALL

    SELECT dd.month_name, dd.month_number, dd.year_number, cast(dd.dim_date_id as string) AS dim_bookingdate_id,
    CASE WHEN  P.products_type IS NULL THEN 'package' ELSE products_type END AS product_type ,
    CASE WHEN  P.dim_group_id IS NULL THEN '1' ELSE dim_group_id END AS dim_group_id,
    CASE WHEN  P.dim_store_id IS NULL THEN '1' ELSE dim_store_id END AS dim_store_id,
    CASE WHEN COUNT(DISTINCT(id))  IS NULL THEN 0 ELSE COUNT(DISTINCT(id)) END AS bookings_count,
    CASE WHEN SUM(iov)  IS NULL THEN 0 ELSE SUM(iov) END AS ibv
    FROM tajawal_bi.dim_date  dd
    LEFT JOIN (
      SELECT * 
      FROM tajawal_bi.fact_package_orders_final  WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (1) 
    ) AS P ON P.dim_bookingdate_id = dd.dim_date_id
    WHERE  dd.dim_date_id <= date_format(current_timestamp(), 'yyyyMMdd')
    GROUP BY dd.month_name, dd.month_number, dd.year_number,dd.dim_date_id,P.products_type, P.dim_group_id, P.dim_store_id

    UNION ALL

    SELECT dd.month_name, dd.month_number, dd.year_number, cast(dd.dim_date_id as string) AS dim_bookingdate_id,
    CASE WHEN  P.products_type IS NULL THEN 'package' ELSE products_type END AS product_type ,
    CASE WHEN  P.dim_group_id IS NULL THEN '2' ELSE dim_group_id END AS dim_group_id,
    CASE WHEN  P.dim_store_id IS NULL THEN '1' ELSE dim_store_id END AS dim_store_id,
    CASE WHEN COUNT(DISTINCT(id))  IS NULL THEN 0 ELSE COUNT(DISTINCT(id)) END AS bookings_count,
    CASE WHEN SUM(iov)  IS NULL THEN 0 ELSE SUM(iov) END AS ibv
    FROM  tajawal_bi.dim_date  dd
    LEFT  JOIN 
    (
      SELECT * FROM tajawal_bi.fact_package_orders_final WHERE dim_group_id != 5 AND dim_status_id != 91 AND dim_group_id IN (2) 
    ) AS P ON P.dim_bookingdate_id = dd.dim_date_id
    WHERE  dd.dim_date_id <= date_format(current_timestamp(), 'yyyyMMdd')
    GROUP BY dd.month_name, dd.month_number, dd.year_number,dd.dim_date_id,P.products_type, P.dim_group_id, P.dim_store_id
  ) A
    LEFT OUTER JOIN dim_store ds ON A.dim_store_id = ds.store_id
    LEFT OUTER JOIN tajawal_bi.dim_groups gr ON A.dim_group_id = gr.group_id
    GROUP BY A.month_name ,
    A.`month_number`,
    A.year_number ,
    A.dim_bookingdate_id ,
    gr.group_name ,
    A.product_type ,
    ds.type ,
    ds.pos                     

    UNION ALL

    SELECT
    dd.month_name AS MONTH,
    dd.`month_number` AS Month_num,
    dd.year_number AS YEAR,
    cast(dd.dim_date_id as string) AS dim_bookingdate_id,
    fsc.group_name AS Brand ,
    fsc.product AS Product,
    fsc.device AS Device,
    fsc.pos AS POS,
    0 AS IBV,
    0 AS Booking,
    ROUND(SUM(spend),0) AS spend
    FROM  tajawal_bi.fact_spend_data_channelwise as fsc
    INNER JOIN tajawal_bi.dim_date dd ON dd.dim_date_id = fsc.dim_date_id
    WHERE  product  NOT IN ('NA','both') AND group_name != 'NA'
    GROUP BY  dd.month_name,dd.`month_number`,dd.year_number,dd.dim_date_id ,fsc.group_name,fsc.product,fsc.device,fsc.POS
  ) A
  LEFT OUTER JOIN (
    SELECT
    product,
    brand ,
    SUM(target_spend) AS target_spend,
    SUM(bookings_target) AS bookings_target,
    SUM(IBV ) AS IBV_target,
    AVG(CPO_target) AS CPO_target,
    AVG(CRR_target) AS CRR_target,
    `dim_date`
    FROM tajawal_bi.dim_spend_daily_targets 
    WHERE product !='Total' AND channel !='Performance Total'
    GROUP BY   product,brand ,dim_date
    ORDER BY brand ,product ,dim_date
) dt
ON dt.brand = A.Brand AND dt.dim_date=A.dim_bookingdate_id  AND dt.product= A.product


;

DROP TABLE IF EXISTS tajawal_bi.marketing_performance_daily;
ALTER TABLE tajawal_bi.inter_marketing_performance_daily RENAME TO tajawal_bi.marketing_performance_daily;


