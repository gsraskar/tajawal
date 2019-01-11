
drop table if exists Fact_kpi_management_summary_newgbv_1_tmp;
create table Fact_kpi_management_summary_newgbv_1_tmp like Fact_kpi_management_summary;

drop table if exists dashboard_management_kpi_input_tmp;
alter table dashboard_management_kpi_input rename to dashboard_management_kpi_input_tmp;
create table dashboard_management_kpi_input like dashboard_management_kpi_input_tmp;
insert into dashboard_management_kpi_input select case when day = 'null' then '19700101' else day end as day, business_type, brand, product, kpi, value, mtd from dashboard_management_kpi_input_tmp;

-- Insert into final table union of 4 queries
insert into Fact_kpi_management_summary_newgbv_1_tmp
-- QUERY 1 : Group, Product level all KPIs except AOV
select  case when Day = 'null' then '1970-01-01' else Day end, FH.business_type, gr.group_name , FH.Product,KPI,Value,MTD,round(MTD / (day(Day)/DAY(LAST_DAY(Day))),2) as "Month End Forecasted" , 
case when KPI ='IBV ($)' Then  ibv_target      
     when KPI ='GBV ($)' Then gbv_target
     when KPI ='# of Transactions'  Then booking_target 
     when KPI ='Spend ($)' Then  spend_target
     when KPI ='CPO ($)' Then  cpo_target
     when KPI ='CRR ($)' Then  crr_target
     when KPI ='AOV ($)' Then  aov_target
    
Else 0 end  
 as 'Target' 

from dashboard_management_kpi_input FH inner join bi.dim_groups gr on FH.brand = gr.group_id
left join dim_kpi_daily_target as DKT on FH.Product = DKT.product and DKT.Month= MONTHNAME(Day) and DKT.Year= YEAR(Day) and DKT.brand=gr.group_name COLLATE utf8_unicode_ci  and DKT.dim_date=Day  

where KPI != "AOV ($)"

union all

-- QUERY 2 : Group, Product level for KPI : AOV
select A.day, A.business_type, max(dim_groups.group_name), A.product as product, 'AOV ($)' as KPI, case when sum(B.value) = 0 then 0 else sum(A.value) / sum(B.value) end as value, 
case when sum(B.mtd) = 0 then 0 else sum(A.mtd) / sum(B.mtd) end as mtd,
case when (sum(B.mtd) / (day(A.Day)/DAY(LAST_DAY(A.Day)))) = 0 then 0 else round( (sum(A.mtd) /(day(A.Day)/DAY(LAST_DAY(A.Day)))) / (sum(B.mtd) / (day(A.Day)/DAY(LAST_DAY(A.Day)))), 2) end as 'Month End Forecasted',
max(DKTs.AOV)
from
(select day, business_type, brand, product, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input 
where kpi = 'IBV ($)' group by day, business_type, brand, product ) A
inner join
(select day, business_type, brand, product, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input where kpi = '# of Transactions' group by day,business_type, brand, product) B
on A.day = B.day and A.business_type = B.business_type and A.brand = B.brand and A.product = B.product
inner join dim_groups on A.brand = dim_groups.group_id

left join dim_kpi_target_new as DKTs on DKTs.product = A.product and DKTs.Month= MONTHNAME(A.Day) and DKTs.Year= YEAR(A.Day) and DKTs.brand=dim_groups.group_name
 COLLATE utf8_unicode_ci group by A.business_type, A.day, A.brand, A.product

union all

-- QUERY 3 : Group, for 'all' products all KPIs
select case when Day = 'null' then '1970-01-01' else Day end, FH.business_type, gr.group_name,'all' as Product,KPI,Sum(Value),SUM(MTD), case when KPI ='AOV ($)' then SUM(MTD) else SUM(round(MTD / (day(Day)/DAY(LAST_DAY(Day))),2))END  as "Month End Forecasted" ,
max(case  when KPI ='IBV ($)' Then  ibv_target      
     when KPI ='GBV ($)' Then gbv_target
     when KPI ='# of Transactions'  Then booking_target 
     when KPI ='Spend ($)' Then  spend_target
     when KPI ='CPO ($)' Then  cpo_target
     when KPI ='CRR ($)' Then  crr_target
     when KPI ='AOV ($)' Then  aov_target

Else 0 end  
)
 as 'Target'  
from dashboard_management_kpi_input  FH  inner join uat_bi.dim_groups gr on FH.brand = gr.group_id 
left join dim_kpi_daily_target as DKT on DKT.Month= MONTHNAME(Day) and DKT.Year= YEAR(Day) and DKT.brand=gr.group_name COLLATE utf8_unicode_ci and  DKT.dim_date=Day and DKT.product = 'all' 
where KPI != "AOV ($)"
group by FH.business_type,Day,FH.Brand,KPI, gr.group_name 

union all 

-- QUERY 4 : Group, for 'all' products KPI : AOV
select A.day, A.business_type, max(dim_groups.group_name) as group_name, 'all' as product, 'AOV ($)' as KPI, CASE WHEN sum(B.value) = 0 then 0 else sum(A.value) / sum(B.value) end as value, 
case when sum(B.mtd) = 0 then 0 else sum(A.mtd) / sum(B.mtd) end as mtd,
case when sum(B.mtd) = 0 then 0 else round( (sum(A.mtd) /(day(A.Day)/DAY(LAST_DAY(A.Day)))) / (sum(B.mtd) / (day(A.Day)/DAY(LAST_DAY(A.Day)))), 2) end as 'Month End Forecasted',
max(DKTs.AOV) as AOV
from
(select day, business_type, brand, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input 
where kpi = 'IBV ($)' group by day, business_type, brand ) A
inner join
(select day, business_type, brand, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input where kpi = '# of Transactions' group by day, business_type, brand) B
on A.day = B.day and A.business_type = B.business_type and A.brand = B.brand
inner join dim_groups on A.brand = dim_groups.group_id
left join dim_kpi_target_new as DKTs on DKTs.Month= MONTHNAME(A.Day) and DKTs.Year= YEAR(A.Day) and DKTs.brand=dim_groups.group_name COLLATE utf8_unicode_ci and DKTs.product = 'all'
group by A.day, A.business_type, A.brand

union all

-- QUERY 5 : Group 'all', Product level all KPIs except AOV
select  case when Day = 'null' then '1970-01-01' else Day end, FH.business_type, 'all' as Brand , FH.Product,KPI,sum(Value),sum(MTD),round(sum(MTD) / (day(Day)/DAY(LAST_DAY(Day))),2)  as "Month End Forecasted" , 
sum(case when KPI ='IBV ($)' Then  ibv_target      
     when KPI ='GBV ($)' Then gbv_target
     when KPI ='# of Transactions'  Then booking_target 
     when KPI ='Spend ($)' Then  spend_target
     when KPI ='CPO ($)' Then  cpo_target
     when KPI ='CRR ($)' Then  crr_target
     when KPI ='AOV ($)' Then  aov_target
Else 0 end) 
 as 'Target' 
from dashboard_management_kpi_input FH inner join uat_bi.dim_groups gr on FH.brand = gr.group_id
left join 
-- dim_kpi_daily_target
(
  select brand, product, month, year, dim_date, sum(ibv_target) as ibv_target, sum(gbv_target) as gbv_target, sum(booking_target) as booking_target, sum(spend_target) as spend_target, 
  avg(aov_target) as aov_target, avg(cpo_target) as cpo_target, avg(crr_target) as crr_target
  from dim_kpi_daily_target 
  group by brand, product, month, year, dim_date
) as DKT on gr.group_name = DKT.brand and DKT.product = FH.Product and DKT.Month= MONTHNAME(Day) and DKT.Year= YEAR(Day) and DKT.dim_date=Day  
where KPI != "AOV ($)" and KPI!= 'CPO ($)' and KPI!= 'CRR ($)'
group by case when Day = 'null' then '1970-01-01' else Day end, FH.business_type, FH.Product,FH.day, KPI

union all
-- QUERY 6 : Group 'all', Product level for KPIs CPO and CRR
select  case when Day = 'null' then '1970-01-01' else Day end, FH.business_type, 'all' as Brand , FH.Product,KPI,sum(Value),sum(MTD),
case when (day(Day)/DAY(LAST_DAY(Day))) = 0 then 0 else round(sum(MTD) / (day(Day)/DAY(LAST_DAY(Day))),2) end  as "Month End Forecasted" ,
avg(case when KPI ='IBV ($)' Then  ibv_target      
     when KPI ='GBV ($)' Then gbv_target
     when KPI ='# of Transactions'  Then booking_target 
     when KPI ='Spend ($)' Then  spend_target
     when KPI ='CPO ($)' Then  cpo_target
     when KPI ='CRR ($)' Then  crr_target
     when KPI ='AOV ($)' Then  aov_target
Else 0 end)
 as 'Target'
from dashboard_management_kpi_input FH inner join uat_bi.dim_groups gr on FH.brand = gr.group_id
left join 
-- dim_kpi_daily_target as DKT on DKT.product = FH.product and DKT.Month= MONTHNAME(Day) and DKT.Year= YEAR(Day) and DKT.brand = gr.group_name and DKT.dim_date=Day  
(
  select product, month, year, dim_date, sum(ibv_target) as ibv_target, sum(gbv_target) as gbv_target, sum(booking_target) as booking_target, sum(spend_target) as spend_target, 
  avg(aov_target) as aov_target, avg(cpo_target) as cpo_target, avg(crr_target) as crr_target
  from dim_kpi_daily_target 
  group by product, month, year, dim_date
) as DKT on DKT.product = FH.Product and DKT.Month= MONTHNAME(Day) and DKT.Year= YEAR(Day) and DKT.dim_date=Day
where KPI= 'CPO ($)' or KPI= 'CRR ($)'
group by case when Day = 'null' then '1970-01-01' else Day end, FH.business_type, FH.Product, FH.day, KPI

union all

-- QUERY 7 : Group 'all', Product level for KPI : AOV
select T.day, T.business_type, 'all' as Brand, T.product as product, 'AOV ($)' as KPI, 
case when sum(T.bookings_value) = 0 then 0 else sum(T.ibv_value) / sum(T.bookings_value) end as value, 
case when sum(T.bookings_mtd) = 0 then 0 else sum(T.ibv_mtd) / sum(T.bookings_mtd) end as mtd,
case when (sum(T.bookings_mtd) / (day(T.Day)/DAY(LAST_DAY(T.Day)))) = 0 then 0 else round( (sum(T.ibv_mtd) /(day(T.Day)/DAY(LAST_DAY(T.Day)))) / (sum(T.bookings_mtd) / (day(T.Day)/DAY(LAST_DAY(T.Day)))), 2) end as 'Month End Forecasted',
T.AOV_target
from
(
select A.day, A.business_type, A.product, sum(A.value) as ibv_value, sum(A.mtd) as ibv_mtd, sum(B.value) as bookings_value, sum(B.mtd) as bookings_mtd, avg(DKTs.AOV) as AOV_target from 
(select day, business_type, brand, product, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input 
where kpi = 'IBV ($)' group by day, business_type, brand, product ) A
inner join
(select day, business_type, brand, product, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input where kpi = '# of Transactions' group by day, business_type, brand, product) B
on A.day = B.day and A.business_type = B.business_type  and A.brand = B.brand and A.product = B.product
inner join dim_groups on A.brand = dim_groups.group_id
left join dim_kpi_target_new as DKTs on DKTs.product = A.product and DKTs.Month= MONTHNAME(A.Day) and DKTs.Year= YEAR(A.Day) 
group by A.day, A.business_type, A.product
) T
group by T.day, T.business_type, T.product

union all

-- QUERY 8 : Group 'all', Product 'all' all KPIs except AOV, CRR, CPO
select  case when Day = 'null' then '1970-01-01' else Day end, business_type, 'all' as Brand , 'all' as product,KPI,sum(Value),sum(MTD),
case when (day(Day)/DAY(LAST_DAY(Day))) = 0 then 0 else round(sum(MTD) / (day(Day)/DAY(LAST_DAY(Day))),2) end  as "Month End Forecasted" , 
sum(case when KPI ='IBV ($)' Then  ibv_target      
     when KPI ='GBV ($)' Then gbv_target
     when KPI ='# of Transactions'  Then booking_target 
     when KPI ='Spend ($)' Then  spend_target
     when KPI ='CPO ($)' Then  cpo_target
     when KPI ='CRR ($)' Then  crr_target
     when KPI ='AOV ($)' Then  aov_target
Else 0 end) 
 as 'Target' 
from dashboard_management_kpi_input FH inner join uat_bi.dim_groups gr on FH.brand = gr.group_id 
left join (
select * from dim_kpi_daily_target -- where product = 'all'
) as DKT on DKT.Month= MONTHNAME(Day) and DKT.Year= YEAR(Day)
 and FH.product = DKT.product and DKT.brand= gr.group_name and DKT.dim_date=Day -- and FH.product != 'package' 
where KPI != "AOV ($)" and KPI != "CPO ($)" and KPI != "CRR ($)"
group by case when Day = 'null' then '1970-01-01' else Day end, business_type, KPI, FH.day

union all
-- QUERY 9 : Group 'all', Product 'all' all KPIs for CRR and CPO
select  case when Day = 'null' then '1970-01-01' else Day end, business_type, 'all' as Brand , 'all' as product,KPI,sum(Value),sum(MTD),
case when (day(Day)/DAY(LAST_DAY(Day))) = 0 then 0 else round(sum(MTD) / (day(Day)/DAY(LAST_DAY(Day))),2) end as "Month End Forecasted" ,
avg(case when KPI ='IBV ($)' Then  ibv_target      
     when KPI ='GBV ($)' Then gbv_target
     when KPI ='# of Transactions'  Then booking_target 
     when KPI ='Spend ($)' Then  spend_target
     when KPI ='CPO ($)' Then  cpo_target
     when KPI ='CRR ($)' Then  crr_target
     when KPI ='AOV ($)' Then  aov_target
Else 0 end)
 as 'Target'
from dashboard_management_kpi_input FH inner join uat_bi.dim_groups gr on FH.brand = gr.group_id
left join dim_kpi_daily_target as DKT on DKT.Month= MONTHNAME(Day) and DKT.Year= YEAR(Day) and DKT.brand=gr.group_name and DKT.Product = 'all' and DKT.dim_date=Day and FH.product != 'package'
where KPI = "CPO ($)" or KPI = "CRR ($)"
group by case when Day = 'null' then '1970-01-01' else Day end, business_type,FH.day,  KPI


union all

-- QUERY 10 : Group, 'all' level for KPI : AOV
select T.day, T.business_type, 'all' as Brand, 'all' as product, 'AOV ($)' as KPI, CASE WHEN T.bookings_value = 0 then 0 else sum(T.ibv_value) / sum(T.bookings_value) end as value, 
case when sum(T.bookings_mtd) = 0 then 0 else sum(T.ibv_mtd) / sum(T.bookings_mtd) end as mtd,
case when sum(T.bookings_mtd) = 0 then 0 else round( (sum(T.ibv_mtd) /(day(T.Day)/DAY(LAST_DAY(T.Day)))) / (sum(T.bookings_mtd) / (day(T.Day)/DAY(LAST_DAY(T.Day)))), 2) end as 'Month End Forecasted',
T.AOV_target
from
(
select A.day, A.business_type, sum(A.value) as ibv_value, sum(A.mtd) as ibv_mtd, sum(B.value) as bookings_value, sum(B.mtd) as bookings_mtd, avg(DKTs.AOV) as AOV_target from 
(select day, business_type, brand, product, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input 
where kpi = 'IBV ($)' group by day, business_type, brand, product ) A
inner join
(select day, business_type, brand, product, sum(value) as value, sum(mtd) as mtd from dashboard_management_kpi_input where kpi = '# of Transactions' group by day, business_type, brand, product) B
on A.day = B.day and A.business_type = B.business_type  and A.brand = B.brand and A.product = B.product
inner join dim_groups on A.brand = dim_groups.group_id
left join dim_kpi_target_new as DKTs on DKTs.Month= MONTHNAME(A.Day) and DKTs.Year= YEAR(A.Day)  and DKTs.product = 'all' and A.product != 'package'
group by A.day, A.business_type
) T
group by T.day, T.business_type;

-- Replace CPO, CRR with correct values (Spend / <metric>)
drop table if exists Fact_kpi_management_summary_newgbv_spend_cpocrr_1;
create table Fact_kpi_management_summary_newgbv_spend_cpocrr_1  as
select 
Day,
business_type,
Brand,
Product,
KPI,
round(Value, 2) as Value,
round(MTD, 2) as MTD,
round(`Month End Forecasted`, 2) as `Month End Forecasted`,
round(Target, 2) as Target 
from Fact_kpi_management_summary_newgbv_1_tmp
where KPI not in ('CPO ($)', 'CRR ($)')
union
select 
B.Day, 
B.business_type, 
B.Brand, 
B.Product, 
B.KPI, 
case when coalesce(B.Value, 0) = 0 then 0 else round(A.Value / B.Value, 5) end as Value, 
case when coalesce(B.MTD, 0) = 0 then 0 else round(A.MTD / B.MTD, 5) end as MTD,
case when coalesce(B.`Month End Forecasted`, 0) = 0 then 0 else round(A.`Month End Forecasted` / B.`Month End Forecasted`, 5) end as `Month End Forecasted`,
round(B.Target, 5) as Target  
from 
(
    select * from Fact_kpi_management_summary_newgbv_1_tmp 
    where KPI = 'Spend ($)' -- and business_type = 'online'
) A inner join 
(
    select * from Fact_kpi_management_summary_newgbv_1_tmp 
    where KPI in ('CPO ($)', 'CRR ($)') -- and business_type = 'online'
) B 
on A.Day = B.Day and A.business_type = B.business_type and 
A.Brand = B.Brand and A.Product = B.Product
;
-- union
-- select * from Fact_kpi_management_summary_newgbv_1_tmp where KPI in ('CPO ($)', 'CRR ($)') and business_type like '%offline%';


drop table if exists Fact_kpi_management_summary_newgbv;
alter table Fact_kpi_management_summary_newgbv_spend_cpocrr_1 rename to Fact_kpi_management_summary_newgbv;
-- We do not want incorrect data to be displayed for today, so deleting today's data
delete from Fact_kpi_management_summary_newgbv where Day = current_date();
-- There is only 1 brand for offline data, so deleting 'all' from it
delete from Fact_kpi_management_summary_newgbv where Brand = 'all' and business_type = 'offline';
-- In GBV calculations above, GBV is getting calculated at product level, this is not required at product level so removing it.
-- update Fact_kpi_management_summary_newgbv set Target = 0 where KPI = 'GBV ($)' and Product != 'all';

