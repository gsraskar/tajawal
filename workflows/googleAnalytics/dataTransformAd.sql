set hive.mv.files.thread=0;

drop table if exists tajawal_bi.tmp_mobile_adnetwork_affiliate;
create table if not exists tajawal_bi.tmp_mobile_adnetwork_affiliate as
select adnetwork_name, `date`, max(metric_name) as metric_name, brand, max(type) as type,
case when coalesce(product,'') = '' then 'all' else ltrim(rtrim(lower(product))) end as product,
max(value) as value,
case when coalesce(mobile_os,'') = '' then 'all' else ltrim(rtrim(lower(mobile_os))) end as mobile_os
from googlesheets.mobile_adnetwork
where lower(type) like '%affiliate%' and lower(adnetwork_name) != 'adnetwork_name'
group by 
adnetwork_name, 
`date`, 
brand,
case when coalesce(product,'') = '' then 'all' else ltrim(rtrim(lower(product))) end,
case when coalesce(mobile_os,'') = '' then 'all' else ltrim(rtrim(lower(mobile_os))) end
-- coalesce(mobile_os,'all'),
-- coalesce(product,'all')
;

drop table if exists tajawal_bi.tmp_mobile_adnetwork;
alter table tajawal_bi.tmp_mobile_adnetwork_affiliate rename to tajawal_bi.tmp_mobile_adnetwork;
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
alter table tajawal_bi.tmp_mobile_adnetwork_inter rename to tajawal_bi.tmp_mobile_adnetwork_affiliate;


drop table if exists tajawal_bi.tmp_mobile_adnetwork_cost_affiliate;
create table if not exists tajawal_bi.tmp_mobile_adnetwork_cost_affiliate as
select A.adnetwork_name, A.cost_date as start_date,
LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name,mobile_os,product ORDER BY cost_date) as end_date,metric_name, value, brand, type,mobile_os,product
from
(
select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, value, brand, type,mobile_os,product
from
(
    select adnetwork_name, `date`, metric_name, value, brand, type,mobile_os,product from tajawal_bi.tmp_mobile_adnetwork_affiliate where `date` != 'date'
    union
    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, '0' as value, brand, type,mobile_os,product from
    (select adnetwork_name, brand, type,mobile_os,product,count(*) from tajawal_bi.tmp_mobile_adnetwork_affiliate where `date` != 'date' group by adnetwork_name, brand, type,mobile_os,product) T
) T1
where lower(brand) like '%almosafer%'
order by adnetwork_name, cost_date asc
) A;

insert into tajawal_bi.tmp_mobile_adnetwork_cost_affiliate
select A.adnetwork_name, A.cost_date as start_date,
LEAD(cost_date, 1, from_unixtime(unix_timestamp(CURRENT_TIMESTAMP(), 'yyyy-MM-dd hh:mm:ss.S'), 'yyyyMMdd')) OVER (PARTITION BY adnetwork_name,mobile_os,product ORDER BY cost_date) as end_date,metric_name, value, brand, type,mobile_os,product
from
(
select adnetwork_name, from_unixtime(unix_timestamp(`date`, 'dd/MM/yyyy'), 'yyyyMMdd') as cost_date, metric_name, value, brand, type,mobile_os,product
from
(
    select adnetwork_name, `date`, metric_name, value, brand, type,mobile_os,product from tajawal_bi.tmp_mobile_adnetwork_affiliate where `date` != 'date'
    union
    select adnetwork_name, '01/01/1970' as `date`, 'CPI' as metric_name, '0' as value, brand, type,mobile_os,product from
    (select adnetwork_name, brand, type,mobile_os,product,count(*) from tajawal_bi.tmp_mobile_adnetwork_affiliate where `date` != 'date' group by adnetwork_name, brand, type,mobile_os,product) T
) T1
where lower(brand) like '%tajawal%'
order by adnetwork_name, cost_date asc
) A;

drop table if exists tajawal_bi.fact_google_analytics_ad_incr_inter;
alter table tajawal_bi.fact_google_analytics_ad_incr rename to tajawal_bi.fact_google_analytics_ad_incr_inter;
--Drop table if exists tajawal_bi.fact_adjust_data;
Drop table if exists tajawal_bi.fact_google_analytics_ad_incr_inter2;
Create table if not exists tajawal_bi.fact_google_analytics_ad_incr_inter2 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
--tblproperties("skip.header.line.count"="1")
AS
  select
  `gadate` , 
  `source` , 
  case when lower(regexp_replace(sourcemedium, ' ', '')) like 'google/cpc' then 
    case when upper(campaign) like '%SN-%' then concat(sourcemedium, '/SN') else 
      case when upper(campaign) like '%CN-%' then concat(sourcemedium, '/CN') else 
        case when upper(campaign) like '%VN-%' then concat(sourcemedium, '/VN') else 
          case when c.source_medium is not null then c.source_medium else sourcemedium 
          end
        end
      end 
    end
    else sourcemedium 
  end as sourcemedium,
  `device_category` , 
  `adwords_campaign_id` , 
  `adwords_adgroup_id`,
  `adwords_creative_id`,
  `campaign`,
   COALESCE(`impressions`,0.0) as impressions, 
  COALESCE(`adclicks` ,0.0) as adclicks, 
  -- case when lower(sourcemedium) like '%meta%' or lower(sourcemedium) like '%referral%' or lower(sourcemedium) like '%wego%' 
  -- then transactions_revenue * 0.02
  -- else  COALESCE(`adcost` ,0.0)
  -- end as adcost,
  case when lower(coalesce(B.channel_group, 'na')) like '%meta%' 
    then  transactions_revenue * 0.02
      else case when d.value is not null then coalesce(transactions, 0) * coalesce(d.value, 0)
      else  COALESCE(`adcost` ,0.0)
    end
  end as adcost,
  COALESCE(`transactions` ,0.0) as transactions, 
  COALESCE(`transactions_revenue` ,0.0) as transactions_revenue, 
  COALESCE(`sessions` ,0.0) as sessions, 
  COALESCE(`bounces`,0.0) as bounces,
  COALESCE(`entraces`,0.0) as entraces,
  `view_id`,
(
case when campaign like '%hotel%' and  campaign like '%flight%' 
	 then 'both'
	 when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
     then 'both'
	 when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%'
	 then 'flight'
	 when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
	 then 'hotel' 
	 when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%' 
	 then 'package'
	 when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\fh\_%' 
	 then 'both' 
	 else 'NA' 
	 end) as product,
(
  -- case when lower(campaign) like '%ksa%' or  lower(campaign) like '%sa\_%' or  lower(campaign) like '%kas\_%' or lower(campaign) like '%192%'
  case when lower(campaign) like '%\_sa%' or lower(campaign) like '%\-sa\-%' or lower(campaign) like '%\_ks%' or lower(campaign) like '%ksa%' or  (lower(campaign) like '%sa\_%' and lower(campaign) not like '%usa\_%') or  lower(campaign) like '%kas\_%' or ( lower(campaign) like '%192%' and lower(B.channel) like '%revx%' )
   then 'Saudi Arabia' else
     case when (lower(campaign) like '%ae%' and lower(campaign) not like '%(ae%') or  lower(campaign) like '%uae%' or ( lower(campaign) like '%226%' and lower(B.channel) like '%revx%' ) or lower(campaign) like '%\-uae\-%'
     then 'UAE' else
       case when lower(campaign) like '%kwt_%' or lower(campaign) like '%-kw-%' or lower(campaign) like '%kwi%' or  lower(campaign) like '%kw\_%' or lower(campaign) like '%kuw%' or ( lower(campaign) like '%117%' and lower(B.channel) like '%revx%' )
       then 'Kuwait' else
         case when lower(campaign) like '%bh\_%' or lower(campaign) like '%bah%' or (lower(campaign) like '%20%' and lower(campaign) not like '%220%' and lower(B.channel) like '%revx%')
         then 'Baharain' else 'others'
         end
       end
     end
    end
) as pos,

-- (case when lower(campaign) like '%ksa%' or  lower(campaign) like '%sa\_%' or  lower(campaign) like '%kas\_%' then 'Saudi Arabia' else case when lower(campaign) like '%ae%' or  lower(campaign) like '%uae%' then 'UAE' else case when lower(campaign) like '%kw\_%' or lower(campaign) like '%kuw%' then 'kuwait' else case when lower(campaign) like '%bh\_%' or lower(campaign) like '%bah%' then 'Baharain' else 'NA' end end end end) as pos,
-- (case when lower(campaign) like '%android%' then 'android' else case when lower(campaign) like '%ios\_%' or lower(campaign) like '%\_ios%' then 'ios' else 'NA' end end) as mobile_os,
dim_spend_ga_views.mobile_os,
(case when lower(campaign) like '%ar\_%' or lower(campaign) like '%\_ar%' then 'ar' else case when lower(campaign) like '%en\_%' or lower(campaign) like '%\_en%' then 'en' else 'NA' end end) as language
--case when upper(campaign) like '%SN-%' then
--  case when upper(campaign) like '%BRAND%' or upper(campaign) like '%[TAJAWAL]%' then 'Brand' else 'NonBrand' end
--else 'na'
--end brand_nonbrand,
 
  -- , B.dim_channel_id as dim_channel_id
  -- from tajawal_bi.fact_google_analytics_ad_incr_inter
  from tajawal_bi.fact_google_analytics_ad_incr_inter
  left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', ''))) A on lower(regexp_replace(sourcemedium, ' ', '')) = lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
  left outer join (select dim_channel_id, max(channel_group) as channel_group, max(channel) as channel from tajawal_bi.dim_spend_channels group by dim_channel_id ) B on A.dim_channel_id = B.dim_channel_id
  left outer join googlesheets.dim_google_cpc_sourcemedium_mapping c on lower(campaign) = lower(c.campaign_name)
  left outer join tajawal_bi.dim_spend_ga_views on fact_google_analytics_ad_incr_inter.view_id = dim_spend_ga_views.dim_view_id
  left outer join tajawal_bi.tmp_mobile_adnetwork_cost_affiliate d on 
    lower(rtrim(ltrim(regexp_replace(fact_google_analytics_ad_incr_inter.sourcemedium, ' ', '')))) = lower(rtrim(ltrim(regexp_replace(d.adnetwork_name, ' ', '')))) 
    and dim_spend_ga_views.group_name = d.brand 
    -- and ltrim(rtrim(lower((case when lower(campaign) like '%android%' then 'android' else case when lower(campaign) like '%ios\_%' or lower(campaign) like '%\_ios%' then 'ios' else 'all' end end))))=ltrim(rtrim(lower(d.mobile_os))) 
    and ltrim(rtrim(lower(case when coalesce(dim_spend_ga_views.mobile_os, 'NULL') = 'NULL' then 'all' else dim_spend_ga_views.mobile_os end))) = ltrim(rtrim(lower(d.mobile_os)))
    and ltrim(rtrim(lower(case when campaign like '%hotel%' and  campaign like '%flight%'
         then 'all'
         when (campaign like '%hotel%' or campaign like '%flight%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%') and (lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%')
     then 'all'
         when lower(campaign) like '%flight%' or upper(campaign) like '%-FL-%' or upper(campaign) like '%\_FL\_%' or upper(campaign) like '%\_F\_%'
         then 'flight'
         when lower(campaign) like '%hotel%' or upper(campaign) like '%-HO-%' or upper(campaign) like '%\_HL\_%' or upper(campaign) like '%\_H\_%'
         then 'hotel'
         when lower(campaign) like '%package%' or lower(campaign) like '%\_p\_%'
         then 'package'
         when lower(campaign) like '%both%' or lower(campaign) like '%\_f_h\_%' or lower(campaign) like '%\fh\_%'
         then 'all'
         else 'all'
         end
)))=ltrim(rtrim(lower(d.product)))
  where 
  gadate != 'gadate' and
  (
    d.start_date is null
    or (regexp_replace(gadate, '-', '') >= d.start_date and regexp_replace(gadate, '-', '') < d.end_date)
  )
; 


Create table if not exists tajawal_bi.fact_google_analytics_ad_incr ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
--tblproperties("skip.header.line.count"="1")
AS
  select C.*, 
  case when lower(ltrim(rtrim(channel_group))) like '%sem%' and lower(ltrim(rtrim(c.campaign))) like '%-brand-%' then 'Brand'
       when lower(ltrim(rtrim(channel_group))) like '%crm%' or lower(ltrim(rtrim(channel_group))) like  '%display%' then 'Brand'
       else 'Non Brand'
  end as brand_nonbrand,
  case when lower(ltrim(rtrim(channel_group))) like '%social media%' and lower(ltrim(rtrim(c.campaign))) like '%nc%' then 'Aquisition'
       when lower(ltrim(rtrim(channel_group))) like '%social media%' then 'Retargeting'
       when lower(ltrim(rtrim(channel_group))) like '%retargeting%' or lower(ltrim(rtrim(channel_group))) like  '%crm%' then 'Retargeting'
       when lower(ltrim(rtrim(channel_group))) like '%direct%' then 'NA' 
       when lower(ltrim(rtrim(channel_group))) like '%re%engagement%' then 'Retargeting' else 'Aquisition'
  end as objective,
  B.dim_channel_id as dim_channel_id
  from tajawal_bi.fact_google_analytics_ad_incr_inter2 C
  left outer join (select lower(regexp_replace(ga_sourcemedium, ' ', '')) as ga_sourcemedium, max(dim_channel_id) as dim_channel_id from tajawal_bi.dim_spend_ga_sourcemedium_channels group by lower(regexp_replace(ga_sourcemedium, ' ', ''))) A on lower(regexp_replace(sourcemedium, ' ', '')) = lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
  left outer join (select dim_channel_id, max(channel_group) as channel_group from tajawal_bi.dim_spend_channels group by dim_channel_id ) B on A.dim_channel_id = B.dim_channel_id
;
 
drop table if exists tajawal_bi.fact_google_analytics_ad_incr_inter;

-- ==== Step 1 : Merge new channels seen in googlesheets.dim_ga_sourcemedium_channels into dim_spend_channels table ==== --
-- Create table dim_spend_channels if not exists
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_channels (
    dim_channel_id string,
    channel_group string,
    channel string,
    paid_unpaid string
);
-- Populate table with new channels
DROP TABLE IF EXISTS tajawal_bi.dim_spend_channels_inter;
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_channels_inter row format delimited fields terminated by '|' as
SELECT dim_channel_id, 
case when max(coalesce(channel_group, -1)) = '-1' then null else max(coalesce(channel_group, -1)) end as channel_group,
case when max(coalesce(channel, -1)) = '-1' then null else max(coalesce(channel, -1)) end as channel,
case when max(coalesce(paid_unpaid, -1)) = '-1' then null else max(coalesce(paid_unpaid, -1)) end as paid_unpaid
FROM
(
--    select * from
--    tajawal_bi.dim_spend_channels
--    union all
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

-- ==== Step 2 : Merge new network=>channel mapping into dim_ga_sourcemedium_channels table ==== --
-- Create table dim_spend_ga_sourcemedium_channels
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_ga_sourcemedium_channels (
    dim_sourcemedium string,
    dim_channel_id string
);
-- POPULATE table with new channels.
DROP TABLE IF EXISTS tajawal_bi.dim_spend_ga_sourcemedium_channels_inter;
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_ga_sourcemedium_channels(                                                                      
channel_group varchar(128),
dim_sourcemedium varchar(128),                                                                                                         
dim_channel_id varchar(128)
);
CREATE TABLE IF NOT EXISTS tajawal_bi.dim_spend_ga_sourcemedium_channels_inter row format delimited fields terminated by '|' as
SELECT *
FROM
(
--    select * from
--    (
--        select * from tajawal_bi.dim_spend_ga_sourcemedium_channels
--    ) T1
--    union
    select T2.ga_channel_grouping, T2.ga_sourcemedium, T2.dim_channel_id from
    (
        select da.ga_channel_grouping, da.ga_sourcemedium, da.channel_group, da.channel, ds.dim_channel_id
        from googlesheets.dim_ga_sourcemedium_channels da inner join tajawal_bi.dim_spend_channels ds 
        on lower(regexp_replace(concat(da.channel_group, da.channel), ' ', '')) = lower(regexp_replace(concat(ds.channel_group, ds.channel), ' ', ''))
        union
        select da.channel_group as ga_channel_grouping, da.source_medium, da.channel_group, da.channel, ds.dim_channel_id
        from googlesheets.dim_google_cpc_sourcemedium_mapping da inner join tajawal_bi.dim_spend_channels ds
        on lower(regexp_replace(concat(da.channel_group, da.channel), ' ', '')) = lower(regexp_replace(concat(ds.channel_group, ds.channel), ' ', ''))
    ) T2 
--    left outer join tajawal_bi.dim_spend_ga_sourcemedium_channels dsa on T2.dim_channel_id = dsa.dim_channel_id
--    where dsa.dim_channel_id is null
) T;
DROP TABLE tajawal_bi.dim_spend_ga_sourcemedium_channels;
ALTER TABLE tajawal_bi.dim_spend_ga_sourcemedium_channels_inter RENAME TO tajawal_bi.dim_spend_ga_sourcemedium_channels;

-- Drop table if exists tajawal_bi.fact_affiliate_data_incr;
-- Create table if not exists tajawal_bi.fact_affiliate_data_incr row format delimited fields terminated by '|'  as
-- select 
-- C.channel_group, C.channel, B.group_name, A.gadate, A.sourcemedium, A.device_category, A.campaign, 
-- sum(A.impressions) as impressions, sum(A.adclicks) as adclicks, sum(A.adcost) as adcost,
-- sum(A.cpm)/ count(*) as cpm, sum(A.cpc) / count(*) as cpc, sum(A.ctr) / count(*) as ctr, sum(A.transactions) as transactions, sum(A.transactions_revenue) as transactions_revenue, sum(A.sessions) as sessions, 
-- A.view_id, A.product, A.pos, A.mobile_os, A.language, A.brand_nonbrand
-- from tajawal_bi.fact_google_analytics_ad_incr A inner join tajawal_bi.dim_spend_ga_views B on A.view_id = B.dim_view_id
-- inner join tajawal_bi.dim_spend_channels C on A.dim_channel_id = C.dim_channel_id
-- where lower(C.channel_group) like '%affiliate%'
-- group by
-- C.channel_group, C.channel, B.group_name, A.gadate, A.sourcemedium, A.device_category, A.campaign,
-- A.view_id, A.product, A.pos, A.mobile_os, A.language, A.brand_nonbrand
-- ;
-- Drop table if exists tajawal_bi.fact_affiliate_data_incr;
-- Create table if not exists tajawal_bi.fact_affiliate_data_incr row format delimited fields terminated by '|'  as
-- select C.channel_group, C.channel, B.group_name, A.gadate, A.sourcemedium, A.device_category, A.campaign, A.impressions, A.adclicks, A.adcost,
--  A.transactions, A.transactions_revenue, A.sessions, A.view_id, A.product, A.pos, A.mobile_os, A.language, A.brand_nonbrand
-- from tajawal_bi.fact_google_analytics_ad_incr A inner join tajawal_bi.dim_spend_ga_views B on A.view_id = B.dim_view_id
-- inner join
-- (
--     select B.channel_group, B.channel, A.dim_channel_id, A.ga_sourcemedium, count(*) 
--     from tajawal_bi.dim_spend_ga_sourcemedium_channels A inner join tajawal_bi.dim_spend_channels B 
--     on A.dim_channel_id = B.dim_channel_id 
--     where lower(B.channel_group) like '%affiliate%'
--     group by B.channel_group, B.channel, A.dim_channel_id, A.ga_sourcemedium
-- ) C on regexp_replace(A.sourcemedium, ' ', '') = regexp_replace(C.ga_sourcemedium, ' ', '');




