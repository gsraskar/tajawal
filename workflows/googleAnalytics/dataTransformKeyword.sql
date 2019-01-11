set hive.mv.files.thread=0;

create table tajawal_bi.fact_google_analytics_keyword_incr_inter row format delimited fields terminated by '|'
as
select 
campaign,
sourcemedium,
keyword,
transactionid,
gadate,
adwords_customer_id,
transactions,
transaction_revenue,
view_id,
dsgv.group_name,
dca.account_name
from tajawal_bi.fact_google_analytics_keyword_incr fgak
left outer join tajawal_bi.dim_customer_account dca
on ltrim(rtrim(lower(regexp_replace(dca.customer_id,'-',''))))=ltrim(rtrim(lower(fgak.adwords_customer_id)))
left outer join tajawal_bi.dim_spend_ga_views dsgv
on ltrim(rtrim(fgak.view_id))=ltrim(rtrim(dsgv.dim_view_id))
;

drop table if exists tajawal_bi.fact_google_analytics_keyword_incr;
alter table tajawal_bi.fact_google_analytics_keyword_incr_inter rename to tajawal_bi.fact_google_analytics_keyword_incr;
