set hive.mv.files.thread=0;
drop table if exists tajawal_bi.fact_google_analytics_app_pageviews_incr_inter;
create table tajawal_bi.fact_google_analytics_app_pageviews_incr_inter ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
as
select
galang,
country,
gadate,
sourcemedium,
devicecategory,
rtrim(ltrim(screen_name)) as screen_name,
screen_views,
uniq_screen_views,
view_id,
case when ltrim(rtrim(screen_name)) = 'Home' then 'home'
     when rtrim(ltrim(screen_name)) = 'Hotel Results' then 'results'
     when screen_name rlike 'Flight Home|Holidays Home|Hotel Home' then 'home'
     when screen_name rlike 'Flight Details|Hotel Details|Holidays Package Details' then 'details'
     when screen_name rlike 'Flight Payment|Hotel Payment|Holidays Payment' then 'payment'
     when screen_name rlike 'Flight Results|Holidays Select Package' then 'results'
     when screen_name rlike 'Holidays Confrimation|Hotel Confirmation|Flight Confirmation' then 'confirmation'
     when screen_name rlike 'Flight Travelers Details|Hotel Guests|Holidays Traveller Details' then 'guest details'
     else 'na'
     end
as pagetype,
case when lower(screen_name) like '%flight%' then 'flight'
     when lower(screen_name) like '%hotel%' then 'hotel'
     when lower(screen_name) like '%holidays%' then 'package'
     else 'na'
end as product
from tajawal_bi.fact_google_analytics_app_pageviews_incr
where screen_name rlike 'Flight Home|Flight Results|Flight Details|Flight Travelers Details|Flight Payment|Flight Confirmation|Hotel Home|Hotel Details|Hotel Guests|Hotel Payment|Hotel Confirmation|Holidays Home|Holidays Select Package|Holidays Package Details|Holidays Traveller Details|Holidays Payment|Holidays Confrimation' or ltrim(rtrim(screen_name))='Home' or ltrim(rtrim(screen_name))='Hotel Results';
drop table if exists tajawal_bi.fact_google_analytics_app_pageviews_incr;
alter table tajawal_bi.fact_google_analytics_app_pageviews_incr_inter rename to tajawal_bi.fact_google_analytics_app_pageviews_incr;
