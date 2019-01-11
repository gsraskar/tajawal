set hive.mv.files.thread=0;
DROP TABLE IF EXISTS tajawal_bi.intermediate_fact_facebook_spend;
CREATE TABLE IF NOT EXISTS tajawal_bi.intermediate_fact_facebook_spend ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
SELECT
account_id,
account_name,
ad_id,
regexp_replace(ad_name,'\n','') as ad_name,
adset_id,
adset_name,
campaign_id,
campaign_name,
impressions,
clicks,
cpc,
cpm,
cpp,
ctr,
deeplink_clicks,
date_start,
date_stop,
inline_link_clicks,
spend,
website_clicks,
country,
region,
case when lower(campaign_name) like 'in\_%' or lower(campaign_name) like 'insta\_%' then 'instagram'
  else case when lower(campaign_name) like '%fb\_%' or lower(campaign_name) like 'facebook\_%' then 'facebook' else 'na' end
end fb_in_flag,
(
case when ((lower(ad_name) like '%hotel%' or lower(ad_name) like '%hotels%') and (lower(ad_name) like '%flight%' or lower(ad_name) like '%flights%' )) or lower(ad_name) like '%both%' 
	 then 'both'
	 when (ad_name like '%hotel%' or ad_name like '%flight%' or upper(ad_name) like '%-HO-%' or upper(ad_name) like '%\_HL\_%' or upper(ad_name) like '%-FL-%' or upper(ad_name) like '%\_FL\_%') and (lower(ad_name) like '%package%' or lower(ad_name) like '%\_p\_%')
     then 'both'
     when lower(ad_name) like '%flight%' or lower(ad_name) like '%flights%' or upper(ad_name) like '%\_FL\_%' 
     then 'flight'
     when lower(ad_name) like '%hotel%' or lower(ad_name) like '%hotels%' or upper(ad_name) like '%\_HO\_%' 
     then 'hotel'
     when lower(ad_name) like '%package%' or lower(ad_name) like '%\_p\_%' 
	 then 'package'
     else 'NA' 
     end) as product
, case when lower(account_name) like '% ksa %' or lower(account_name) like '% sa %' or lower(account_name) like '%kas%' then 'Saudi Arabia'
    else
      case when lower(account_name) like '% ae %' or lower(account_name) like '% uae %' then 'UAE'
      else
        case when lower(account_name) like '% kuwait %' then 'Kuwait'
        else
          case when lower(account_name) like '$bh%' then 'Bahrain' else 'NA' end
        end
      end
    end as pos
, case when lower(campaign_name) like '%android%' then 'android' else case when lower(campaign_name) like '%ios\_%' or lower(campaign_name) like '%_ios%' then 'ios' else 'NA' end end as mobile_os
, case when lower(campaign_name) like '%ar\_%' or lower(campaign_name) like '%\_ar%' then 'ar' else case when lower(campaign_name) like '%en\_%' or lower(campaign_name) like '%\_en%' then 'en' else 'NA' end end as language
,case when lower(campaign_name) like '%app%' or lower(campaign_name) like '%mobile%' then 'app' else 'web' end device,
case when lower(campaign_name) like 'in\_%' or lower(campaign_name) like 'insta\_%' then 'instagram paid'
  else case when lower(campaign_name) like '%fb\_%' or lower(campaign_name) like 'facebook\_%' then 'facebook paid' else 'na' end
end sub_channel,
'social media' as channel
, md5(lower(regexp_replace(concat('social media', case when lower(campaign_name) like 'in\_%' or lower(campaign_name) like 'insta\_%' then 'instagram paid'
  else case when lower(campaign_name) like '%fb\_%' or lower(campaign_name) like 'facebook\_%' then 'facebook paid' else 'na' end
end), ' ', ''))) as dim_channel_id,
case when lower(account_name) like '%almosafer%' then 'almosafer'
  else case when lower(account_name) like '%tajawal%' then 'tajawal' else 'na' end
end groupName,
'NonBrand' as brand_nonbrand,
CASE when lower(campaign_name) like '%nc%' then 'aquisition' else 'retargeting' end objective
from tajawal_bi.fact_facebook_spend;
DROP TABLE IF EXISTS tajawal_bi.fact_facebook_spend;
ALTER TABLE tajawal_bi.intermediate_fact_facebook_spend RENAME TO tajawal_bi.fact_facebook_spend;


--=========================================Consolidation query

Drop table if exists tajawal_bi.inter1_fact_facebook_consolidated;
Create table  tajawal_bi.inter1_fact_facebook_consolidated ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  as
select
A.campaign_id,
A.campaign_name,
A.adset_id,
A.adset_name,
A.ad_id,
A.ad_name,
A.date_start,
A.account_id,
A.account_name,
A.impressions,
A.clicks,
A.deeplink_clicks,
A.inline_link_clicks,
A.spend,
A.website_clicks,
A.fb_in_flag,
A.product,
A.pos,
A.mobile_os,
A.language,
A.device,
A.sub_channel,
A.channel,
A.dim_channel_id,
A.groupname,
COALESCE(B.installs,0.0) as installs,
COALESCE(B.bookings,0.0) as bookings,
COALESCE(B.revenue,0.0) as revenue,
COALESCE(B.sessions,0.0) as sessions,
COALESCE(B.hotel_searches_adjust,0.0) as hotel_searches_adjust,
COALESCE(B.flight_searches_adjust,0.0) as flight_searches_adjust,
B.source
from
(
Select
campaign_id,
adset_id,
ad_id,
date_start,
max(account_id) as account_id,
max(account_name) as account_name,
max(ad_name) as ad_name,
max(adset_name) as adset_name,
max(campaign_name) as campaign_name,
sum(COALESCE(impressions,0.0)) as impressions,
sum(COALESCE(clicks,0.0)) as clicks,
sum(COALESCE(deeplink_clicks,0.0)) as deeplink_clicks,
sum(COALESCE(inline_link_clicks,0.0)) as inline_link_clicks,
sum(COALESCE(spend,0.0)) as spend,
sum(COALESCE(website_clicks,0.0)) as website_clicks,
max(fb_in_flag) as fb_in_flag,
max(product) as product,
max(pos) as pos,
max(mobile_os) as mobile_os,
max(language) as language,
max(device) as device,
max(sub_channel) as sub_channel,
max(channel) as channel,
max(dim_channel_id) as dim_channel_id,
max(groupname) as groupname
From tajawal_bi.fact_facebook_spend Group By
--campaign_id, campaign_name, adset_id, adset_name, ad_id,ad_name,date_start
campaign_id, adset_id, ad_id,date_start
 )A
left outer join (
Select
ltrim(rtrim(regexp_replace(split(campaign,' ')[1],'\\(|\\)',''))) as campaign_id,
ltrim(rtrim(regexp_replace(split(adgroup,' ')[1],'\\(|\\)',''))) as adgroup_id,
ltrim(rtrim(regexp_replace(split(creative,' ')[1],'\\(|\\)',''))) as ad_id,
`date`,
sum(COALESCE(installs,0.0)) as installs,
sum(COALESCE(revenue_events,0.0)) as bookings,
sum(COALESCE(revenue,0.0)) as revenue,
sum(COALESCE(sessions,0.0)) as sessions,
sum(COALESCE(hotelsearch_events,0.0)) as hotel_searches_adjust,
sum(COALESCE(flightsearch_events,0.0)) as flight_searches_adjust,
'adjust' as source
From
tajawal_bi.fact_adjust_data_multi_app_final A
    inner join
    (
        select lower(regexp_replace(A.adjust_network_name, ' ', '')) as adjust_network_name, 
        max(B.channel_group) as channel_group, max(B.channel) as channel, count(*) 
        from tajawal_bi.dim_spend_adjust_network_channels A inner join tajawal_bi.dim_spend_channels B 
        on A.dim_channel_id = B.dim_channel_id 
        where lower(B.channel) like '%facebook%paid%' or lower(B.channel) like '%instagram%paid%' 
        group by lower(regexp_replace(A.adjust_network_name, ' ', ''))
    ) B on lower(regexp_replace(A.network, ' ', '')) = lower(regexp_replace(B.adjust_network_name, ' ', ''))
Group by
ltrim(rtrim(regexp_replace(split(campaign,' ')[1],'\\(|\\)',''))),
ltrim(rtrim(regexp_replace(split(adgroup,' ')[1],'\\(|\\)',''))),
ltrim(rtrim(regexp_replace(split(creative,' ')[1],'\\(|\\)',''))),
`date`) B on B.campaign_id=ltrim(rtrim(A.campaign_id)) and B.adgroup_id=ltrim(rtrim(A.adset_id)) and B.ad_id=ltrim(rtrim(A.ad_id)) and A.date_start=B.`date`
;



Drop table if exists tajawal_bi.inter2_fact_facebook_consolidated;
Create table  tajawal_bi.inter2_fact_facebook_consolidated ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  as
select
'na' as campaign_id,
B.campaign,
'na' as adset_id,
'na' as adset_name,
'na' as ad_id,
'na' as ad_name,
B.gadate as date_start,
'na' as account_id,
'na' as account_name,
0 impressions,
0 clicks,
0 deeplink_clicks,
0 inline_link_clicks,
0 spend,
0 website_clicks,
'na' fb_in_flag,
B.product,
B.pos,
B.mobile_os,
B.language,
'web' as device,
B.sub_channel,
B.channel,
B.dim_channel_id,
B.group_name as groupname,
B.installs,
B.bookings,
B.revenue,
B.sessions,
B.hotel_searches,
B.flight_searches,
B.source
from
(
Select
max(campaign_id) as campaign_id,
'all' as adset_id,
'all' as ad_id,
date_start,
max(account_id) as account_id,
max(account_name) as account_name,
max(ad_name) as ad_name,
max(adset_name) as adset_name,
ltrim(rtrim(lower(campaign_name))) as campaign_name,
COALESCE(sum(impressions),0.0) as impressions,
COALESCE(sum(clicks),0.0) as clicks,
COALESCE(sum(deeplink_clicks),0.0) as deeplink_clicks,
COALESCE(sum(inline_link_clicks),0.0) as inline_link_clicks,
COALESCE(sum(spend),0.0) as spend,
COALESCE(sum(website_clicks),0.0) as website_clicks,
max(fb_in_flag) as fb_in_flag,
max(product) as product,
max(pos) as pos,
max(mobile_os) as mobile_os,
max(language) as language,
max(device) as device,
max(sub_channel) as sub_channel,
max(channel) as channel,
max(dim_channel_id) as dim_channel_id,
max(groupname) as groupname
From tajawal_bi.fact_facebook_spend Group By
ltrim(rtrim(lower(campaign_name))),date_start
 )A
right outer join
(
    Select
    gadate,
    ltrim(rtrim(lower(campaign))) as campaign,
    max(B.channel_group) as channel,
    max(B.channel) as sub_channel,
    max(B.dim_channel_id) as dim_channel_id,
    max(A.product) as product,
    max(A.pos) as pos,
    max(A.language) as language,
    max(A.mobile_os) as mobile_os,
    max(gv.group_name) as group_name,
    0.0 as installs,
    COALESCE(sum(transactions),0.0) as bookings,
    COALESCE(sum(transactions_revenue),0.0) as revenue,
    COALESCE(sum(sessions),0.0) as sessions,
    0.0 as hotel_searches,
    0.0 as flight_searches,
    'GA' as source
    from tajawal_bi.fact_google_analytics_ad A 
    inner join tajawal_bi.dim_spend_ga_views gv on A.view_id = gv.dim_view_id
    inner join
        (
            select lower(regexp_replace(A.ga_sourcemedium, ' ', '')) as ga_sourcemedium, 
            max(B.channel_group) as channel_group, max(B.channel) as channel, max(B.dim_channel_id) as dim_channel_id, count(*) 
            from tajawal_bi.dim_spend_ga_sourcemedium_channels A inner join tajawal_bi.dim_spend_channels B 
            on A.dim_channel_id = B.dim_channel_id 
            where lower(B.channel) like '%facebook%paid%' or lower(B.channel) like '%instagram%paid%' 
            group by lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
        ) B  
        on lower(regexp_replace(regexp_replace(A.sourcemedium, ' ', ''),'\(|\)','')) = lower(regexp_replace(B.ga_sourcemedium, ' ', ''))
    Where view_id in ('130709589', '109503410') 
    -- and B.campaign_name is null
    Group by
    gadate,
    ltrim(rtrim(lower(campaign)))
)B on ltrim(rtrim(lower(A.campaign_name)))=ltrim(rtrim(lower(B.campaign)))  and A.date_start=B.gadate
where A.campaign_name is null;



-- == Include records which are only in Adjust and not in Facebook
drop table if exists tajawal_bi.inter5_fact_facebook_consolidated;
create table if not exists tajawal_bi.inter5_fact_facebook_consolidated as
select
B.campaign_id,
COALESCE(C.campaign_name, B.adjust_campaign_name) as campaign_name,
B.adset_id,
COALESCE(C.adset_name, 'na') as adset_name,
B.ad_id,
COALESCE(C.ad_name, 'na') as ad_name,
B.date_start,
COALESCE(C.account_id, 'na') as account_id,
COALESCE(C.account_name, 'na') as account_name,
0 as impressions,
0 as clicks,
0 as deeplink_clicks,
0 as inline_link_clicks,
0 as spend,
0 as website_clicks,
COALESCE(C.fb_in_flag, 'na') as fb_in_flag,
COALESCE(COALESCE(B.product, C.product), 'na') as product,
COALESCE(COALESCE(B.pos, C.pos), 'na') as pos,
COALESCE(COALESCE(B.mobile_os, C.mobile_os), 'na') as mobile_os,
COALESCE(COALESCE(B.language, C.language), 'na') as language,
'app' as device,
COALESCE(B.channel, C.sub_channel) as sub_channel,
COALESCE(B.channel_group, C.channel) as channel,
COALESCE(C.dim_channel_id, 'na') as dim_channel_id,
COALESCE(B.group_name, 'na') as groupname,
B.installs,
B.bookings,
B.revenue,
B.sessions,
B.hotel_searches_adjust,
B.flight_searches_adjust,
case when C.campaign_name is null then 'adj_exclusive' else 'adj_only' end as source
from
(
    Select
    campaign_id,
    adset_id,
    ad_id,
    date_start,
    count(*) as total
    From tajawal_bi.fact_facebook_spend Group By
    campaign_id, adset_id, ad_id, date_start
) A right outer join (
    Select
    ltrim(rtrim(regexp_replace(split(campaign,' ')[1],'\\(|\\)',''))) as campaign_id,
    ltrim(rtrim(regexp_replace(split(adgroup,' ')[1],'\\(|\\)',''))) as adset_id,
    ltrim(rtrim(regexp_replace(split(creative,' ')[1],'\\(|\\)',''))) as ad_id,
    `date` as date_start,
   B.channel_group as channel_group,
   B.channel as channel,
   A.pos,
   A.product,
   A.mobile_os,
   A.language,
   case when lower(A.app_name) like '%tajawal%' then 'tajawal' else case when lower(A.app_name) like '%almosafer%' then 'almosafer' else 'na' end end as group_name,
    max(split(campaign, ' ')[0]) as adjust_campaign_name,
    sum(COALESCE(installs,0.0)) as installs,
    sum(COALESCE(revenue_events,0.0)) as bookings,
    sum(COALESCE(revenue,0.0)) as revenue,
    sum(COALESCE(sessions,0.0)) as sessions,
    sum(COALESCE(hotelsearch_events,0.0)) as hotel_searches_adjust,
    sum(COALESCE(flightsearch_events,0.0)) as flight_searches_adjust,
    'adjust' as source
    From
    tajawal_bi.fact_adjust_data_multi_app_final A
    inner join
    (
        select lower(regexp_replace(A.adjust_network_name, ' ', '')) as adjust_network_name, 
        max(B.channel_group) as channel_group, max(B.channel) as channel, count(*) 
        from tajawal_bi.dim_spend_adjust_network_channels A inner join tajawal_bi.dim_spend_channels B 
        on A.dim_channel_id = B.dim_channel_id 
        where lower(B.channel) like '%facebook%paid%' or lower(B.channel) like '%instagram%paid%' 
        group by lower(regexp_replace(A.adjust_network_name, ' ', ''))
    ) B on lower(regexp_replace(A.network, ' ', '')) = lower(regexp_replace(B.adjust_network_name, ' ', ''))
     -- where campaign like "%fb%" or  campaign like "%\_in\_%" or campaign like "%facebook%" or campaign like "%\_insta\_%"
    Group by
    ltrim(rtrim(regexp_replace(split(campaign,' ')[1],'\\(|\\)',''))),
    ltrim(rtrim(regexp_replace(split(adgroup,' ')[1],'\\(|\\)',''))),
    ltrim(rtrim(regexp_replace(split(creative,' ')[1],'\\(|\\)',''))),
    `date`,
    B.channel_group, B.channel, A.pos,
   A.product,
   A.mobile_os,
   A.language,
   case when lower(A.app_name) like '%tajawal%' then 'tajawal' else case when lower(A.app_name) like '%almosafer%' then 'almosafer' else 'na' end end
) B on 
B.campaign_id=ltrim(rtrim(A.campaign_id)) and 
B.adset_id=ltrim(rtrim(A.adset_id)) and 
B.ad_id=ltrim(rtrim(A.ad_id)) and A.date_start=B.date_start
left outer join 
( 
    select
    campaign_id,
    adset_id,
    ad_id,
    max(account_id) as account_id,
    max(account_name) as account_name,
    max(ad_name) as ad_name,
    max(adset_name) as adset_name,
    max(campaign_name) as campaign_name,
    max(fb_in_flag) as fb_in_flag,
    max(product) as product,
    max(pos) as pos,
    max(mobile_os) as mobile_os,
    max(language) as language,
    max(device) as device,
    max(sub_channel) as sub_channel,
    max(channel) as channel,
    max(dim_channel_id) as dim_channel_id,
    max(groupname) as groupname
    From tajawal_bi.fact_facebook_spend Group By
    campaign_id, adset_id, ad_id
) C on B.campaign_id = C.campaign_id and B.adset_id = C.adset_id and B.ad_id = C.ad_id
where A.campaign_id is null or A.adset_id is null or A.ad_id is null or A.date_start is null
;


-- ====Final table creation with campaigns null in adjust and GA

-- ==Create Final table

Drop table if exists tajawal_bi.tmp_fact_facebook_consolidated;
Create table  tajawal_bi.tmp_fact_facebook_consolidated ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  as
select * from tajawal_bi.inter1_fact_facebook_consolidated
union
select * from tajawal_bi.inter2_fact_facebook_consolidated
union
select * from tajawal_bi.inter5_fact_facebook_consolidated;

Drop table if exists tajawal_bi.fact_facebook_consolidated;
Alter table tajawal_bi.tmp_fact_facebook_consolidated rename to tajawal_bi.fact_facebook_consolidated;

-- Change channel names to lower case removing spaces
Drop table if exists tajawal_bi.fact_facebook_consolidated_1;
Create table tajawal_bi.fact_facebook_consolidated_1 like tajawal_bi.fact_facebook_consolidated;
insert into tajawal_bi.fact_facebook_consolidated_1
select
campaign_id,
campaign_name,
adset_id,
adset_name,
ad_id,
ad_name,
date_start,
account_id,
account_name,
impressions,
clicks,
deeplink_clicks,
inline_link_clicks,
spend,
website_clicks,
fb_in_flag,
(
case when ((lower(ad_name) like '%hotel%' or lower(ad_name) like '%hotels%') and (lower(ad_name) like '%flight%' or lower(ad_name) like '%flights%' )) or lower(ad_name) like '%both%'
         then 'both'
         when (ad_name like '%hotel%' or ad_name like '%flight%' or upper(ad_name) like '%-HO-%' or upper(ad_name) like '%\_HL\_%' or upper(ad_name) like '%-FL-%' or upper(ad_name) like '%\_FL\_%') and (lower(ad_name) like '%package%' or lower(ad_name) like '%\_p\_%')
     then 'both'
     when lower(ad_name) like '%flight%' or lower(ad_name) like '%flights%' or upper(ad_name) like '%\_FL\_%'
     then 'flight'
     when lower(ad_name) like '%hotel%' or lower(ad_name) like '%hotels%' or upper(ad_name) like '%\_HO\_%'
     then 'hotel'
     when lower(ad_name) like '%package%' or lower(ad_name) like '%\_p\_%'
         then 'package'
     else 'NA'
     end) as product,
pos,
mobile_os,
language,
device,
lower(ltrim(rtrim(sub_channel))),
lower(ltrim(rtrim(channel))),
dim_channel_id,
groupname,
installs,
bookings,
revenue,
sessions,
hotel_searches_adjust,
flight_searches_adjust,
source
from tajawal_bi.fact_facebook_consolidated;

drop table tajawal_bi.fact_facebook_consolidated;
alter table tajawal_bi.fact_facebook_consolidated_1 rename to tajawal_bi.fact_facebook_consolidated;


Drop table if exists tajawal_bi.fact_facebook_consolidated_2;

Create table tajawal_bi.fact_facebook_consolidated_2 like tajawal_bi.fact_facebook_consolidated;
insert into tajawal_bi.fact_facebook_consolidated_2
select
campaign_id,
campaign_name,
adset_id,
adset_name,
ad_id,
ad_name,
date_start,
account_id,
account_name,
sum(impressions) as impressions,
sum(clicks) as clicks,
sum(deeplink_clicks) as deeplink_clicks,
sum(inline_link_clicks) as inline_link_clicks,
sum(spend) as spend,
sum(website_clicks) as website_clicks,
fb_in_flag,
product,
pos,
mobile_os,
language,
device,
sub_channel,
channel,
dim_channel_id,
groupname,
sum(installs) as installs,
sum(bookings) as bookimgs,
sum(revenue) as revenue,
sum(sessions) as sessions,
sum(hotel_searches_adjust) as hotel_searches_adjust,
sum(flight_searches_adjust) as flight_searches_adjust,
source
from tajawal_bi.fact_facebook_consolidated
group by 
campaign_id,
campaign_name,
adset_id,
adset_name,
ad_id,
ad_name,
date_start,
account_id,
account_name,
fb_in_flag,
product,
pos,
mobile_os,
language,
device,
sub_channel,
channel,
dim_channel_id,
groupname,
source
;

drop table tajawal_bi.fact_facebook_consolidated;
alter table tajawal_bi.fact_facebook_consolidated_2 rename to tajawal_bi.fact_facebook_consolidated;


