truncate table ${hiveDB}.fact_twitter_data;

insert into ${hiveDB}.fact_twitter_data
(accountid,
accountname,
consumerkey,
campaignid,
campaign_name,
day_date,
start_date,end_date,
daily_budget,
total_budget,
currency,
spend,impressions,engagements,billed_engagements,retweets,replies,follows,clicks,media_engagements,likes,engagement_rate,link_clicks,app_clicks,cost_per_engagement,cost_per_follow,cost_per_link_click,
card_engagements,
tweets_send,
qualified_impressions,
video_views_25,
video_views_75,
video_views_100,
video_total_views,
video_3s100pct_views,
video_cta_clicks,
video_content_starts,
video_mrc_views,
media_views,
product,
pos,
mobile_os,
language,
sub_channel,
channel,
dim_channel_id,
device,
installs,
bookings,
revenue,
sessions,
hotel_searches_adjust,
flight_searches_adjust,
group_name,
brand,
objective,
funding_source
)
select
accountid,
accountname,
consumerkey,
campaignid,
campaignname,
from_unixtime(unix_timestamp(cdate, 'yyyy-MM-dd'), 'yyyyMMdd'),
from_unixtime(unix_timestamp(startdate, 'yyyy-MM-dd'), 'yyyyMMdd') startdate,
CASE WHEN enddate='null' THEN 'NA' ELSE from_unixtime(unix_timestamp(enddate, 'yyyy-MM-dd'), 'yyyyMMdd')  END,
CASE WHEN daily_budget_amount_local_micro='null' THEN 0 ELSE round(daily_budget_amount_local_micro/1000000,3) END,
CASE WHEN total_budget_amount_local_micro='null' THEN 0 ELSE round(total_budget_amount_local_micro/1000000,3) END,
currency,
CASE WHEN spend='null' THEN 0 ELSE round(spend/1000000,6) END,
CASE WHEN impressions='null' THEN 0 ELSE impressions END,
CASE WHEN engagements='null' THEN 0 ELSE engagements END,
CASE WHEN billed_engagements='null' THEN 0 ELSE billed_engagements END,
CASE WHEN retweets='null' THEN 0 ELSE retweets END,
CASE WHEN replies='null' THEN 0 ELSE replies END,
CASE WHEN follows='null' THEN 0 ELSE follows END,
CASE WHEN clicks='null' THEN 0 ELSE clicks END,
CASE WHEN media_engagements='null' THEN 0 ELSE media_engagements END,
CASE WHEN likes='null' THEN 0 ELSE likes END,
CASE WHEN(engagements / impressions) IS null THEN 0 ELSE engagements / impressions END,
CASE WHEN url_clicks='null' THEN 0 ELSE url_clicks END,
CASE WHEN app_clicks='null' THEN 0 ELSE app_clicks END,
CASE WHEN(spend / engagements) IS null THEN 0 ELSE round(spend/1000000,6) / engagements END,
CASE WHEN(spend / follows) IS null THEN 0 ELSE round(spend/1000000,6) / follows END,
CASE WHEN(spend / url_clicks) IS null THEN 0 ELSE round(spend/1000000,6) / url_clicks END,
CASE WHEN card_engagements='null' THEN 0 ELSE card_engagements END,
CASE WHEN tweets_send='null' THEN 0 ELSE tweets_send END,
CASE WHEN qualified_impressions='null' THEN 0 ELSE qualified_impressions END,
CASE WHEN video_views_25='null' THEN 0 ELSE video_views_25 END,
CASE WHEN video_views_75='null' THEN 0 ELSE video_views_75 END,
CASE WHEN video_views_100='null' THEN 0 ELSE video_views_100 END,
CASE WHEN video_total_views='null' THEN 0 ELSE video_total_views END,
CASE WHEN video_3s100pct_views='null' THEN 0 ELSE video_3s100pct_views END,
CASE WHEN video_cta_clicks='null' THEN 0 ELSE video_cta_clicks END,
CASE WHEN video_content_starts='null' THEN 0 ELSE video_content_starts END,
CASE WHEN video_mrc_views='null' THEN 0 ELSE video_mrc_views END,
CASE WHEN media_views='null' THEN 0 ELSE media_views END,
CASE WHEN (lower(campaignname) like '%hotel%' 
                   or lower(campaignname) like '%flight%' 
                   or upper(campaignname) like '%-HO-%' 
                   or upper(campaignname) like '%\_HL\_%' 
                   or upper(campaignname) like '%-FL-%' 
                   or upper(campaignname) like '%\_FL\_%') 
             and (lower(campaignname) like '%package%' 
                   or lower(campaignname) like '%\_p\_%') then 'both' 
 else CASE WHEN lower(campaignname) like '%hotel%' and  lower(campaignname) like '%flight%' then 'both' 
 else CASE WHEN lower(campaignname) like '%package%'  or lower(campaignname) like '%\_p\_%' THEN 'package'
 else CASE WHEN lower(campaignname) like '%flight%' or upper(campaignname) like '%-FL-%' or upper(campaignname) like '%\_FL\_%' then 'flight' 
 else CASE WHEN lower(campaignname) like '%hotel%' or upper(campaignname) like '%-HO-%' or upper(campaignname) like '%\_HL\_%' then 'hotel' 
 else CASE WHEN lower(campaignname) like '%both%' or lower(campaignname) like '%\_f_h\_%' or lower(campaignname) like '%\fh\_%'  then 'both' 
 else 'NA' 
 end end end end end end as PRODUCT,
CASE WHEN lower(accountname) like '%ksa%' or lower(accountname) like '%al mosafer travel - ads%'  then 'Saudi Arabia' else CASE WHEN lower(accountname) like '%uae%' then 'UAE' else CASE WHEN lower(accountname) like '%kw%' or lower(accountname) like '%kuwait%' then 'Kuwait' else 'NA' end end end,
CASE WHEN lower(campaignname) like '%android%' then 'android' else CASE WHEN lower(campaignname) like '%ios\_%' or lower(campaignname) like '%\_ios%' or lower(campaignname) like '%ios%' then 'ios' else 'NA' end end,
(case when lower(campaignname) like '%ar\_%' or lower(campaignname) like '%\_ar%' then 'ar' else case when lower(campaignname) like '%\_en\_%' then 'en' else 'NA' end end),
'twitter paid',
'social media',
md5(lower(regexp_replace(concat('social media', 'twitter paid'), ' ', ''))),
CASE WHEN lower(campaignname) like '%\_app%' or campaignname like '% App %' then 'app' else CASE WHEN lower(campaignname) like '%\_web%' then 'web' else 'app' end end,
CASE WHEN ad.installs is null THEN 0 ELSE ad.installs END,
CASE WHEN ad.bookings is null THEN 0 ELSE ad.bookings END,
CASE WHEN ad.revenue is null THEN 0 ELSE ad.revenue END,
CASE WHEN ad.sessions is null THEN 0 ELSE ad.sessions END,  
CASE WHEN ad.hotel_searches_adjust is null THEN 0 ELSE ad.hotel_searches_adjust END, 
CASE WHEN ad.flight_searches_adjust is null THEN 0 ELSE ad.flight_searches_adjust END,
CASE WHEN lower(accountname) like '%tajawal%' then 'tajawal' else CASE WHEN lower(accountname) like '%mosafer%' then 'almosafer' else 'NA' end end,
'Non Brand',
CASE WHEN lower(campaignname) like '%nc%' then 'Aquisition' ELSE 'Retargeting' END,
CASE WHEN funding_source is NULL THEN 'NA' ELSE ltrim(rtrim(funding_source)) END
from ${hiveDB}.twitter_data tw left outer join
(
select campaign, `date`, sum(installs) as installs,sum(bookings) as bookings,sum(revenue) as revenue,sum(sessions) as sessions,sum(hotel_searches_adjust) as hotel_searches_adjust,sum(flight_searches_adjust) as flight_searches_adjust from
(
select regexp_replace(lOWER(LTRIM(RTRIM(split(campaign,' \\(')[1]))), '\\(|\\)|\\"', '') as campaign,`date`,sum(installs) as installs,
sum(revenue_events) as bookings,sum(revenue) as revenue,sum(sessions) as sessions,
sum(hotelsearch_events) as hotel_searches_adjust,sum(flightsearch_events) as flight_searches_adjust
from tajawal_bi.fact_adjust_data_multi_app_final 
Group by Campaign,`date`
union 
Select Bx.campaignid as campaign, A.gadate as `date`,
0 as installs,
sum(A.transactions) as bookings,
sum(A.transactions_revenue) as revenue,
sum(sessions) as sessions,
0 as hotel_searches_adjust,
0 as flight_searches_adjust
FROM tajawal_bi.fact_google_analytics_ad A inner join
( select campaignid, campaignname, count(*) from ${hiveDB}.twitter_data group by campaignid,campaignname ) Bx
on A.campaign = Bx.campaignname
where A.view_id in ('130709589', '109503410')
Group by Bx.campaignid, A.gadate
) b
group by b.campaign,b.`date`
) ad 
on lOWER(tw.campaignId)=lOWER(ad.campaign) and regexp_replace(tw.cdate, '-', '') = ad.`date`;


insert into ${hiveDB}.fact_twitter_data
(accountid,
accountname,
consumerkey,
campaignid,
campaign_name,
day_date,
start_date,end_date,
daily_budget,
total_budget,
currency,
spend,impressions,engagements,billed_engagements,retweets,replies,follows,clicks,media_engagements,likes,engagement_rate,link_clicks,app_clicks,cost_per_engagement,cost_per_follow,cost_per_link_click,
card_engagements,
tweets_send,
qualified_impressions,
video_views_25,
video_views_75,
video_views_100,
video_total_views,
video_3s100pct_views,
video_cta_clicks,
video_content_starts,
video_mrc_views,
media_views,
product,
pos,
mobile_os,
language,
sub_channel,
channel,
dim_channel_id,
device,
installs,
bookings,
revenue,
sessions,
hotel_searches_adjust,
flight_searches_adjust,
group_name,
brand,
objective,
funding_source
)
-- drop table if exists tajawal_uat_bi.tmp_tmp91;
-- create table if not exists tajawal_uat_bi.tmp_tmp91 as
select
CASE WHEN C.accountid is null THEN 'NA' ELSE C.accountid END,
CASE WHEN C.accountname is null THEN 'NA' ELSE C.accountname END,
CASE WHEN C.consumerkey is null THEN 'NA' ELSE C.consumerkey END,
CASE WHEN ad.campaign is null or ad.campaign='na' THEN 'NA' ELSE ad.campaign END as campaignid,
COALESCE(C.campaign_name, ad.campaignname) as campaign_name,
ad.`date` as day_date,
CASE WHEN C.start_date is null THEN 'NA' ELSE C.start_date END,
CASE WHEN C.end_date is null THEN 'NA' ELSE C.end_date END,
COALESCE(C.daily_budget,0.0),
COALESCE(C.total_budget,0.0),
CASE WHEN C.currency is null THEN 'NA' ELSE C.currency END,
0 as spend,
0 as impressions,
0 as engagements,
0 as billed_engagements,
0 as retweets,
0 as replies,
0 as follows,
0 as clicks,
0 as media_engagements,
0 as likes,
0 as engagement_rate,
0 as link_clicks,
0 as app_clicks,
0 as cost_per_engagement,
0 as cost_per_follow,
0 as cost_per_link_click,
0 as card_engagements,
0 as tweets_send,
0 as qualified_impressions,
0 as video_views_25,
0 as video_views_75,
0 as video_views_100,
0 as video_total_views,
0 as video_3s100pct_views,
0 as video_cta_clicks,
0 as video_content_starts,
0 as video_mrc_views,
0 as media_views,
CASE WHEN (lower(COALESCE(C.campaign_name, ad.campaignname)) like '%hotel%' 
                   or lower(COALESCE(C.campaign_name, ad.campaignname)) like '%flight%' 
                   or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%-HO-%' 
                   or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%\_HL\_%' 
                   or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%-FL-%' 
                   or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%\_FL\_%') 
             and (lower(COALESCE(C.campaign_name, ad.campaignname)) like '%package%' 
                   or lower(COALESCE(C.campaign_name, ad.campaignname)) like '%\_p\_%') then 'both' 
 else CASE WHEN lower(COALESCE(C.campaign_name, ad.campaignname)) like '%hotel%' and  lower(COALESCE(C.campaign_name, ad.campaignname)) like '%flight%' then 'both' 
 else CASE WHEN lower(COALESCE(C.campaign_name, ad.campaignname)) like '%package%'  or lower(COALESCE(C.campaign_name, ad.campaignname)) like '%\_p\_%' THEN 'package'
 else CASE WHEN lower(COALESCE(C.campaign_name, ad.campaignname)) like '%flight%' or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%-FL-%' or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%\_FL\_%' then 'flight' 
 else CASE WHEN lower(COALESCE(C.campaign_name, ad.campaignname)) like '%hotel%' or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%-HO-%' or upper(COALESCE(C.campaign_name, ad.campaignname)) like '%\_HL\_%' then 'hotel' 
 else CASE WHEN lower(COALESCE(C.campaign_name, ad.campaignname)) like '%both%' or lower(COALESCE(C.campaign_name, ad.campaignname)) like '%\_f_h\_%' or lower(COALESCE(C.campaign_name, ad.campaignname)) like '%\fh\_%'  then 'both' 
 else 'NA' 
 end end end end end end as PRODUCT,
ad.pos,
ad.mobile_os,
ad.language,
'twitter paid' as sub_channel,
'social media' as channel,
-- COALESCE(C.sub_channel, ad.channel) as sub_channel,
-- COALESCE(C.channel, ad.channel_group) as channel,
md5(lower(regexp_replace(concat('social media', 'twitter paid'), ' ', ''))) as dim_channel_id,
-- CASE WHEN C.dim_channel_id is null THEN 'NA' ELSE C.dim_channel_id END,
ad.device,
CASE WHEN ad.installs is null THEN 0 ELSE ad.installs END,
CASE WHEN ad.bookings is null THEN 0 ELSE ad.bookings END,
CASE WHEN ad.revenue is null THEN 0 ELSE ad.revenue END,
CASE WHEN ad.sessions is null THEN 0 ELSE ad.sessions END,  
CASE WHEN ad.hotel_searches_adjust is null THEN 0 ELSE ad.hotel_searches_adjust END, 
CASE WHEN ad.flight_searches_adjust is null THEN 0 ELSE ad.flight_searches_adjust END,
ad.group_name,
'Non Brand',
CASE WHEN lower(COALESCE(C.campaign_name, ad.campaignname)) like '%nc%' then 'Aquisition' ELSE 'Retargeting' END,
CASE WHEN C.funding_source is null THEN 'NA' ELSE C.funding_source END
from ${hiveDB}.fact_twitter_data tw right outer join
(
    select 
    campaign, 
    `date`,
    product,
    pos,
    mobile_os,
    language,
    group_name,
    device,
    max(campaign_name) as campaignname, 
    sum(installs) as installs,
    sum(bookings) as bookings,
    sum(revenue) as revenue,
    sum(sessions) as sessions,
    sum(hotel_searches_adjust) as hotel_searches_adjust,
    sum(flight_searches_adjust) as flight_searches_adjust,
    max(channel_group) as channel_group,
    max(channel) as channel
    from
    (
        select regexp_replace(lOWER(LTRIM(RTRIM(split(A.campaign,' \\(')[1]))), '\\(|\\)|\\"', '') as campaign,
        A.`date`,
        A.product,
        A.pos,
        A.mobile_os,
        A.language,
        case when lower(A.app_name) like '%tajawal%' then 'tajawal' else
            case when lower(A.app_name) like '%almosafer%' then 'almosafer' else 'na' end
        end as group_name,
        'app' as device,
        sum(A.installs) as installs,
        sum(A.revenue_events) as bookings,sum(A.revenue) as revenue,sum(A.sessions) as sessions,
        sum(A.hotelsearch_events) as hotel_searches_adjust,sum(A.flightsearch_events) as flight_searches_adjust,
        max(B.channel_group) as channel_group, max(B.channel) as channel, max(campaign) as campaign_name
        from tajawal_bi.fact_adjust_data_multi_app_final A
        inner join
        (
            select lower(regexp_replace(A.adjust_network_name, ' ', '')) as adjust_network_name, 
            max(B.channel_group) as channel_group, max(B.channel) as channel, count(*) 
            from tajawal_bi.dim_spend_adjust_network_channels A inner join tajawal_bi.dim_spend_channels B 
            on A.dim_channel_id = B.dim_channel_id 
            where lower(B.channel) like '%twitter%paid%'
            group by lower(regexp_replace(A.adjust_network_name, ' ', ''))
        ) B on lower(regexp_replace(A.network, ' ', '')) = lower(regexp_replace(B.adjust_network_name, ' ', ''))
        Group by Campaign,`date`,A.product,
        A.pos,
        A.mobile_os,
        A.language,
        case when lower(A.app_name) like '%tajawal%' then 'tajawal' else
            case when lower(A.app_name) like '%almosafer%' then 'almosafer' else 'na' end
        end

        union

        Select coalesce(Bx.campaignid, 'na') as campaign, A.gadate as `date`,
        A.product,
        A.pos,
        A.mobile_os,
        A.language,
        C.group_name,
        'web' as device,
        0 as installs,
        sum(A.transactions) as bookings,
        sum(A.transactions_revenue) as revenue,
        sum(sessions) as sessions,
        0 as hotel_searches_adjust,
        0 as flight_searches_adjust, max(B.channel_group) as channel_group, max(B.channel) as channel,
        max(campaign) as campaign_name
        FROM 
        tajawal_bi.fact_google_analytics_ad A 
        inner join
        (
            select
            lower(regexp_replace(A.ga_sourcemedium, ' ', '')) as ga_sourcemedium, 
            max(B.channel_group) as channel_group, max(B.channel) as channel, count(*)
            from tajawal_bi.dim_spend_ga_sourcemedium_channels A inner join tajawal_bi.dim_spend_channels B
            on A.dim_channel_id = B.dim_channel_id
            where lower(B.channel) like '%twitter%paid%'
            group by lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
        ) B on lower(regexp_replace(A.sourcemedium, ' ', '')) = lower(regexp_replace(B.ga_sourcemedium, ' ', ''))
        left outer join
        ( 
           select max(campaignid) as campaignid, campaignname, count(*) 
           from tajawal_uat_bi.twitter_data 
           group by campaignname 
        ) Bx
        on A.campaign = Bx.campaignname
        inner join tajawal_bi.dim_spend_ga_views C on A.view_id = C.dim_view_id
        where A.view_id in ('130709589', '109503410')
        Group by Bx.campaignid, A.gadate,
        A.product,
        A.pos,
        A.mobile_os,
        A.language,
        C.group_name
    ) b
    group by b.campaign,b.`date`,
    product,
    pos,
    mobile_os,
    language,
    group_name,
    device
) ad
on lOWER(tw.campaignId)=lOWER(ad.campaign) and regexp_replace(tw.day_date, '-', '') = ad.`date`
left outer join (
    select
    campaignid,
    max(accountid) as accountid,
    max(accountname) as accountname,
    max(consumerkey) as consumerkey,
    max(campaign_name) as campaign_name,
    max(day_date) as day_date,
    max(start_date) as start_date,
    max(end_date) as end_date,
    max(daily_budget) as daily_budget,
    max(total_budget) as total_budget,
    max(currency) as currency,
    max(product) as product,
    max(pos) as pos,
    max(mobile_os) as mobile_os,
    max(language) as language,
    max(sub_channel) as sub_channel,
    max(channel) as channel,
    max(dim_channel_id) as dim_channel_id,
    max(device) as device,
    max(group_name) as group_name,
    max(funding_source) as funding_source
    from ${hiveDB}.fact_twitter_data
    group by campaignid
) C
on lower(ad.campaign) = lower(C.campaignid)
where tw.campaignid is null;
