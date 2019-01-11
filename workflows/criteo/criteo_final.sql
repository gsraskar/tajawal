truncate table ${hiveDB}.fact_criteo_data;

insert into ${hiveDB}.fact_criteo_data (
accountname, 
campaignid, 
campaignname, 
campaignenddate, 
daydate,
cost, 
impressions, 
click, 
sales,
costofsale, 
ordervalue, 
datetimeposix, 
convrate, 
ctr, 
revcpc, 
salespostview, 
convratepostview, 
ordervaluepostview, 
overallcompetitionwin, 
costperorder, 
ecpm, 
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
group_name  
)
select
accountName, 
campaignId, 
campaignname, 
camPaignEndDate, 
from_unixtime(unix_timestamp(dayDate, 'yyyy-MM-dd'), 'yyyyMMdd'),
CASE WHEN cost='null' THEN 0 ELSE cost END,  
CASE WHEN impressions='null' THEN 0 ELSE impressions END, 
CASE WHEN click='null' THEN 0 ELSE click END,  
CASE WHEN sales='null' THEN 0 ELSE sales END, 
CASE WHEN costOfSale='null' THEN 0 ELSE costOfSale END,  
CASE WHEN orderValue='null' THEN 0 ELSE orderValue END,         
CASE WHEN dateTimePosix='null' THEN 0 ELSE dateTimePosix END,                  
CASE WHEN convRate='null' THEN 0 ELSE convRate END,                            
CASE WHEN CTR='null' THEN 0 ELSE CTR END,                            
CASE WHEN revcpc='null' THEN 0 ELSE revcpc END,                            
CASE WHEN salesPostView='null' THEN 0 ELSE salesPostView END,                  
CASE WHEN convRatePostView='null' THEN 0 ELSE convRatePostView END,            
CASE WHEN orderValuePostView='null' THEN 0 ELSE orderValuePostView END,        
CASE WHEN overallCompetitionWin='null' THEN 0 ELSE overallCompetitionWin END,  
CASE WHEN costPerOrder='null' THEN 0 ELSE costPerOrder END,                    
CASE WHEN ecpm='null' THEN 0 ELSE ecpm END,                            
case WHEN lower(campaignname) like '%package%' or lower(campaignname) like '%\_p\_%' THEN 'package' else CASE WHEN campaignname like '%hotel%' and  campaignname like '%flight%' then 'both' else CASE WHEN lower(campaignname) like '%flight%' or upper(campaignname) like '%-FL-%' or upper(campaignname) like '%\_FL\_%' then 'flight' else CASE WHEN lower(campaignname) like '%hotel%' or upper(campaignname) like '%-HO-%' or upper(campaignname) like '%\_HL\_%' then 'hotel' else CASE WHEN lower(campaignname) like '%both%' or lower(campaignname) like '%\_f_h\_%' or lower(campaignname) like '%\fh\_%'  then 'both' else 'NA' end end end end end,
CASE WHEN lower(accountName) like '%ksa%' then 'Saudi Arabia' else CASE WHEN lower(accountName) like '%uae%' then 'UAE' else CASE WHEN lower(accountName) like '%kw%' then 'Kuwait' else 'NA' end end end,
CASE WHEN lower(campaignname) like '%android%' then 'android' else CASE WHEN lower(campaignname) like '%ios\_%' or lower(campaignname) like '%\_ios%' or lower(campaignname) like '%ios%' then 'ios' else 'NA' end end,
(case when lower(campaignname) like '%ar\_%' or lower(campaignname) like '%\_ar%' or lower(campaignname) like '%\-ar%' then 'ar' else case when lower(campaignname) like '%\_en\_%' or lower(campaignname) like '%\-en%' then 'en' else 'NA' end end),
'Criteo',
'Retargeting',
md5(lower(regexp_replace(concat('Retargeting', 'Criteo'), ' ', ''))),
CASE WHEN lower(campaignname) like '%inapp%' or lower(campaignname) like '%\_app%' then 'app'  else CASE WHEN lower(campaignname) like '%lowerfunnel%' or lower(campaignname) like '%\_web%' then 'web' else coalesce(ad.device, 'NA') end end,
CASE WHEN ad.installs is null THEN 0 ELSE ad.installs END,
CASE WHEN ad.bookings is null THEN 0 ELSE ad.bookings END,
CASE WHEN ad.revenue is null THEN 0 ELSE ad.revenue END,
CASE WHEN ad.sessions is null THEN 0 ELSE ad.sessions END,  
CASE WHEN ad.hotel_searches_adjust is null THEN 0 ELSE ad.hotel_searches_adjust END, 
CASE WHEN ad.flight_searches_adjust is null THEN 0 ELSE ad.flight_searches_adjust END,
CASE WHEN lower(accountname) like '%tajawal%' then 'tajawal' else CASE WHEN lower(accountname) like '%mosafer%' then 'almosafer' else 'NA' end end

-- select substr(`date`, 1, 6) as month, sum(bookings), sum(revenue)

from ${hiveDB}.criteo_data cr left outer join 
(
    select group_name, lower(b.campaign) as campaign, `date`, sum(installs) as installs,sum(bookings) as bookings,sum(revenue) as revenue,sum(sessions) as sessions,sum(hotel_searches_adjust) as hotel_searches_adjust,sum(flight_searches_adjust) as flight_searches_adjust, max(device) as device
    
    -- select substr(`date`, 1, 6) as month, sum(bookings), sum(revenue)
    
    from
    (
        select aa.group_name, ltrim(rtrim(lower(cm2.criteo_campaign))) as campaign,ga.`date`,
        sum(ga.installs) as installs,
        sum(ga.revenue_events) as bookings,
        sum(ga.revenue) as revenue,
        sum(ga.sessions) as sessions,
        sum(ga.hotelsearch_events) as hotel_searches_adjust,
        sum(ga.flightsearch_events) as flight_searches_adjust
  	, 'app' as device
        
        -- select substr(`date`, 1, 6) as month, sum(revenue_events), sum(revenue)
        
        from 
        ${hiveDB}.fact_adjust_data_multi_app_final ga
        inner join ${hiveDB}.dim_spend_adjust_apps aa on ltrim(rtrim(lower(ga.app_token))) = ltrim(rtrim(lower(aa.dim_app_id)))
        inner join
        (
            select lower(regexp_replace(A.adjust_network_name, ' ', '')) as adjust_network_name, 
            max(B.channel_group) as channel_group, max(B.channel) as channel, count(*) 
            from ${hiveDB}.dim_spend_adjust_network_channels A inner join ${hiveDB}.dim_spend_channels B 
            on A.dim_channel_id = B.dim_channel_id 
            where lower(B.channel) like '%criteo%'
            group by lower(regexp_replace(A.adjust_network_name, ' ', ''))
        ) B on lower(regexp_replace(ga.network, ' ', '')) = lower(regexp_replace(B.adjust_network_name, ' ', ''))
        inner join 
        (select lower(ltrim(rtrim(ga_adjust_campaign))) as ga_adjust_campaign, max(criteo_campaign) as criteo_campaign from ${hiveDB}.criteo_campaign_mapping group by             lower(ltrim(rtrim(ga_adjust_campaign)))) cm2 
        on lOWER(LTRIM(RTRIM(split(ga.campaign,' ')[0])))=lower(ltrim(rtrim(cm2.ga_adjust_campaign)))
        Group by aa.group_name, ltrim(rtrim(lower(cm2.criteo_campaign))), ga.`date`
    
        union all
    
        Select gv.group_name, ltrim(rtrim(lower(cm.criteo_campaign))) as campaign,ad.gadate as `date`,
        0 as installs,
        sum(ad.transactions) as bookings,
        sum(ad.transactions_revenue) as revenue,
        sum(ad.sessions) as sessions,
        0 as hotel_searches_adjust,
        0 as flight_searches_adjust
	, 'web' as device
        
        -- select substr(gadate, 1, 6) as month, sum(transactions), sum(transactions_revenue)
        
        FROM ${hiveDB}.fact_google_analytics_ad ad
        inner join ${hiveDB}.dim_spend_ga_views gv on ad.view_id = gv.dim_view_id
        inner join
        (
            select
            lower(regexp_replace(A.ga_sourcemedium, ' ', '')) as ga_sourcemedium, 
            max(B.channel_group) as channel_group, max(B.channel) as channel, count(*)
            from ${hiveDB}.dim_spend_ga_sourcemedium_channels A inner join ${hiveDB}.dim_spend_channels B
            on A.dim_channel_id = B.dim_channel_id
            where lower(B.channel) like '%criteo%'
            group by lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
        ) B on lower(regexp_replace(ad.sourcemedium, ' ', '')) = lower(regexp_replace(B.ga_sourcemedium, ' ', ''))
        inner join 
        (select lower(ltrim(rtrim(ga_adjust_campaign))) as ga_adjust_campaign, max(criteo_campaign) as criteo_campaign from ${hiveDB}.criteo_campaign_mapping group by lower(ltrim(rtrim(ga_adjust_campaign)))) cm 
        on lower(ad.campaign)=lower(ltrim(rtrim(cm.ga_adjust_campaign)))
        where ad.view_id in ('130709589', '109503410')
        Group by gv.group_name, ltrim(rtrim(lower(cm.criteo_campaign))), ad.gadate 
    ) b
    group by group_name, lower(b.campaign),b.`date`
) ad 
on lOWER(cr.campaignname)=lOWER(ad.campaign) and regexp_replace(cr.daydate, '-', '') = ad.`date`
and CASE WHEN lower(accountname) like '%tajawal%' then 'tajawal' else CASE WHEN lower(accountname) like '%mosafer%' then 'almosafer' else 'NA' end end = ltrim(rtrim(ad.group_name))
-- group by substr(`date`, 5, 2)
;

insert into ${hiveDB}.fact_criteo_data (
accountname, 
campaignid, 
campaignname, 
campaignenddate, 
daydate,
cost, 
impressions, 
click, 
sales,
costofsale, 
ordervalue, 
datetimeposix, 
convrate, 
ctr, 
revcpc, 
salespostview, 
convratepostview, 
ordervaluepostview, 
overallcompetitionwin, 
costperorder, 
ecpm, 
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
group_name  
)
select
'NA' as accountname, 
'NA' as campaignid, 
COALESCE(ad.campaign,'NA') as campaignname,
'NA' as campaignenddate, 
ad.`date` as daydate,
0 as cost, 
0 as impressions, 
0 as click, 
0 as sales,
0 as costofsale, 
0 as ordervalue, 
0 as datetimeposix, 
0 as convrate, 
0 as ctr, 
0 as revcpc, 
0 as salespostview, 
0 as convratepostview, 
0 as ordervaluepostview, 
0 as overallcompetitionwin, 
0 as costperorder, 
0 as ecpm,
ad.product,
ad.pos,
ad.mobile_os,
ad.language,
ad.channel as sub_channel,
ad.channel_group as channel,
md5(lower(regexp_replace(concat('Retargeting', 'Criteo'), ' ', ''))) as dim_channel_id,
ad.device,
CASE WHEN ad.installs is null THEN 0 ELSE ad.installs END,
CASE WHEN ad.bookings is null THEN 0 ELSE ad.bookings END,
CASE WHEN ad.revenue is null THEN 0 ELSE ad.revenue END,
CASE WHEN ad.sessions is null THEN 0 ELSE ad.sessions END,  
CASE WHEN ad.hotel_searches_adjust is null THEN 0 ELSE ad.hotel_searches_adjust END, 
CASE WHEN ad.flight_searches_adjust is null THEN 0 ELSE ad.flight_searches_adjust END,
ad.group_name

--select substr(ad.`date`, 1, 6) as month, sum(ad.bookings), sum(ad.revenue)

from ${hiveDB}.fact_criteo_data cr right outer join
(
    select 
    campaign, 
    `date`,
    --max(product) as product,
    product,
    --max(pos) as pos,
    pos,
    --max(mobile_os) as mobile_os,
    mobile_os,
    --max(language) as language,
    language,
    --max(group_name) as group_name,
    group_name,
    --max(device) as device,
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
        select LOWER(LTRIM(RTRIM(split(A.campaign,' ')[0]))) as campaign,
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
        max(B.channel_group) as channel_group, max(B.channel) as channel, max(COALESCE(cm.criteo_campaign, LOWER(LTRIM(RTRIM(split(A.campaign,' ')[0]))))) as campaign_name
        from ${hiveDB}.fact_adjust_data_multi_app_final A
        inner join
        (
            select lower(regexp_replace(A.adjust_network_name, ' ', '')) as adjust_network_name, 
            max(B.channel_group) as channel_group, max(B.channel) as channel, count(*) 
            from ${hiveDB}.dim_spend_adjust_network_channels A inner join ${hiveDB}.dim_spend_channels B 
            on A.dim_channel_id = B.dim_channel_id 
            where lower(B.channel) like '%criteo%'
            group by lower(regexp_replace(A.adjust_network_name, ' ', ''))
        ) B on lower(regexp_replace(A.network, ' ', '')) = lower(regexp_replace(B.adjust_network_name, ' ', ''))
        left outer join
        (select lower(ltrim(rtrim(ga_adjust_campaign))) as ga_adjust_campaign, max(criteo_campaign) as criteo_campaign from ${hiveDB}.criteo_campaign_mapping group by lower(ltrim(rtrim(ga_adjust_campaign)))) cm 
        on lOWER(LTRIM(RTRIM(split(A.campaign,' ')[0])))=lower(ltrim(rtrim(cm.ga_adjust_campaign)))
        where cm.ga_adjust_campaign is null
        Group by LOWER(LTRIM(RTRIM(split(A.campaign,' ')[0]))),`date`,A.product,
        A.pos,
        A.mobile_os,
        A.language,
        case when lower(A.app_name) like '%tajawal%' then 'tajawal' else
            case when lower(A.app_name) like '%almosafer%' then 'almosafer' else 'na' end
        end

        union

        Select  LOWER(LTRIM(RTRIM(A.campaign))) as campaign, A.gadate as `date`,
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
        max(COALESCE(cm2.criteo_campaign, A.campaign)) as campaign_name
        FROM 
        ${hiveDB}.fact_google_analytics_ad A 
        inner join
        (
            select
            lower(regexp_replace(A.ga_sourcemedium, ' ', '')) as ga_sourcemedium, 
            max(B.channel_group) as channel_group, max(B.channel) as channel, count(*)
            from ${hiveDB}.dim_spend_ga_sourcemedium_channels A inner join ${hiveDB}.dim_spend_channels B
            on A.dim_channel_id = B.dim_channel_id
            where lower(B.channel) like '%criteo%'
            group by lower(regexp_replace(A.ga_sourcemedium, ' ', ''))
        ) B on lower(regexp_replace(A.sourcemedium, ' ', '')) = lower(regexp_replace(B.ga_sourcemedium, ' ', ''))
        inner join ${hiveDB}.dim_spend_ga_views C on A.view_id = C.dim_view_id
        left outer join
        (select lower(ltrim(rtrim(ga_adjust_campaign))) as ga_adjust_campaign, max(criteo_campaign) as criteo_campaign from ${hiveDB}.criteo_campaign_mapping group by lower(ltrim(rtrim(ga_adjust_campaign)))) cm2 
        on lOWER(LTRIM(RTRIM(A.campaign)))=lower(ltrim(rtrim(cm2.ga_adjust_campaign)))
        where A.view_id in ('130709589', '109503410') and cm2.ga_adjust_campaign is null
        Group by  LOWER(LTRIM(RTRIM(A.campaign))), A.gadate,
        A.product,
        A.pos,
        A.mobile_os,
        A.language,
        C.group_name
    ) b
     where b.bookings!=0
    group by b.device,b.language,b.mobile_os,b.pos,b.product,b.group_name,b.campaign,b.`date`
   ) ad
on lOWER(cr.campaignname)=lOWER(ad.campaign) and regexp_replace(cr.daydate, '-', '') = ad.`date`
