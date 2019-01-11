drop table if exists  ${hiveDB}.fact_bing_data_intermediate;

create table if not exists  ${hiveDB}.fact_bing_data_intermediate 
as
select 
ACCOUNT_ID,
ACCOUNT_NAME,
DAY_DATE,      
CAMPAIGN_NAME,    
CAMPAIGN_ID,      
CAMPAIGN_STATUS,  
KEYWORD,          
KEYWORD_ID,       
DEVICE_TYPE,      
BID_MATCH_TYPE,   
CASE WHEN CLICKS='null' THEN 0 ELSE cast(CLICKS as int) END as CLICKS,            
IMPRESSIONS,      
regexp_replace(CTR,'%','') as CTR,              
AVERAGE_CPC,      
CASE WHEN SPEND='null' THEN 0 ELSE cast(SPEND as decimal(20,6)) END as SPEND ,      
QUALITY_SCORE,    
AVERAGE_POSITION, 
CONVERSIONS,      
regexp_replace(CONVERSION_RATE,'%','') as CONVERSION_RATE, 
CASE WHEN DAILY_BUDGET='null' THEN 0 ELSE cast(DAILY_BUDGET as decimal(20,6)) END as DAILY_BUDGET ,
CASE WHEN (lower(campaign_name) like '%hotel%'
                   or lower(campaign_name) like '%flight%'
                   or upper(campaign_name) like '%-HO-%'
                   or upper(campaign_name) like '%\_HL\_%'
                   or upper(campaign_name) like '%-FL-%'
                   or upper(campaign_name) like '%\_FL\_%')
             and (lower(campaign_name) like '%package%'
                   or lower(campaign_name) like '%\_p\_%') then 'both'
 else CASE WHEN lower(campaign_name) like '%hotel%' and  lower(campaign_name) like '%flight%' then 'both'
 else CASE WHEN lower(campaign_name) like '%package%'  or lower(campaign_name) like '%\_p\_%' THEN 'package'
 else CASE WHEN lower(campaign_name) like '%flight%' or upper(campaign_name) like '%-FL-%' or upper(campaign_name) like '%\_FL\_%' then 'flight'
 else CASE WHEN lower(campaign_name) like '%hotel%' or upper(campaign_name) like '%-HO-%' or upper(campaign_name) like '%\_HL\_%' then 'hotel'
 else CASE WHEN lower(campaign_name) like '%both%' or lower(campaign_name) like '%\_f_h\_%' or lower(campaign_name) like '%\fh\_%'  then 'both'
 else 'NA'
 end end end end end end as PRODUCT,
CASE WHEN lower(ACCOUNT_NAME) like '%ksa%' then 'Saudi Arabia' else CASE WHEN lower(ACCOUNT_NAME) like '%uae%' then 'UAE' else CASE WHEN lower(ACCOUNT_NAME) like '%kuwait%' then 'Kuwait' else CASE WHEN lower(ACCOUNT_NAME) like '%bahrain%' then 'Bahrain' else CASE WHEN lower(ACCOUNT_NAME) like '%qatar%' then 'Qatar' else CASE WHEN lower(ACCOUNT_NAME) like '%oman%' then 'Oman' else CASE WHEN lower(ACCOUNT_NAME) like '%egypt%' then 'Egypt' else 'NA' end end end end end end end as POS,
case when lower(CAMPAIGN_NAME) like '%-ar-%' then 'ar' else case when lower(CAMPAIGN_NAME) like '%-en-%' then 'en' else 'NA' end end as LANGUAGE,
'SEM' as CHANNAL,
'Bing' as SUB_CHANNEL,
'040dbd7c404d2b44ee8a1e239ed7c443' as DIM_CHANNEL_ID,
CASE WHEN lower(device_type) like '%smartphone%' then 'app' else CASE WHEN lower(device_type) like '%tablet%' then 'app'else CASE WHEN lower(device_type) like '%computer%' then 'web' else 'NA' end end end as DEVICE, 
CASE WHEN lower(ACCOUNT_NAME) like '%mosafer%' then 'almosafer' else 'tajawal' end as GROUP_NAME,
CASE WHEN lower(CAMPAIGN_NAME) like '%-brand%' then 'Brand' else 'Non Brand' END as BRAND,
'Aquisition' as OBJECTIVE
from  ${hiveDB}.bing_data;

--insert into  ${hiveDB}.fact_bing_data select * from fact_bing_data_intermediate;
