Drop table if exists ${hiveDB}.tmp_fact_googleadwords;
CREATE TABLE ${hiveDB}.tmp_fact_googleadwords ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' as 
 select 
 `date` , 
  `cost` , 
  `impressions` , 
  `clicks` , 
  `cost_per_conversion`, 
  `ctr` , 
  `active_view_ctr` , 
  `average_cpv` , 
  `conversion_rate` , 
  `ad_group_id`, 
  `ad_group_name`, 
  `ad_group_status`, 
  `campaign_id` , 
  `campaign_name`, 
  `campaign_status`, 
  `conversions`, 
  `engagements`, 
  `averageposition`, 
  `cpcbid`, 
  `ad_network_type1`, 
  `ad_network_type2`,
  `device` , 
   `customer_id`,
  CASE when campaign_name like '%-HO-%' then 'hotel' else CASE when campaign_name like '%-FL-%' then 'flight' else 'both' end end `product` 
  from ${hiveDB}.fact_googleadwords;
Drop table ${hiveDB}.fact_googleadwords;
Alter table ${hiveDB}.tmp_fact_googleadwords rename TO ${hiveDB}.fact_googleadwords;
