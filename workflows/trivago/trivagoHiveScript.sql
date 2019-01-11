set hive.mv.files.thread=0;
truncate table ${hiveDB}.fact_trivago_hotel_data_intermediate;
--create table tajawal_uat_bi.fact_trivago_hotel_data as
insert into ${hiveDB}.fact_trivago_hotel_data_intermediate
select 
regexp_replace(hotelname,'\t','') as HotelName
,PartnerRef
,POS
,BidCPC
,Status
,OpportunityCPC
,HotelImpr
,CASE WHEN clicks='null' THEN '0' ELSE clicks END
--,CASE WHEN cost='null' THEN '0' ELSE cost END
,CASE WHEN cost='null' THEN '0' ELSE ROUND(cost*1.18,2) as cost END
,AvgCPC
,TopPosShare
,ImprShare
,OutbidRatio
,Beat
,Meet
,Lose
,Unavailability
,MaxPotential
,Bookings
,BookingRate
,CPA
,GrossRev
,ABV
,BookingAmountperClickIndex
,NetRev
,NetRev_Click
,Profit
,ROAS
,Margin
,LastPushed
,Region
,Country
,City
,Stars
,Rating
,trivagoID
,trivagoURL
,MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(hotelname),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(city,',')[0]),'\n|\\(|\\)',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(city,',')[0]),'\n|\\(|\\)',''),'\t',''),'"',''),' ',''),'-',''),'NULL','')))  as `dim_hotel_id` 
,day_date
,group_name
,'Meta' as Channel_group
,'3ef74792006f4b4662b216f65c7b8786' as dim_channel_id
,'Non brand' as brand_nonbrand
,'Acquisition' as objective
from ${hiveDB}.trivago_hotel_data;

--POS DATA
truncate table ${hiveDB}.fact_trivago_pos_data_intermediate;
insert into ${hiveDB}.fact_trivago_pos_data_intermediate select * from ${hiveDB}.trivago_pos_data;


