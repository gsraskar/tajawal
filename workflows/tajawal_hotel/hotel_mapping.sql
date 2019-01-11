--=================For dim_hotel_city mapping 

--drop table if exists tajawal_bi.tmp_dim_hotel;
--create table tajawal_bi.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
--select
--a.dim_hotel_id,
-- a.name,
-- a.address,
--lower(COALESCE(c.city,a.city)) as city,
-- district,
-- lower(COALESCE(c.country,a.country_code)) as country_code,
-- star_rating,
-- longitude,
-- latitude,
-- zipcode,
-- case when regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain_code))),'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain_code))),'na'), '#|N/A', '') end as chain,
-- case when regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain_name))),'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain_name))),'na'), '#|N/A', '') end as chain_name,
-- case when regexp_replace(COALESCE(ltrim(rtrim(lower(c.hotel_brand))),'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(ltrim(rtrim(lower(c.hotel_brand))),'na'), '#|N/A', '') end as hotel_brand
-- from tajawal_bi.dim_hotel a
-- left outer join (select dim_hotel_id, max(city) as city, max(country) as country, max(chain_code) as chain_code, max(chain_name) as chain_name, max(hotel_brand) as hotel_brand from googlesheets.dim_hotel_chain_mapping group by dim_hotel_id) c
-- on lower(regexp_replace(a.dim_hotel_id, ' ', '')) = lower(regexp_replace(c.dim_hotel_id, ' ', ''))
-- ;

-- Drop table if exists tajawal_bi.dim_hotel;
--Alter table tajawal_bi.tmp_dim_hotel RENAME TO tajawal_bi.dim_hotel ;

--================= MDM dim_hotel to dim_hotel mapping ==========================

drop table if exists tajawal_bi.tmp_dim_hotel;
create table tajawal_bi.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select
 a.dim_hotel_id,
 COALESCE(c.name,a.name) as name,
 COALESCE(c.address,a.address) as address,
lower(COALESCE(c.city,a.city)) as city,
 district,
 lower(COALESCE(c.country_code,a.country_code)) as country_code,
 star_rating,
 longitude,
 latitude,
 zipcode,
 case when regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain))),'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain))),'na'), '#|N/A', '') end as chain,
 case when regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain_name))),'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(ltrim(rtrim(lower(c.chain_name))),'na'), '#|N/A', '') end as chain_name,
 case when regexp_replace(COALESCE(ltrim(rtrim(lower(c.hotel_brand))),'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(ltrim(rtrim(lower(c.hotel_brand))),'na'), '#|N/A', '') end as hotel_brand
 from tajawal_bi.dim_hotel a
 left outer join (select dim_hotel_id,max(name) as name ,max(address) address , max(city) as city, max(country_code) as country_code, max(chain) as chain, max(chain_name) as chain_name, max(hotel_brand) as hotel_brand from tajawal_bi.dim_hotel_mdm group by dim_hotel_id) c
 on lower(regexp_replace(a.dim_hotel_id, ' ', '')) = lower(regexp_replace(c.dim_hotel_id, ' ', ''))
 ;

 Drop table if exists tajawal_bi.dim_hotel;
Alter table tajawal_bi.tmp_dim_hotel RENAME TO tajawal_bi.dim_hotel ;

--=================For clean city mapping 

drop table if exists tajawal_bi.tmp_dim_hotel;
create table tajawal_bi.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select
a.dim_hotel_id,
 a.name,
 a.address,
lower(COALESCE(b.cleaned_city_name,a.city)) as city,
 district,
upper(country_code) as country_code,
 star_rating,
 longitude,
 latitude,
 zipcode,
 chain,
 chain_name,
 hotel_brand
 from tajawal_bi.dim_hotel a
 left outer join
 (select regexp_replace(ltrim(rtrim(lower(city_name))), ' ','') as city_name, max(cleaned_city_name) as cleaned_city_name from googlesheets.dim_hotel_city_cleanup group by regexp_replace(ltrim(rtrim(lower(city_name))), ' ','')) b
 on regexp_replace(ltrim(rtrim(lower(a.city))),' ','')=regexp_replace(ltrim(rtrim(lower(b.city_name))),' ','')
 ;

 Drop table if exists tajawal_bi.dim_hotel;
Alter table tajawal_bi.tmp_dim_hotel RENAME TO tajawal_bi.dim_hotel ;

