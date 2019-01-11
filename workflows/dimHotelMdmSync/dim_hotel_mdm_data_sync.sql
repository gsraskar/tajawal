insert into ${hiveDB}.dim_hotel_mdm
select
h1.*
from ${hiveDB}.dim_hotel h1
left outer join ${hiveDB}.dim_hotel_mdm h2
on h1.dim_hotel_id=h2.dim_hotel_id
where h2.dim_hotel_id is null;


--================= MDM dim_hotel to dim_hotel mapping ==========================

drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
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
 from ${hiveDB}.dim_hotel a
 left outer join 
    (select dim_hotel_id,
     max(name) as name,
     max(address) address,
     max(city) as city,
     max(country_code) as country_code,
     max(chain) as chain, max(chain_name) as chain_name,
     max(hotel_brand) as hotel_brand 
     from ${hiveDB}.dim_hotel_mdm 
     group by dim_hotel_id
    ) c
 on lower(regexp_replace(a.dim_hotel_id, ' ', '')) = lower(regexp_replace(c.dim_hotel_id, ' ', ''));

Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel ;