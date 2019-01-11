set hive.mv.files.thread=0;

-- ========For eagle Traveles
Drop table if exists tajawal_bi.tmp_eagle_travels;
 Create table tajawal_bi.tmp_eagle_travels ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
 select * from tajawal_bi.eagle_travels
 where lower(`status`) not like "%status%";
Drop table if exists tajawal_bi.eagle_travels_final;
Alter table tajawal_bi.tmp_eagle_travels rename to tajawal_bi.eagle_travels_final;


--=============================




Drop table if exists tajawal_bi.tmp_tickets_flight;
 Create table tajawal_bi.tmp_tickets_flight ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
 select 
booking_id,               
 ticket_number,               
from_unixtime(unix_timestamp(booking_date,'dd-MMM-yyyy'),'yyyyMMdd') as booking_date,
from_unixtime(unix_timestamp(travel_date,'dd-MMM-yyyy'),'yyyyMMdd') as travel_date,
--  booking_date,           
--  travel_date,               
 origin_city,            
 destination_city,               
 trip_type,               
 flight_type,               
 airline_name,               
 route,               
 booking_status,               
 currency,               
 COALESCE(selling_amount,'0.0') as selling_amount,               
 COALESCE(split(passenger_count,'.')[0],'0') as passenger_count,                  
 passenger_type
 from tajawal_bi.tickets_flight
 where lower(`booking_id`) not like "%booking id%";
Drop table if exists tajawal_bi.tickets_flight_final;
-- Alter table tajawal_bi.tmp_tickets_flight rename to tajawal_bi.tickets_flight;
-- Create table tajawal_bi.tmp_tickets_flight_bkup ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
-- Select * from tajawal_bi.tmp_tickets_flight;

Alter table tajawal_bi.tmp_tickets_flight rename to tajawal_bi.tickets_flight_final;

Drop table if exists tajawal_bi.tmp_tours_hotel;
 Create table tajawal_bi.tmp_tours_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
 select 
   `booking_id` ,                                                                                    
   `supplier` ,                                                                                      
   `booking_date` ,                                                                                  
   `cancellation_date` ,                                                                             
   `payment_date` ,                                                                                  
   `payment_status` ,                                                                                
   `booking_status` ,                                                                                
   `rate_type` ,                                                                                     
   COALESCE(`exchange_rate`,'0.0') as exchange_rate,                                                                                 
   COALESCE(`vendor_cost`,0.0) as vendor_cost ,                                                                                   
   COALESCE(`markup_value`,0.0) as markup_value,                                                                                  
   `vendor_id` ,                                                                                     
   `vendor_name` ,                                                                                   
   COALESCE(`gross_amount`,0.0) as gross_amount ,                                                                                  
   `currency` ,
    MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(hotel_name),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(hotel_city,',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(hotel_city,',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL','')))  as`hotel_id` ,                                                                                      
   `hotel_name` ,                                                                                    
   regexp_replace(`hotel_city`,'-','') as hotel_city,                                                                                    
   `hotel_country` ,                                                                                 
   `hotel_service_category`,                                                                        
   `checkin_date` ,                                                                                  
   `checkout_date` ,                                                                                 
   COALESCE(`room_count`,0) as room_count,                                                                                       
   `room_type` ,                                                                                     
   `service_category` ,                                                                              
   COALESCE(`number_of_nights`,0) as number_of_nights ,                                                                                 
   COALESCE(`selling_amount`,0.0) as selling_amount ,                                                                                
   `payment_currency` ,                                                                              
   COALESCE(`number_of_guests`,0) as number_of_guests  ,                                                                                 
   COALESCE(`discount`,0.0) as discount,                                                                                      
   `onedmc_reference_no` 
 from tajawal_bi.tours_hotel
 where lower(`supplier`) not like "supplier";
Drop table if exists tajawal_bi.tours_hotel;

-- Create table tajawal_bi.tmp_tours_hotel_bkup ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
-- Select * from tajawal_bi.tmp_tours_hotel;


Alter table tajawal_bi.tmp_tours_hotel rename to tajawal_bi.tours_hotel;



truncate table tajawal_bi.fact_altayyar_hotel_orders;
insert into table tajawal_bi.`fact_altayyar_hotel_orders`(
   `id` ,
    `order_no` ,
    `dim_group_id`,
   `order_type`,
   `product_type` ,
   `office_id` ,
   `dim_product_category_type` ,
   `dim_bookingdate_id` ,
   `dim_cancellation_date` ,
   `dim_bookingpaid_id` ,
   `dim_hotel_id` ,
   `dim_supplier_id`,
   `dim_vendor_id`,
   `dim_supplier_currency`,
   `dim_checkin_date_id` ,
   `dim_checkout_date_id` ,
   `dim_total_currency` ,
   `dim_request_currency_id` ,
   `dim_status_id` ,
   `dim_bookingtype_id` ,
   `dim_markup_currency` ,
   `dim_paymentstatus_id` ,
   `sale_price_currency` ,
   `net_price_currency` ,
   `sale_price` ,
   `payment_amount` ,
   `supplier_cost` ,
   `length_of_stay` ,
   `booking_window`,
   `pax_count` ,
   `discount_amount` ,
   `payment_amount_usd` ,
   `discount_amount_usd` ,
   `ibv` ,
   `gbv` ,
   `room_nights` ,
   `markup_total` ,
   `markup_percentage` ,
   `markup_final_amount` ,
   `markup_final_currency` ,
   `markup_final_amount_usd` ,
   `onedmc_reference_no`
   )
   select
    booking_id,
   booking_id,
   7,
   'hotel',
   'hotel',
   'Al Tayyar',
   'hotel',
  from_unixtime(unix_timestamp(booking_date, 'dd-MMM-yyyy'),'yyyyMMdd') ,
   from_unixtime(unix_timestamp(cancellation_date, 'dd-MMM-yyyy'),'yyyyMMdd'),
   from_unixtime(unix_timestamp(payment_date, 'dd-MMM-yyyy'),'yyyyMMdd'),
   MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(hotel_name),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(hotel_city,',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(hotel_city,',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL',''))) as dim_hotel_id,
ltrim(rtrim(lower(supplier))) as supplier,
   vendor_id,
   'SAR',
   from_unixtime(unix_timestamp(checkin_date, 'dd-MMM-yyyy'),'yyyyMMdd'),
   from_unixtime(unix_timestamp(checkout_date, 'dd-MMM-yyyy'),'yyyyMMdd'),
   'SAR',
   currency,
   booking_status,
   'Pay Now',
   currency,
   payment_status,
   currency,
   currency,
   COALESCE(gross_amount,0.0),
   COALESCE(selling_amount,0.0),
   COALESCE(vendor_cost,0.0),
   DATEDIFF(from_unixtime(unix_timestamp(checkout_date, 'dd-MMM-yyyy'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(checkin_date, 'dd-MMM-yyyy'),'yyyy-MM-dd')) as length_of_stay,
   DATEDIFF(from_unixtime(unix_timestamp(checkin_date, 'dd-MMM-yyyy'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(booking_date, 'dd-MMM-yyyy'),'yyyy-MM-dd')) as booking_window,
   number_of_guests ,
   COALESCE(discount,0.0),
COALESCE(round((selling_amount / 3.75),4),0.0),
   COALESCE(round((discount / 3.75),4),0.0) as discount_amount_usd,
   COALESCE(round((selling_amount/3.75),4),0.0) as ibv,
   COALESCE(round(((selling_amount-discount)/3.75),4),0.0) as gbv,
   COALESCE(number_of_nights * room_count ,0),
   COALESCE(markup_value,0.0),
   COALESCE(round((markup_value / selling_amount*100),4),0.0) as markup_percentage,
   COALESCE(markup_value,0.0),
   currency,
   COALESCE(round((markup_value /3.75),4),0.0) as markup_final_amount_usd,
   onedmc_reference_no
    from tajawal_bi.tours_hotel;
--==================================For eagles data script
truncate table tajawal_bi.`fact_eagle_travels`;
 insert into table tajawal_bi.`fact_eagle_travels`(
   `id` ,
    dim_group_id,
   `order_type` ,
   `product_type`,
   `office_id` ,
   `dim_product_category_type` ,
   `dim_bookingpaid_id` ,
   `dim_hotel_id` ,
`dim_checkin_date_id`,
   `dim_supplier_id` ,
   `dim_supplier_currency` ,
   `dim_status_id`,
   `dim_bookingtype_id`,
   `dim_markup_currency`,
   `sale_price_currency`,
   `net_price_currency` ,
   `sale_price`,
   `payment_amount`,
   `supplier_cost`,
   `payment_amount_usd`,
   `ibv`,
   `gbv`,
   `markup_total`,
   `markup_percentage`,
   `markup_final_amount`,
   `markup_final_currency`,
   `markup_final_amount_usd`,
`markup_final_amount_sar`
   )
   select
    CONCAT('ATGH',reflect("java.util.UUID", "randomUUID")),
   8,
   'hotel',
   'hotel',
   'Altayyar One DMC',
   'hotel',
   COALESCE(case when date_of_issuance like '%-%' then from_unixtime(unix_timestamp(date_of_issuance, 'dd-MMM-yyyy'),'yyyyMMdd') else CASE WHEN  date_of_issuance like '%/%' then from_unixtime(unix_timestamp(date_of_issuance, 'dd/mm/yyyy'),'yyyyMMdd') else '' end end,'')  ,
   MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(concept),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(city,',')[0]),'\n|\\*',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(city,',')[0]),'\n|\\*',''),'\t',''),'"',''),' ',''),'-',''),'NULL','')))  as dim_hotel_id,
COALESCE(case when departure_time like '%-%' then from_unixtime(unix_timestamp(departure_time, 'dd-MMM-yyyy'),'yyyyMMdd') else CASE WHEN  departure_time like '%/%' then from_unixtime(unix_timestamp(departure_time, 'dd/mm/yyyy'),'yyyyMMdd') else '' end end,'') `dim_checkin_date_id` ,
   ltrim(rtrim(lower(supplier))) as supplier,
   suppliers_currency,
   status,
   'Pay Now',
   suppliers_currency,
   suppliers_currency,
   suppliers_currency,
   COALESCE(regexp_replace(regexp_replace(selling_price,'"',''),',',''),0.0),
   COALESCE(regexp_replace(regexp_replace(retail_price,'"',''),',',''),0.0),
   COALESCE(regexp_replace(regexp_replace(selling_price,'"',''),',',''),0.0),
COALESCE(round(regexp_replace(regexp_replace(retail_price,'"',''),',','')
* CASE WHEN suppliers_currency = "USD" then 1 else CASE WHEN suppliers_currency = "SAR" then 0.266639 else CASE WHEN suppliers_currency = "AED" then 0.272226 else case when suppliers_currency = "GBP" then 1.31996 else case when suppliers_currency="MYR" then 0.237294 else CASE WHEN suppliers_currency = "CHF"then 1.02634 else case when suppliers_currency="EUR" then 1.18319 else 0 end end end end end end end
 ,2)
 ,0.0) as payment_amount_usd,
COALESCE(round(regexp_replace(regexp_replace(retail_price,'"',''),',','')
* CASE WHEN suppliers_currency = "USD" then 1 else CASE WHEN suppliers_currency = "SAR" then 0.266639 else CASE WHEN suppliers_currency = "AED" then 0.272226 else case when suppliers_currency = "GBP" then 1.31996 else case when suppliers_currency="MYR" then 0.237294 else CASE WHEN suppliers_currency = "CHF"then 1.02634 else case when suppliers_currency="EUR" then 1.18319 else 0 end end end end end end end
 ,2)
 ,0.0) as ibv,
COALESCE(round(regexp_replace(regexp_replace(retail_price,'"',''),',','')
* CASE WHEN suppliers_currency = "USD" then 1 else CASE WHEN suppliers_currency = "SAR" then 0.266639 else CASE WHEN suppliers_currency = "AED" then 0.272226 else case when suppliers_currency = "GBP" then 1.31996 else case when suppliers_currency="MYR" then 0.237294 else CASE WHEN suppliers_currency = "CHF"then 1.02634 else case when suppliers_currency="EUR" then 1.18319 else 0 end end end end end end end
 ,2)
 ,0.0) as gbv,
   round(COALESCE((regexp_replace(regexp_replace(retail_price,'"',''),',','') - regexp_replace(regexp_replace(selling_price,'"',''),',','')),0.0),2) as markup_total,
   round(COALESCE(((regexp_replace(regexp_replace(retail_price,'"',''),',','') - regexp_replace(regexp_replace(selling_price,'"',''),',',''))/regexp_replace(regexp_replace(selling_price,'"',''),',','') * 100 ),0.0),2) as markup_percentage,
  round(COALESCE((regexp_replace(regexp_replace(retail_price,'"',''),',','') - regexp_replace(regexp_replace(selling_price,'"',''),',','')),0.0),2) as markup_final_amount,
   'SAR',
round(COALESCE((regexp_replace(regexp_replace(retail_price,'"',''),',','') - regexp_replace(regexp_replace(selling_price,'"',''),',','')),0.0) * CASE WHEN suppliers_currency = "USD" then 1 else CASE WHEN suppliers_currency = "SAR" then 0.266639 else CASE WHEN suppliers_currency = "AED" then 0.272226 else case when suppliers_currency = "GBP" then 1.31996 else case when suppliers_currency="MYR" then 0.237294 else CASE WHEN suppliers_currency = "CHF"then 1.02634 else case when suppliers_currency="EUR" then 1.18319 else 0 end end end end end end end
  ,4) as markup_final_amount_usd,
round(COALESCE((regexp_replace(regexp_replace(retail_price,'"',''),',','') - regexp_replace(regexp_replace(selling_price,'"',''),',','')),0.0) * CASE WHEN suppliers_currency = "USD" then 3.75023 else CASE WHEN suppliers_currency = "SAR" then 1 else CASE WHEN suppliers_currency = "AED" then 1.02105 else case when suppliers_currency = "GBP" then 4.91813 else case when suppliers_currency="MYR" then 0.886619 else CASE WHEN suppliers_currency = "CHF" then 3.84645 else case when suppliers_currency="EUR" then 4.39725
 else 0 end end end end end end end
  ,4) as markup_final_amount_sar
    from tajawal_bi.eagle_travels_final;
 


 
--=========================

truncate table tajawal_bi.fact_altayyar_flight_orders;
 insert into tajawal_bi.fact_altayyar_flight_orders 
(
id,
`order_type`,
`product_type`,
`dim_group_id`,
`order_no`,
`dim_product_category_type`,
`dim_office_id`,
dim_bookingdate_id,
dim_travel_date,
dim_origin,
dim_destination,
dim_airline_leg1,
dim_trip_type,
dim_journey_type,
dim_totals_currency,
dim_status_id,
dim_bookingtype_id,
is_manual,
`conversion_rate_aed`,
`conversion_rate_usd`,
payment_amount,
adult_count,
no_of_passengers,
total_seat,
discount_amount,
payment_amount_usd,
discount_amount_usd,
iov,
gbv,
length_of_trip,
tax,
base_amount,
inputvat,
outputvat,
total_vat,
children_count,
infants_count,
no_of_legs,
no_of_segments,
booking_window
 )
 select
 CONCAT(ticket_number,booking_id) as id,
 'flight',
 'flight',
 '7',
booking_id,
'flight',
'Al Tayyar',
booking_date,
travel_date,
SUBSTR(origin_city,1,3) as origin_city,
SUBSTR(destination_city,1,3) as destination_city,
SUBSTR(airline_name,1,2) as airline_name,
`trip_type`,
SUBSTR(`flight_type`,1,3) as flight_type,
COALESCE(REGEXP_REPLACE(currency,'-','SAR'),'SAR') as currency,
booking_status,
'Pay Now',
'false',
 CASE WHEN currency = 'SAR' THEN '0.98' WHEN currency = 'AED' THEN '1' WHEN currency = 'USD' THEN '0.2723' WHEN currency = 'QAR' THEN '0.9913' WHEN currency = 'KWD' THEN '0.0821' WHEN currency = 'BHD' THEN '0.1026' WHEN currency = 'EGP' THEN '0.21' WHEN currency = '-' THEN '0.98' END as conversion_rate_aed,
CASE WHEN currency = 'SAR' THEN '0.27' WHEN currency = 'AED' THEN '0.2739' WHEN currency = 'USD' THEN '1' WHEN currency = 'QAR' THEN '0.2747' WHEN currency = 'KWD' THEN '3.28' WHEN currency = 'BHD' THEN '2.702' WHEN currency = 'EGP' THEN '0.056'  WHEN currency = '-' THEN '0.27' END as conversion_rate_usd,
selling_amount,
CASE WHEN lower(passenger_type) = 'adult' then '1' else '0' end adult_count,
passenger_count,
passenger_count,
'0' as discount_amount,
round(selling_amount * CASE WHEN currency = 'SAR' THEN '0.27' WHEN currency = 'AED' THEN '0.2739' WHEN currency = 'USD' THEN '1' WHEN currency = 'QAR' THEN '0.2747' WHEN currency = 'KWD' THEN '3.28' WHEN currency = 'BHD' THEN '2.702' WHEN currency = 'EGP' THEN '0.056'  WHEN currency = '-' THEN '0.27' END,2) as payment_amount_usd,
'0' as discount_amount_usd,
round(selling_amount * CASE WHEN currency = 'SAR' THEN '0.27' WHEN currency = 'AED' THEN '0.2739' WHEN currency = 'USD' THEN '1' WHEN currency = 'QAR' THEN '0.2747' WHEN currency = 'KWD' THEN '3.28' WHEN currency = 'BHD' THEN '2.702' WHEN currency = 'EGP' THEN '0.056' WHEN currency = '-' THEN '0.27' END,2) as iov,
(round(selling_amount * CASE WHEN currency = 'SAR' THEN '0.27' WHEN currency = 'AED' THEN '0.2739' WHEN currency = 'USD' THEN '1' WHEN currency = 'QAR' THEN '0.2747' WHEN currency = 'KWD' THEN '3.28' WHEN currency = 'BHD' THEN '2.702' WHEN currency = 'EGP' THEN '0.056' WHEN currency = '-' THEN '0.27' END,2) - 0) as gbv,
'0.0',
'0.0',
'0.0',
'0.0',
'0.0',
'0.0',
CASE WHEN lower(passenger_type) = 'child' then '1' else '0' end children_count,
CASE WHEN lower(passenger_type) = 'infant' then '1' else '0' end infant_count,
'0.0',
'0.0', 
'0.0'
from tajawal_bi.tickets_flight_final;



--==Code to count no of passangers
Drop table if exists tajawal_bi.fact_altayyar_flight_orders_final; 
create table tajawal_bi.fact_altayyar_flight_orders_final row format delimited fields terminated by '\t' as 
select
id ,
track_id ,
order_type ,
product_type ,
dim_group_id ,
order_no ,
dim_product_category_type ,
dim_office_id ,
dim_bookingdate_id ,
dim_booking_hour ,
dim_booking_timestamp ,
dim_travel_date ,
dim_arrival_date ,
dim_travel_hour ,
dim_bookingupdated_id ,
dim_firstcancellation_date ,
dim_cancellation_date ,
dim_bookingpaid_id ,
dim_store_id ,
dim_promo_id ,
dim_customer_id ,
dim_vendor_id ,
dim_language ,
dim_origin ,
dim_destination ,
dim_flightcode_leg1 ,
dim_airline_leg1 ,
dim_flightcode_leg2 ,
dim_airlinecode_leg2 ,
dim_trip_type ,
dim_journey_type , 
dim_totals_currency ,
dim_status_id ,
dim_paymentstatus_id ,
dim_state_id ,
dim_bookingtype_id ,
is_manual ,
carrier_type ,
payment_type ,
visa_checkout_flag ,
client_os ,
cabin_class ,
deal_amount ,
loss_amount ,
devicetype ,
xplatform ,
device_category ,
conversion_rate_aed ,
conversion_rate_usd ,
payment_amount ,
adult_count ,
children_count ,
infants_count ,
no_of_passengers ,
total_seat ,
no_of_legs ,
no_of_segments ,
discount_amount ,
booking_window ,
payment_amount_usd ,
discount_amount_usd ,
iov ,
gbv ,
length_of_trip ,
tax ,
base_amount ,
inputvat ,
outputvat ,
total_vat 
 from tajawal_bi.fact_altayyar_flight_orders;


