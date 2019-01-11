-- Query to insert almosafer 2016 hotel data into fact_hotel_orders table
set hive.mv.files.thread=0;


--===Package order discounts
Drop table if exists ${hiveDB}.fact_package_orders_final;
Create table if not exists ${hiveDB}.fact_package_orders_final row format delimited fields terminated by '\t' as
select
A.id ,               
A.trackid,               
A.order_no,               
A.dim_group_id,               
A.order_type,               
A.products_type,               
A.dim_bookingdate_id,               
A.dim_bookingupdated_id,               
A.dim_bookingpaid_id,               
A.dim_supplier_id,               
A.dim_supplier_currecny,               
A.dim_customer_id,               
A.dim_store_id,               
A.dim_language_id,               
B.dim_promo_id,               
A.dim_checkin_date_id,               
A.dim_checkout_date_id,               
A.dim_validfrom_date_id,               
A.dim_validto_date_id,               
A.dim_status_id,               
A.dim_payment_status_id,               
A.dim_bookingtype_id,               
A.payment_type,               
A.client_os,               
A.sale_price_currency,               
A.package_name,               
A.destination,               
A.isactive,               
A.sku,               
A.package_code,               
A.mastercode,
A.payment_method,               
A.quantity,               
A.net_price_currency,               
A.duration,               
A.profit,               
A.totalsalesprice,               
A.markuppercentage,               
A.discountpercentage,               
A.pax_count,               
A.payment_amount,               
A.supplier_cost,               
A.sale_price_base,               
A.sale_price_total,               
A.payment_amount_usd,               
COALESCE(B.discount_amount,'0.0') as discount_amount,               
COALESCE(round(B.discount_amount*A.conversion_rate_usd),'0.0') as discount_amount_usd,               
round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd),2) as iov,
CASE when ltrim(rtrim(upper(A.dim_status_id))) like '%FRAUD%' then 0.0 else case when ltrim(rtrim(A.dim_status_id)) in ('91', '94') then 0.0 else round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd),2) end end as gbv,
A.conversion_rate_aed,               
A.conversion_rate_usd,               
A.totals_currency   
from ${hiveDB}.fact_package_orders A                 
LEFT OUTER JOIN
(
select id, max(CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'SUMMER OFFER' or ltrim(rtrim(upper(dim_promo_id))) like 'ENBD OFFERR' then 'ENBD Offer' else  CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL' or ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL OFFER' then 'Visa hotel offer' else dim_promo_id end end ) as dim_promo_id,sum(discount_amount)*-1 as discount_amount from ${hiveDB}.fact_package_orders where products_type = 'rule' and discount_amount < 0
group by id
) B
ON A.id = B.id  where A.products_type="package";


--==========Package flight with discount amount

Drop table if exists ${hiveDB}.fact_package_flight_final;
Create table if not exists ${hiveDB}.fact_package_flight_final row format delimited fields terminated by '\t' as
select
A.id ,               
A.order_no,               
A.order_type,               
A.category,
A.dim_group_id,               
A.airline_name,               
A.status,               
A.supplier_status,               
A.valid,               
A.code,               
A.added_by,               
A.dim_supplier_id,               
A.dim_customer_id,               
B.dim_promo_id,               
A.dim_bookingdate_id,               
A.dim_bookingupdated_id,               
A.cabin_class,               
A.flight_number,               
A.airline_code,               
A.origin_city,               
A.destination_city,               
A.origin_country,               
A.destination_country,               
A.dim_departure_time_id,               
A.dim_arrival_time_id,               
A.supplier_currency,               
A.payment_status,               
A.sale_price_currency, 
A.payment_method,              
A.qty,                  
A.flight_duration,               
A.stops_number,               
A.sale_price_total,               
A.sale_price_base,               
COALESCE(B.discount,'0.0') as discount,               
COALESCE(round(B.discount*A.conversion_rate_usd),'0.0') as discount_amount_usd,               
A.tax,               
A.supplier_price_total,               
A.supplier_price_base,               
A.pax_count,                  
A.payment_amount_usd,               
round((A.sale_price_total * A.conversion_rate_usd) + (COALESCE(B.discount, 0.0) * A.conversion_rate_usd),2) as iov,
CASE when ltrim(rtrim(upper(A.status))) like '%FRAUD%' then 0.0 else case when ltrim(rtrim(A.status)) in ('91', '94') then 0.0 else round((A.sale_price_total * A.conversion_rate_usd) + (COALESCE(B.discount, 0.0) * A.conversion_rate_usd),2) end end as gbv,
A.conversion_rate_aed,               
A.conversion_rate_usd,               
A.totals_currency                        
from ${hiveDB}.fact_package_flight A                 
LEFT OUTER JOIN
(
select id, max(CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'SUMMER OFFER' or ltrim(rtrim(upper(dim_promo_id))) like 'ENBD OFFERR' then 'ENBD Offer' else  CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL' or ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL OFFER' then 'Visa hotel offer' else dim_promo_id end end ) as dim_promo_id,sum(discount)*-1 as discount from ${hiveDB}.fact_package_flight where category = 'rule' and discount < 0
group by id
) B
ON A.id = B.id  where A.category="flight";

--===Package order discounts
Drop table if exists ${hiveDB}.fact_package_orders_discounts;
Create table if not exists ${hiveDB}.fact_package_orders_discounts row format delimited fields terminated by '\t' as
select
A.id ,               
A. trackid,               
A. order_no,               
A. dim_group_id,               
A. order_type,               
A. products_type,               
A. dim_bookingdate_id,               
A.dim_bookingupdated_id,               
A.dim_bookingpaid_id,               
A.dim_supplier_id,               
A.dim_supplier_currecny,               
A.dim_customer_id,               
A.dim_store_id,               
A.dim_language_id,               
A.dim_promo_id,               
A.dim_checkin_date_id,               
A.dim_checkout_date_id,               
A.dim_validfrom_date_id,               
A.dim_validto_date_id,               
A.dim_status_id,               
A.dim_payment_status_id,               
A.dim_bookingtype_id,               
A.payment_type,               
A.client_os,               
A.sale_price_currency,               
A.package_name,               
A.destination,               
A.isactive,               
A.sku,               
A.package_code,               
A.mastercode, 
A.payment_method,              
A.quantity,               
A.net_price_currency,               
A.duration,               
A.profit,               
A.totalsalesprice,               
A.markuppercentage,               
A.discountpercentage,               
A.pax_count,               
A.payment_amount,               
A.supplier_cost,               
A.sale_price_base,               
A.sale_price_total,               
round((A.payment_amount * A.conversion_rate_usd),2) as payment_amount_usd,               
COALESCE(A.discount_amount,'0.0') as discount_amount,               
COALESCE(round((A.discount_amount*A.conversion_rate_usd),2),'0.0') as discount_amount_usd,               
round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(A.discount_amount, 0.0) * A.conversion_rate_usd),2) as iov,
CASE when ltrim(rtrim(upper(A.dim_status_id))) like '%FRAUD%' then 0.0 else case when ltrim(rtrim(A.dim_status_id)) in ('91', '94') then 0.0 else round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(A.discount_amount, 0.0) * A.conversion_rate_usd),2) end end as gbv,
A.conversion_rate_aed,               
A.conversion_rate_usd,               
A.totals_currency   
from ${hiveDB}.fact_package_orders A                 
where A.products_type="rule";

--==========Package flight discount

Drop table if exists ${hiveDB}.fact_package_flight_discounts;
Create table if not exists ${hiveDB}.fact_package_flight_discounts row format delimited fields terminated by '\t' as
select
A.id ,               
A.order_no,               
A.order_type,               
A.category, 
A.dim_group_id,              
A.airline_name,               
A.status,               
A.supplier_status,               
A.valid,               
A.code,               
A.added_by,               
A.dim_supplier_id,               
A.dim_customer_id,               
A.dim_promo_id,               
A.dim_bookingdate_id,               
A.dim_bookingupdated_id,               
A.cabin_class,               
A.flight_number,               
A.airline_code,               
A.origin_city,               
A.destination_city,               
A.origin_country,               
A.destination_country,               
A.dim_departure_time_id,               
A.dim_arrival_time_id,               
A.supplier_currency,               
A.payment_status,               
A.sale_price_currency, 
A.payment_method,              
A.qty,                  
A.flight_duration,               
A.stops_number,               
A.sale_price_total,               
A.sale_price_base,               
COALESCE(A.discount,'0.0') as discount,               
COALESCE(round((A.discount*A.conversion_rate_usd),2),'0.0') as discount_amount_usd,               
A.tax,               
A.supplier_price_total,               
A.supplier_price_base,               
A.pax_count,                  
round((A.sale_price_total * A.conversion_rate_usd),2) as payment_amount_usd,               
round((A.sale_price_total * A.conversion_rate_usd) + (COALESCE(A.discount, 0.0) * A.conversion_rate_usd),2) as iov,
CASE when ltrim(rtrim(upper(A.status))) like '%FRAUD%' then 0.0 else case when ltrim(rtrim(A.status)) in ('91', '94') then 0.0 else round((A.sale_price_total * A.conversion_rate_usd) + (COALESCE(A.discount, 0.0) * A.conversion_rate_usd),2) end end as gbv,
A.conversion_rate_aed,               
A.conversion_rate_usd,               
A.totals_currency                        
from ${hiveDB}.fact_package_flight A                 
 where A.category="rule";



-- == Update to subtract order level VAT from every product's payment_amount --

drop table if exists ${hiveDB}.fact_hotel_orders_vat;
CREATE table if not exists ${hiveDB}.fact_hotel_orders_vat row format delimited fields terminated by "\t" as 
select
A.id,
A.track_id,     
A.order_no,     
A.dim_group_id,     
A.order_type,
A.product_type,     
A.office_id,
A.dim_product_category_type,     
A.dim_bookingdate_id,
A.dim_booking_hour,
A.dim_booking_timestamp,     
A.dim_bookingupdated_id,     
A.dim_firstcancellation_date,     
A.dim_cancellation_date,
A.dim_bookingpaid_id,
A.dim_hotel_id,
A.dim_supplier_id,     
A.dim_vendor_id,
A.dim_supplier_currency,     
A.dim_customer_id,
A.dim_store_id,
A.dim_language_id,     
A.dim_checkin_date_id,     
A.dim_checkout_date_id,     
A.dim_total_currency,
A.dim_request_currency_id,     
A.dim_promo_id,
A.dim_status_id,     
A.dim_state_id,     
A.dim_bookingtype_id,     
A.dim_cancellation_policy,     
A.dim_ratetype,
A.dim_markup_currency,     
A.dim_paymentstatus_id,     
A.is_manual,
A.ahs_group_name,     
A.sale_price_currency,     
A.net_price_currency,     
A.payment_type,
A.visa_checkout_flag,     
A.client_os,
A.dim_hotel_id_source,
A.devicetype,
A.xplatform,
A.payment_method,     
A.sale_price,
A.net_price,     
A.payment_amount - COALESCE(C.total_vat, 0.0) as payment_amount,     
A.booking_value,     
A.supplier_cost,     
A.conversion_rate_aed,     
A.conversion_rate_usd,     
A.conversion_rate_sar,     
A.conversion_rate_kwd,     
A.conversion_rate_qar,     
A.conversion_rate_bhd,     
A.room_count,
A.length_of_stay,     
A.booking_window,     
A.pax_count,
A.discount_amount,     
A.payment_amount_usd,     
A.discount_amount_usd,     
A.iov,
A.gbv,     
A.room_nights,     
A.markup_total,     
A.markup_percentage,     
A.markup_final_amount,     
A.markup_final_currency,     
A.markup_final_amount_usd,
A.inputvat,
A.outputvat,
COALESCE(C.total_vat, 0.0) as total_vat
from ${hiveDB}.fact_hotel_orders A
LEFT OUTER JOIN (select id, max(total_vat) as total_vat from ${hiveDB}.fact_vat_orders group by id) C
ON A.id = C.id 
;

--============For adding dim channel id in hotel orders
drop table if exists ${hiveDB}.tmp_fact_hotel_orders_vat;
create table if not exists ${hiveDB}.tmp_fact_hotel_orders_vat row format delimited fields terminated by "\t" as
select A.*,D.dim_channel_id from ${hiveDB}.fact_hotel_orders_vat A 
LEFT OUTER JOIN (select transaction_id,max(dim_channel_id) as dim_channel_id from ${hiveDB}.fact_google_analytics_transactions_final group by transaction_id) D 
ON A.order_no=D.transaction_id
;
Drop table if exists ${hiveDB}.fact_hotel_orders_vat;
Alter table ${hiveDB}.tmp_fact_hotel_orders_vat rename to ${hiveDB}.fact_hotel_orders_vat;
--==========================

drop table if exists ${hiveDB}.fact_flight_orders_vat;
create table if not exists ${hiveDB}.fact_flight_orders_vat row format delimited fields terminated by "\t" as

select
A.id,
A.track_id,
A.order_type,
A.product_type,
A.dim_group_id,
A.order_no,
A.dim_product_category_type,
A.dim_office_id,
A.dim_bookingdate_id,
A.dim_booking_hour,
A.dim_booking_timestamp,
A.dim_travel_date,
A.dim_arrival_date,
A.dim_travel_hour,
A.dim_bookingupdated_id,
A.dim_firstcancellation_date,
A.dim_cancellation_date,
A.dim_bookingpaid_id,
A.dim_store_id,
A.dim_promo_id,
A.dim_customer_id,
A.dim_vendor_id,
A.dim_language,
A.dim_origin,
A.dim_destination,
A.dim_flightcode_leg1,
A.dim_airline_leg1,
A.dim_flightcode_leg2,
A.dim_airlinecode_leg2,
A.dim_trip_type,
A.dim_journey_type,
A.dim_totals_currency,
A.dim_status_id,
A.dim_paymentstatus_id,
A.dim_state_id,
A.dim_bookingtype_id,
A.is_manual,
case when dim_vendor_id in('tf','travelfusion') and  dim_airline_leg1!= 'PK' or lower(dim_vendor_id)= "al jazeera" or dim_airline_leg1 in('FZ','XY','NA','NE','PG','TZ','SM') then 'low cost' else 
case when  dim_vendor_id in ('tf','travelfusion') and  dim_airline_leg1= 'PK' then 'full service' else 
case when (dim_vendor_id='amd' or dim_vendor_id="Amadeus") and  dim_airline_leg1 not in('FZ','XY','NA','NE','PG','TZ','SM')  or dim_vendor_id in('TK','9W') then 'full service' else 'low cost' end end end as carrier_type,
A.payment_type,
A.visa_checkout_flag,
A.client_os,
A.cabin_class,
A.deal_amount,
A.loss_amount,
A.devicetype,
A.xplatform,
A.rbd,
A.payment_method,
A.conversion_rate_aed,
A.conversion_rate_usd,
A.payment_amount - COALESCE(C.total_vat, 0.0) as payment_amount,
A.adult_count,
A.children_count,
A.infants_count,
A.no_of_passengers,
A.total_seat,
A.no_of_legs,
A.no_of_segments,
A.discount_amount,
A.booking_window,
A.payment_amount_usd,
A.discount_amount_usd,
A.iov,
A.gbv,
A.length_of_trip,
A.tax,
A.base_amount,
A.inputvat,
A.outputvat,
COALESCE(C.total_vat, 0.0) as total_vat,
case when  dim_vendor_id='tf' or  lower(dim_vendor_id)='al jazeera' or dim_vendor_id='travelfusion' then 'non gds' else 
case when dim_vendor_id='amd' or dim_vendor_id="TK" or dim_vendor_id="9W" or dim_vendor_id="Amadeus" then 'gds' else 'non gds' end end as gds_nongds
from ${hiveDB}.fact_flight_orders A
LEFT OUTER JOIN (select id, max(total_vat) as total_vat from ${hiveDB}.fact_vat_orders group by id) C
ON A.id = C.id
;

--============For adding dim channel id flight orders
drop table if exists ${hiveDB}.tmp_fact_flight_orders_vat;
create table if not exists ${hiveDB}.tmp_fact_flight_orders_vat row format delimited fields terminated by "\t" as
select A.*,D.dim_channel_id from ${hiveDB}.fact_flight_orders_vat A 
LEFT OUTER JOIN (select transaction_id,max(dim_channel_id) as dim_channel_id from ${hiveDB}.fact_google_analytics_transactions_final group by transaction_id) D 
ON A.order_no=D.transaction_id
;
Drop table if exists ${hiveDB}.fact_flight_orders_vat;
Alter table ${hiveDB}.tmp_fact_flight_orders_vat rename to ${hiveDB}.fact_flight_orders_vat;



drop table if exists ${hiveDB}.fact_hotel_orders_intermediate;
create table if not exists ${hiveDB}.fact_hotel_orders_intermediate as select * from ${hiveDB}.fact_hotel_orders_vat;

insert into ${hiveDB}.fact_hotel_orders_intermediate(
id,order_no,dim_group_id,order_type,product_type,office_id,dim_product_category_type,dim_bookingdate_id,dim_booking_hour,dim_booking_timestamp,dim_bookingupdated_id,dim_firstcancellation_date,dim_cancellation_date,dim_bookingpaid_id,dim_hotel_id,dim_supplier_id,dim_vendor_id,dim_supplier_currency,dim_customer_id,dim_store_id,dim_language_id,dim_checkin_date_id,dim_checkout_date_id,dim_total_currency,dim_request_currency_id,dim_promo_id,dim_status_id,dim_state_id,dim_bookingtype_id,dim_cancellation_policy,dim_ratetype,dim_markup_currency,dim_paymentstatus_id,is_manual,ahs_group_name,sale_price_currency,net_price_currency,payment_type,visa_checkout_flag,sale_price,net_price,payment_amount,booking_value,supplier_cost,conversion_rate_aed,conversion_rate_usd,conversion_rate_sar,conversion_rate_kwd,conversion_rate_qar,conversion_rate_bhd,room_count,length_of_stay,booking_window,pax_count,discount_amount,payment_amount_usd,discount_amount_usd,iov,gbv,room_nights,markup_total,markup_percentage,markup_final_amount,markup_final_currency,markup_final_amount_usd
)
select
id,order_no,dim_group_id,order_type,product_type,office_id,dim_product_category_type,dim_bookingdate_id,dim_booking_hour,dim_booking_timestamp,dim_bookingupdated_id,dim_firstcancellation_date,dim_cancellation_date,dim_bookingpaid_id,dim_hotel_id,dim_supplier_id,dim_vendor_id,dim_supplier_currency,dim_customer_id,dim_store_id,dim_language_id,dim_checkin_date_id,dim_checkout_date_id,dim_total_currency,dim_request_currency_id,dim_promo_id,dim_status_id,dim_state_id,dim_bookingtype_id,dim_cancellation_policy,dim_ratetype,dim_markup_currency,dim_paymentstatus_id,is_manual,ahs_group_name,sale_price_currency,net_price_currency,payment_type,visa_checkout_flag,sale_price,net_price,payment_amount,booking_value,supplier_cost,conversion_rate_aed,conversion_rate_usd,conversion_rate_sar,conversion_rate_kwd,conversion_rate_qar,conversion_rate_bhd,room_count,length_of_stay,booking_window,pax_count,discount_amount,payment_amount_usd,discount_amount_usd,iov,gbv,room_nights,markup_total,markup_percentage,markup_final_amount,markup_final_currency,markup_final_amount_usd
from ${hiveDB}.fact_hotel_almosafer_2016;



--====insert call center data
insert into ${hiveDB}.fact_hotel_orders_intermediate(
id,
order_no,
dim_group_id,
order_type,
product_type,
office_id,
dim_product_category_type,
dim_bookingdate_id,
dim_hotel_id,
dim_supplier_id,
dim_store_id,
dim_checkin_date_id,
dim_checkout_date_id,
dim_total_currency,
dim_status_id,
dim_paymentstatus_id,
is_manual,
sale_price_currency,
net_price_currency,
payment_type,
sale_price,
net_price,
payment_amount,
booking_value,
supplier_cost,
room_count,
length_of_stay,
booking_window,
discount_amount,
discount_amount_usd,
iov,
gbv,
markup_total,
room_nights,
conversion_rate_aed,
conversion_rate_usd,
conversion_rate_sar,
conversion_rate_kwd,
conversion_rate_qar,
conversion_rate_bhd,
dim_bookingpaid_id,
pax_count,
dim_bookingtype_id
)
select
order_id,
order_id,
'5',
'hotel',
'hotel',
bookedbybranch,
'hotel',
from_unixtime(unix_timestamp(order_date,'MM/dd/yyyy'),'yyyyMMdd') as order_date,
MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(hotel_name),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(destination,',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(destination,',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL','')))  as`hotel_id`,
supplier,
'11000',
from_unixtime(unix_timestamp(start_date,'MM/dd/yyyy'),'yyyyMMdd') as start_date,
from_unixtime(unix_timestamp(end_date,'MM/dd/yyyy'),'yyyyMMdd') as end_date,
sale_currency,
'58',
'80',
'TRUE',
sale_currency,
net_currency,
'prepaid',
sale_price,
net_rate,
sale_price,
sale_price,
net_rate,
rooms,
duration,
datediff(from_unixtime(unix_timestamp(start_date,'MM/dd/yyyy'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(order_date,'MM/dd/yyyy'),'yyyy-MM-dd')) as booking_window,
0,
0,
sale_price,
sale_price,
(sale_price - net_rate) as markup_total,
COALESCE(duration*rooms,0) as room_nights,
CASE WHEN sale_currency = 'SAR' THEN '0.98' WHEN sale_currency = 'AED' THEN '1' WHEN sale_currency = 'USD' THEN '0.2723' WHEN sale_currency = 'QAR' THEN '0.9913' WHEN sale_currency = 'KWD' THEN '0.0821' WHEN sale_currency = 'BHD' THEN '0.1026' WHEN sale_currency = 'EGP' THEN '0.21' END as conversion_rate_aed,
CASE WHEN sale_currency = 'SAR' THEN '0.27' WHEN sale_currency = 'AED' THEN '0.2739' WHEN sale_currency = 'USD' THEN '1' WHEN sale_currency = 'QAR' THEN '0.2747' WHEN sale_currency = 'KWD' THEN '3.28' WHEN sale_currency = 'BHD' THEN '2.702' WHEN sale_currency = 'EGP' THEN '0.056' END as conversion_rate_usd,
CASE WHEN sale_currency = 'SAR' THEN '1' WHEN sale_currency = 'AED' THEN '1.02' WHEN sale_currency = 'USD' THEN '3.75' WHEN sale_currency = 'QAR' THEN '1.03' WHEN sale_currency = 'KWD' THEN '12.37' WHEN sale_currency = 'BHD' THEN '9.94' WHEN sale_currency = 'EGP' THEN '0.21' END as conversion_rate_sar,
CASE WHEN sale_currency = 'SAR' THEN '0.081' WHEN sale_currency = 'AED' THEN '0.83' WHEN sale_currency = 'USD' THEN '0.3' WHEN sale_currency = 'QAR' THEN '0.083' WHEN sale_currency = 'KWD' THEN '1' WHEN sale_currency = 'BHD' THEN '0.8' WHEN sale_currency = 'EGP' THEN '0.017' END as conversion_rate_kwd,
CASE WHEN sale_currency = 'SAR' THEN '0.97' WHEN sale_currency = 'AED' THEN '0.99' WHEN sale_currency = 'USD' THEN '3.64' WHEN sale_currency = 'QAR' THEN '1' WHEN sale_currency = 'KWD' THEN '12.01' WHEN sale_currency = 'BHD' THEN '9.66' WHEN sale_currency = 'EGP' THEN '0.2' END as conversion_rate_qar,
CASE WHEN sale_currency = 'SAR' THEN '0.1' WHEN sale_currency = 'AED' THEN '0.1' WHEN sale_currency = 'USD' THEN '0.38' WHEN sale_currency = 'QAR' THEN '0.1' WHEN sale_currency = 'KWD' THEN '1.24' WHEN sale_currency = 'BHD' THEN '1' WHEN sale_currency = 'EGP' THEN '0.021' END as conversion_rate_bhd,
from_unixtime(unix_timestamp(order_date,'MM/dd/yyyy'),'yyyyMMdd') as dim_bookingpaid_id,
'1',
'Pay Now'
from ${hiveDB}.call_center_hotel_orders;




--====insert call center data
insert into ${hiveDB}.fact_hotel_orders_intermediate(
id,
order_no,
dim_group_id,
order_type,
product_type,
office_id,
dim_product_category_type,
dim_bookingdate_id,
dim_hotel_id,
dim_supplier_id,
dim_store_id,
dim_checkin_date_id,
dim_checkout_date_id,
dim_total_currency,
dim_status_id,
dim_paymentstatus_id,
is_manual,
sale_price_currency,
net_price_currency,
payment_type,
sale_price,
net_price,
payment_amount,
booking_value,
supplier_cost,
room_count,
length_of_stay,
booking_window,
discount_amount,
discount_amount_usd,
iov,
gbv,
markup_total,
room_nights,
conversion_rate_aed,
conversion_rate_usd,
conversion_rate_sar,
conversion_rate_kwd,
conversion_rate_qar,
conversion_rate_bhd,
dim_bookingpaid_id,
pax_count,
dim_bookingtype_id
)
select
order_id,
order_id,
'5',
'hotel',
'hotel',
bookedbybranch,
'hotel',
from_unixtime(unix_timestamp(order_date,'MM/dd/yyyy'),'yyyyMMdd') as order_date,
MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(hotel_name),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(destination,',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(destination,',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL','')))  as`hotel_id`,
supplier,
'11000',
from_unixtime(unix_timestamp(start_date,'MM/dd/yyyy'),'yyyyMMdd') as start_date,
from_unixtime(unix_timestamp(end_date,'MM/dd/yyyy'),'yyyyMMdd') as end_date,
sale_currency,
'58',
'80',
'TRUE',
sale_currency,
net_currency,
'prepaid',
sale_price,
net_rate,
sale_price,
sale_price,
net_rate,
rooms,
duration,
datediff(from_unixtime(unix_timestamp(start_date,'MM/dd/yyyy'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(order_date,'MM/dd/yyyy'),'yyyy-MM-dd')) as booking_window,
0,
0,
sale_price,
sale_price,
(sale_price - net_rate) as markup_total,
COALESCE(duration*rooms,0) as room_nights,
CASE WHEN sale_currency = 'SAR' THEN '0.98' WHEN sale_currency = 'AED' THEN '1' WHEN sale_currency = 'USD' THEN '0.2723' WHEN sale_currency = 'QAR' THEN '0.9913' WHEN sale_currency = 'KWD' THEN '0.0821' WHEN sale_currency = 'BHD' THEN '0.1026' WHEN sale_currency = 'EGP' THEN '0.21' END as conversion_rate_aed,
CASE WHEN sale_currency = 'SAR' THEN '0.27' WHEN sale_currency = 'AED' THEN '0.2739' WHEN sale_currency = 'USD' THEN '1' WHEN sale_currency = 'QAR' THEN '0.2747' WHEN sale_currency = 'KWD' THEN '3.28' WHEN sale_currency = 'BHD' THEN '2.702' WHEN sale_currency = 'EGP' THEN '0.056' END as conversion_rate_usd,
CASE WHEN sale_currency = 'SAR' THEN '1' WHEN sale_currency = 'AED' THEN '1.02' WHEN sale_currency = 'USD' THEN '3.75' WHEN sale_currency = 'QAR' THEN '1.03' WHEN sale_currency = 'KWD' THEN '12.37' WHEN sale_currency = 'BHD' THEN '9.94' WHEN sale_currency = 'EGP' THEN '0.21' END as conversion_rate_sar,
CASE WHEN sale_currency = 'SAR' THEN '0.081' WHEN sale_currency = 'AED' THEN '0.83' WHEN sale_currency = 'USD' THEN '0.3' WHEN sale_currency = 'QAR' THEN '0.083' WHEN sale_currency = 'KWD' THEN '1' WHEN sale_currency = 'BHD' THEN '0.8' WHEN sale_currency = 'EGP' THEN '0.017' END as conversion_rate_kwd,
CASE WHEN sale_currency = 'SAR' THEN '0.97' WHEN sale_currency = 'AED' THEN '0.99' WHEN sale_currency = 'USD' THEN '3.64' WHEN sale_currency = 'QAR' THEN '1' WHEN sale_currency = 'KWD' THEN '12.01' WHEN sale_currency = 'BHD' THEN '9.66' WHEN sale_currency = 'EGP' THEN '0.2' END as conversion_rate_qar,
CASE WHEN sale_currency = 'SAR' THEN '0.1' WHEN sale_currency = 'AED' THEN '0.1' WHEN sale_currency = 'USD' THEN '0.38' WHEN sale_currency = 'QAR' THEN '0.1' WHEN sale_currency = 'KWD' THEN '1.24' WHEN sale_currency = 'BHD' THEN '1' WHEN sale_currency = 'EGP' THEN '0.021' END as conversion_rate_bhd,
from_unixtime(unix_timestamp(order_date,'MM/dd/yyyy'),'yyyyMMdd') as dim_bookingpaid_id,
'1',
'Pay Now'
from ${hiveDB}.call_center_hotel_orders;




--====================Insert almosafer 2016 customers to dim_customer_raw
drop table if exists ${hiveDB}.dim_customer_raw_intermediate;
create table if not exists ${hiveDB}.dim_customer_raw_intermediate ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as select * from ${hiveDB}.dim_customer_raw;

insert into ${hiveDB}.dim_customer_raw_intermediate(
`dim_customer_id`,`email`,`firstname`,`lastname`,`phone`,`title`,`country`,`first_order_date`,`first_order_group` , `first_product` ,`first_store_id`,`first_order_no`,`status`,`first_promo_id` )
   select dim_customer_id,NULL,NULL,NULL,NULL,NULL,NULL,from_unixtime(unix_timestamp(concat(dim_bookingdate_id, ' ', dim_booking_timestamp), 'yyyyMMdd hh:mm:ss'), 'EEE MMM dd hh:mm:ss zzz yyyy') as dim_bookingdate_id,dim_group_id,product_type,dim_store_id,order_no,
  dim_status_id,dim_promo_id
   from ${hiveDB}.fact_hotel_almosafer_2016;
   drop table if exists ${hiveDB}.dim_customer_raw;
   Alter table ${hiveDB}.dim_customer_raw_intermediate rename to ${hiveDB}.dim_customer_raw;
--===============================

DROP TABLE IF EXISTS ${hiveDB}.fact_hotel_orders_final;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_hotel_orders_final ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT
A.id,
A.track_id,
A.order_no,
A.dim_group_id,
A.order_type,
A.product_type,
A.office_id,
A.dim_product_category_type,
A.dim_bookingdate_id,
A.dim_booking_hour,
A.dim_booking_timestamp,
A.dim_bookingupdated_id, 
A.dim_firstcancellation_date ,
A.dim_cancellation_date,
CASE WHEN (A.dim_bookingpaid_id='null' AND Ltrim(RTRIM(A.dim_bookingtype_id)) ='Pay Now') THEN A.dim_bookingdate_id  else A.dim_bookingpaid_id END  dim_bookingpaid_id,
A.dim_hotel_id,
ltrim(rtrim(lower(A.dim_supplier_id))) as dim_supplier_id,
A.dim_vendor_id,
A.dim_supplier_currency,
A.dim_customer_id,
A.dim_store_id,
A.dim_language_id,
A.dim_checkin_date_id,
A.dim_checkout_date_id,
A.dim_total_currency,
A.dim_request_currency_id,
B.dim_promo_id,
A.dim_status_id,
A.dim_state_id,
A.dim_bookingtype_id,
A.dim_cancellation_policy,
A.dim_ratetype,
A.dim_markup_currency,
A.dim_paymentstatus_id,
A.is_manual,
A.ahs_group_name,
A.sale_price_currency,                                     
A.net_price_currency,
A.payment_type,
A.visa_checkout_flag,
A.client_os,
A.dim_hotel_id_source,
A.devicetype,
A.xplatform,
A.payment_method,           	             	                    
case when xplatform = 'ios' or xplatform = 'android' then 'native app'
     when devicetype = 'Smartphone' or devicetype = 'Tablet' or devicetype = 'Smart TV' or devicetype = 'Game console' then 'mobile_web'
     when devicetype = 'Personal computer' then 'web'
     else 'native app'
     end as device_category,
A.sale_price,                                               
A.net_price, 
A.payment_amount,
A.booking_value,
A.supplier_cost,
A.conversion_rate_aed,
A.conversion_rate_usd,
A.conversion_rate_sar,                                       
A.conversion_rate_kwd,                                       
A.conversion_rate_qar,                                       
A.conversion_rate_bhd, 
A.room_count,
A.length_of_stay,
A.booking_window,
A.pax_count,
round(COALESCE(B.discount_amount, 0.0),2) as discount_amount,
round(A.payment_amount * A.conversion_rate_usd,2) as payment_amount_usd,
round(COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd,2) as discount_amount_usd,
round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd),2) as ibv,
CASE when ltrim(rtrim(upper(A.dim_state_id))) like '%FRAUD%' then 0.0 else case when ltrim(rtrim(A.dim_status_id)) in ('91', '94') then 0.0 else round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd),2) end end as gbv,
COALESCE(A.length_of_stay*A.room_count,0) as room_nights
-- Markup amount calculation
, round(CASE WHEN sale_price is null OR net_price is null THEN 0
ELSE
    (sale_price * (
        CASE WHEN net_price_currency = 'SAR' THEN conversion_rate_sar WHEN net_price_currency = 'AED' THEN conversion_rate_aed WHEN net_price_currency = 'USD' THEN conversion_rate_usd WHEN net_price_currency = 'QAR' THEN conversion_rate_qar WHEN net_price_currency = 'KWD' THEN conversion_rate_kwd WHEN net_price_currency = 'BHD' THEN conversion_rate_bhd END
        / 
        CASE WHEN sale_price_currency = 'SAR' THEN conversion_rate_sar WHEN sale_price_currency = 'AED' THEN conversion_rate_aed WHEN sale_price_currency = 'USD' THEN conversion_rate_usd WHEN sale_price_currency = 'QAR' THEN conversion_rate_qar WHEN sale_price_currency = 'KWD' THEN conversion_rate_kwd WHEN sale_price_currency = 'BHD' THEN conversion_rate_bhd END
        )) - net_price
END, 2) as markup_total
-- Markup percentage
, round(CASE WHEN sale_price is null OR net_price is null THEN 0
ELSE
    ((sale_price * (
CASE WHEN net_price_currency = 'SAR' THEN conversion_rate_sar WHEN net_price_currency = 'AED' THEN conversion_rate_aed WHEN net_price_currency = 'USD' THEN conversion_rate_usd WHEN net_price_currency = 'QAR' THEN conversion_rate_qar WHEN net_price_currency = 'KWD' THEN conversion_rate_kwd WHEN net_price_currency = 'BHD' THEN conversion_rate_bhd END 
/ 
CASE WHEN sale_price_currency = 'SAR' THEN conversion_rate_sar WHEN sale_price_currency = 'AED' THEN conversion_rate_aed WHEN sale_price_currency = 'USD' THEN conversion_rate_usd WHEN sale_price_currency = 'QAR' THEN conversion_rate_qar WHEN sale_price_currency = 'KWD' THEN conversion_rate_kwd WHEN sale_price_currency = 'BHD' THEN conversion_rate_bhd END
)) - net_price) * 100 / net_price
END, 2) as markup_percentage
-- Markup rate final
, round(CASE WHEN net_price is null THEN 0
ELSE
payment_amount * conversion_rate_sar - 
((net_price / 
CASE WHEN net_price_currency = 'SAR' THEN conversion_rate_sar WHEN net_price_currency = 'AED' THEN conversion_rate_aed WHEN net_price_currency = 'USD' THEN conversion_rate_usd WHEN net_price_currency = 'QAR' THEN conversion_rate_qar WHEN net_price_currency = 'KWD' THEN conversion_rate_kwd WHEN net_price_currency = 'BHD' THEN conversion_rate_bhd END 
) * conversion_rate_sar)
END, 2) as markup_final_amount
-- Markup rate final currency
, 'SAR' as markup_final_currency
-- Markup rate final in USD
, round(CASE WHEN net_price is null THEN 0
ELSE
payment_amount * conversion_rate_usd - 
((net_price / 
CASE WHEN net_price_currency = 'SAR' THEN conversion_rate_sar WHEN net_price_currency = 'AED' THEN conversion_rate_aed WHEN net_price_currency = 'USD' THEN conversion_rate_usd WHEN net_price_currency = 'QAR' THEN conversion_rate_qar WHEN net_price_currency = 'KWD' THEN conversion_rate_kwd WHEN net_price_currency = 'BHD' THEN conversion_rate_bhd END 
) * conversion_rate_usd)
END, 2) as markup_final_amount_usd,
A.inputvat,
A.outputvat,
A.total_vat,
A.dim_channel_id
FROM
(
select * from ${hiveDB}.fact_hotel_orders_intermediate where product_type = 'hotel' and order_no not in('H7061321835','H70620078352','H70629185350','H70706065350','H70719121352','H70806111350','H7101214025','H71212087252','H80123003457','H80123003457','H80125114750'
,'H61212049250','1420499','1423297','H70812187052','H70812184552','H71219174952','H70222066752','H70906124352','H61211021552','1567435','H70808296252','H71220235054','1594133','H71022050752','H71016059252','H61212012052','H71111107857','1835141','H70117004450','H70117004450','H80302089650')) A
LEFT OUTER JOIN
(
select id, max(CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'SUMMER OFFER' or ltrim(rtrim(upper(dim_promo_id))) like 'ENBD OFFERR' then 'ENBD Offer' else  CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL' or ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL OFFER' then 'Visa hotel offer' else dim_promo_id end end ) as dim_promo_id,sum(discount_amount)*-1 as discount_amount from ${hiveDB}.fact_hotel_orders_intermediate where product_type = 'rule' and discount_amount < 0
group by id

) B
ON A.id = B.id;

--=====Single product per order
Drop table if exists ${hiveDB}.tmp_fact_hotel_orders_final_multi;
Create table ${hiveDB}.tmp_fact_hotel_orders_final_multi row format delimited fields terminated by "\t" as 
select 
A.*                            
from ${hiveDB}.fact_hotel_orders_final A
inner join 
(select max(concat(dim_bookingupdated_id,dim_booking_timestamp)) as dim_bookingupdated_id ,id from ${hiveDB}.fact_hotel_orders_final group by id )B
on COALESCE(ltrim(rtrim(concat(A.dim_bookingupdated_id,A.dim_booking_timestamp))),'na') = COALESCE(ltrim(rtrim(B.dim_bookingupdated_id)),'na') and COALESCE(A.id,'na')=COALESCE(B.id,'na');
Drop table if exists ${hiveDB}.fact_hotel_orders_final;
Alter table ${hiveDB}.tmp_fact_hotel_orders_final_multi rename to ${hiveDB}.fact_hotel_orders_final;
--========

Drop table if exists ${hiveDB}.fact_hotel_discounts;
 Create table if not exists ${hiveDB}.fact_hotel_discounts row format delimited fields terminated by '\t' as
select
A.id, 
A.track_id,
A.order_no,
  A.dim_group_id,
  A.order_type,
  A.product_type,
   A.office_id,
  A.dim_product_category_type,
  A.dim_bookingdate_id,
 A.dim_booking_hour,
  A.dim_booking_timestamp,
  A.dim_bookingupdated_id,
  A.dim_firstcancellation_date ,
  A.dim_cancellation_date,
  CASE WHEN (A.dim_bookingpaid_id='null' AND Ltrim(RTRIM(A.dim_bookingtype_id)) ='Pay Now') THEN A.dim_bookingdate_id  else A.dim_bookingpaid_id END  dim_bookingpaid_id,
  A.dim_hotel_id,
  lower(ltrim(rtrim(A.dim_supplier_id))) as dim_supplier_id,
  A.dim_vendor_id,
  A.dim_supplier_currency,
  A.dim_customer_id,
  A.dim_store_id,
  A.dim_language_id,
  A.dim_checkin_date_id,
  A.dim_checkout_date_id,
  A.dim_total_currency,
  A.dim_request_currency_id,
  CASE WHEN ltrim(rtrim(upper(A.dim_promo_id))) like 'SUMMER OFFER' or ltrim(rtrim(upper(A.dim_promo_id))) like 'ENBD OFFERR' then 'ENBD Offer'  else  CASE WHEN ltrim(rtrim(upper(A.dim_promo_id))) like 'VISA HOTEL' or ltrim(rtrim(upper(A.dim_promo_id))) like 'VISA HOTEL OFFER' then 'Visa hotel offer' else A.dim_promo_id end end as dim_promo_id,
  --A.dim_promo_id,
  A.dim_status_id,
  A.dim_state_id,
  A.dim_bookingtype_id,
  A.dim_cancellation_policy,
  A.dim_ratetype,
  A.dim_markup_currency,
  A.dim_paymentstatus_id,
  A.is_manual,
  A.ahs_group_name,
  A.sale_price_currency,                                     
  A.net_price_currency,
 A.payment_type,
A.visa_checkout_flag,                                        
  A.sale_price ,                                               
  A.net_price , 
  A.payment_amount,
  A.booking_value,
  A.supplier_cost,
  A.conversion_rate_aed,
  A.conversion_rate_usd,
  A.conversion_rate_sar,                                       
  A.conversion_rate_kwd,                                       
  A.conversion_rate_qar,                                       
  A.conversion_rate_bhd, 
  A.room_count,
  A.length_of_stay,
  A.booking_window,
  A.pax_count,
  round(A.discount_amount,2) as discount_amount,
  round(A.payment_amount * A.conversion_rate_usd,2) as payment_amount_usd,
  round(A.discount_amount * A.conversion_rate_usd,2) as discount_amount_usd,
  CASE WHEN A.discount_amount < 0 Then 'Discount' else 'TAX' End discount_type,
  COALESCE(A.length_of_stay*A.room_count,0) as room_nights
from ${hiveDB}.fact_hotel_orders_intermediate A where A.product_type='rule' ;



DROP TABLE IF EXISTS ${hiveDB}.fact_flight_orders_final;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_flight_orders_final ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT
A.id,
A.track_id,
A.order_type,
A.product_type,
A.dim_group_id,
A.order_no,
A.dim_product_category_type,
A.dim_office_id,
A.dim_bookingdate_id,
A.dim_booking_hour,
A.dim_booking_timestamp,
A.dim_travel_date,
A.dim_arrival_date,
A.dim_travel_hour,
A.dim_bookingupdated_id, 
A.dim_firstcancellation_date ,
A.dim_cancellation_date,
A.dim_bookingpaid_id,
A.dim_store_id,
B.dim_promo_id,
A.dim_customer_id,
A.dim_vendor_id,
A.dim_language,
A.dim_origin,
A.dim_destination,
A.dim_flightcode_leg1,
A.dim_airline_leg1,
A.dim_flightcode_leg2,
A.dim_airlinecode_leg2,
A.dim_trip_type,
A.dim_journey_type,
A.dim_totals_currency,
A.dim_status_id,
A.dim_paymentstatus_id,
A.dim_state_id,
A.dim_bookingtype_id,
A.is_manual,
A.carrier_type,
A.payment_type,
A.visa_checkout_flag,
A.client_os,
A.cabin_class,
COALESCE(regexp_replace(regexp_replace(lower(A.deal_amount),' ',''),'/- sar',''),0.0) as deal_amount,
COALESCE(regexp_replace(regexp_replace(lower(A.loss_amount),' ',''),'/- sar',''),0.0) as loss_amount,
A.devicetype,
A.xplatform,
A.rbd,
A.payment_method,
case when xplatform = 'ios' or xplatform = 'android' then 'native app'
     when devicetype = 'Smartphone' or devicetype = 'Tablet' or devicetype = 'Smart TV' or devicetype = 'Game console' then 'mobile_web'
     when devicetype = 'Personal computer' then 'web'
     else 'mobile_web'
     end as device_category,
A.conversion_rate_aed,
A.conversion_rate_usd,
A.payment_amount,
A.adult_count,
A.children_count,
A.infants_count,
A.no_of_passengers,
A.total_seat,
A.no_of_legs,
(A.no_of_segments*(A.adult_count+A.children_count)) as no_of_segments,
round(COALESCE(B.discount_amount, 0.0),2) as discount_amount,
A.booking_window,
round(A.payment_amount * A.conversion_rate_usd,2) as payment_amount_usd,
round(COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd,2) as discount_amount_usd,
round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd),2) as ibv,
CASE WHEN ltrim(rtrim(upper(A.dim_state_id))) like '%FRAUD%' then 0.0 else CASE WHEN ltrim(rtrim(A.dim_status_id)) in ('91', '94') then 0.0 else round((A.payment_amount * A.conversion_rate_usd) + (COALESCE(B.discount_amount, 0.0) * A.conversion_rate_usd),2) end end as gbv,
datediff(from_unixtime(unix_timestamp(A.dim_arrival_date, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(A.dim_travel_date, 'yyyyMMdd'), 'yyyy-MM-dd')) as length_of_trip,A.tax,A.base_amount,A.inputvat,A.outputvat,A.total_vat,A.gds_nongds,A.dim_channel_id
FROM
(
  select * from ${hiveDB}.fact_flight_orders_vat where ltrim(rtrim(product_type)) = 'flight'
) A
LEFT OUTER JOIN
-- (
--   select order_no, dim_customer_id,  max(CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'SUMMER OFFER' or ltrim(rtrim(upper(dim_promo_id))) like 'ENBD OFFERR' then 'ENBD Offer' else CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL' or ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL OFFER' then 'Visa hotel offer' else dim_promo_id end end ) as dim_promo_id, sum(discount_amount)*-1 as discount_amount,dim_booking_timestamp,dim_bookingdate_id from ${hiveDB}.fact_flight_orders_vat where ltrim(rtrim(product_type)) = 'rule' and discount_amount <  0
--  group by order_no ,dim_booking_timestamp,dim_bookingdate_id,dim_customer_id
-- ) B
-- ON A.order_no = B.order_no and A.dim_booking_timestamp = B.dim_booking_timestamp and A.dim_bookingdate_id = B.dim_bookingdate_id and A.dim_customer_id = B.dim_customer_id
(
  select id, max(CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'SUMMER OFFER' or ltrim(rtrim(upper(dim_promo_id))) like 'ENBD OFFERR' then 'ENBD Offer' else CASE WHEN ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL' or ltrim(rtrim(upper(dim_promo_id))) like 'VISA HOTEL OFFER' then 'Visa hotel offer' else dim_promo_id end end ) as dim_promo_id, sum(discount_amount)*-1 as discount_amount from ${hiveDB}.fact_flight_orders_vat where ltrim(rtrim(product_type)) = 'rule' and discount_amount <  0
  group by id
) B on A.id = B.id
;



Drop table if exists ${hiveDB}.fact_flight_discounts;
Create table if not exists ${hiveDB}.fact_flight_discounts row format delimited fields terminated by '\t' as 
select
A.id,
A.track_id,
A.order_type,
A.product_type,
A.dim_group_id,
A.order_no,
A.dim_product_category_type,
A.dim_office_id,
A.dim_bookingdate_id,
A.dim_booking_hour,
A.dim_booking_timestamp,
A.dim_travel_date,
A.dim_arrival_date,
A.dim_travel_hour,
A.dim_bookingupdated_id, 
A.dim_firstcancellation_date ,
A.dim_cancellation_date,
A.dim_bookingpaid_id,
A.dim_store_id,
CASE WHEN ltrim(rtrim(upper(A.dim_promo_id))) like 'SUMMER OFFER' or ltrim(rtrim(upper(dim_promo_id))) like 'ENBD OFFERR' then 'ENBD Offer' else  CASE WHEN ltrim(rtrim(upper(A.dim_promo_id))) like 'VISA HOTEL' or ltrim(rtrim(upper(A.dim_promo_id))) like 'VISA HOTEL OFFER' then 'Visa hotel offer' else A.dim_promo_id end end as dim_promo_id,
--A.dim_promo_id,
A.dim_customer_id,
A.dim_vendor_id,
A.dim_language,
A.dim_origin,
A.dim_destination,
B.dim_flightcode_leg1,
B.dim_airline_leg1,
B.dim_flightcode_leg2,
B.dim_airlinecode_leg2,
A.dim_trip_type,
A.dim_journey_type,
A.dim_totals_currency,
A.dim_status_id,
A.dim_paymentstatus_id,
A.dim_state_id,
A.dim_bookingtype_id,
A.is_manual,
A.carrier_type,
A.payment_type,
A.visa_checkout_flag,
A.conversion_rate_aed,
A.conversion_rate_usd,
A.payment_amount,
A.adult_count,
A.children_count,
A.infants_count,
A.no_of_passengers,
A.total_seat,
B.no_of_legs,
B.no_of_segments,
round(A.discount_amount,2) as discount_amount,
A.booking_window,
round(A.payment_amount * A.conversion_rate_usd,2) as payment_amount_usd,
round(A.discount_amount * A.conversion_rate_usd,2) as discount_amount_usd,
CASE when A.discount_amount < 0 Then 'Discount' else 'TAX' End discount_type
from  ${hiveDB}.fact_flight_orders_vat A 
LEFT OUTER JOIN
(
  select order_no, max(no_of_legs) as no_of_legs,max(no_of_segments) as no_of_segments,
  max(case when dim_flightcode_leg1 = 'null' then '-1' else coalesce(dim_flightcode_leg1, '-1') end) as dim_flightcode_leg1,
  max(case when dim_airline_leg1 = 'null' then '-1' else coalesce(dim_airline_leg1, '-1') end) as dim_airline_leg1,
  max(case when dim_flightcode_leg2 = 'null' then '-1' else coalesce(dim_flightcode_leg2, '-1') end) as dim_flightcode_leg2,
  max(case when dim_airlinecode_leg2 = 'null' then '-1' else coalesce(dim_airlinecode_leg2, '-1') end) as dim_airlinecode_leg2 from ${hiveDB}.fact_flight_orders_vat
 group by order_no
) B
ON A.order_no = B.order_no
where A.product_type='rule';

-- CREATE NEW FACT TABLE FOR DAY LEVEL AGGREGATED IBV, GBV and COUNT OF RECORDS (Remove orderno and customerid)

DROP TABLE IF EXISTS ${hiveDB}.fact_hotel_daily_aggregated_kpis;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_hotel_daily_aggregated_kpis row format delimited fields terminated by '\t' as
SELECT A.*, COALESCE(B.gbv, 0.0) as gbv
FROM
(
select dim_group_id,order_type,product_type, dim_product_category_type, max(case when office_id = 'null' then '0' else office_id end) as office_id,
dim_bookingdate_id, max(dim_hotel_id) as dim_hotel_id, max(case when dim_supplier_id = 'null' then 0 else dim_supplier_id end) as dim_supplier_id,
max(case when dim_vendor_id = 'null' then 0 else dim_vendor_id end) as dim_vendor_id, max(dim_supplier_currency) as dim_supplier_currency,
dim_store_id, dim_language_id, max(dim_checkin_date_id) as dim_checkin_date_id, max(dim_checkout_date_id) as dim_checkout_date_id,
max(dim_total_currency) as dim_total_currency, dim_status_id as dim_status_id,
max(dim_state_id) as dim_state_id, max(dim_bookingtype_id) as dim_bookingtype_id, max(dim_paymentstatus_id) as dim_paymentstatus_id,
sum(payment_amount) as payment_amount, sum(booking_value) as booking_value,sum(supplier_cost) as supplier_cost,
sum(room_count) as room_count, avg(length_of_stay) as length_of_stay, avg(booking_window) as booking_window,
sum(pax_count) as pax_count, sum(discount_amount) as discount_amount, sum(payment_amount_usd) as payment_amount_usd,
sum(discount_amount_usd) as discount_amount_usd, sum(ibv) as ibv, count(distinct(concat(order_no, dim_customer_id))) as bookings_count
, sum(markup_total) as markup_total, avg(markup_percentage) as markup_percentage, sum(markup_final_amount) as markup_final_amount
, max(markup_final_currency) as markup_final_currency, sum(markup_final_amount_usd) as markup_final_amount_usd
FROM ${hiveDB}.fact_hotel_orders_final
group by dim_status_id,dim_group_id,order_type,product_type, dim_product_category_type, dim_bookingdate_id, dim_store_id, dim_language_id
) A LEFT OUTER JOIN
(
SELECT
dim_group_id,order_type,product_type, dim_product_category_type, max(case when office_id = 'null' then '0' else office_id end) as office_id,
dim_bookingpaid_id, max(dim_hotel_id) as dim_hotel_id, max(case when dim_supplier_id = 'null' then 0 else dim_supplier_id end) as dim_supplier_id,
max(case when dim_vendor_id = 'null' then 0 else dim_vendor_id end) as dim_vendor_id, max(dim_supplier_currency) as dim_supplier_currency,
dim_store_id, dim_language_id, max(dim_checkin_date_id) as dim_checkin_date_id, max(dim_checkout_date_id) as dim_checkout_date_id,
max(dim_total_currency) as dim_total_currency, dim_status_id as dim_status_id,
max(dim_state_id) as dim_state_id, max(dim_bookingtype_id) as dim_bookingtype_id, max(dim_paymentstatus_id) as dim_paymentstatus_id,
sum(gbv) as gbv
FROM ${hiveDB}.fact_hotel_orders_final
group by
dim_status_id,dim_group_id,order_type,product_type, dim_product_category_type, dim_bookingpaid_id, dim_store_id, dim_language_id
) B
ON
A.dim_group_id = B.dim_group_id and A.order_type = B.order_type and A.product_type = B.product_type 
and A.dim_product_category_type = B.dim_product_category_type and 
B.dim_bookingpaid_id = A.dim_bookingdate_id and A.dim_store_id = B.dim_store_id and 
A.dim_language_id = B.dim_language_id
and A.dim_status_id = B.dim_status_id
;

DROP TABLE IF EXISTS ${hiveDB}.fact_flight_daily_aggregated_kpis;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_flight_daily_aggregated_kpis row format delimited fields terminated by '\t' as
SELECT A.*, COALESCE(B.gbv, 0.0) as gbv FROM
(
SELECT
order_type,product_type,dim_group_id,dim_product_category_type,dim_office_id,dim_bookingdate_id,dim_travel_date,dim_arrival_date,dim_store_id,dim_language,dim_origin,dim_destination,dim_flightcode_leg1,dim_airline_leg1,dim_flightcode_leg2,dim_airlinecode_leg2,dim_trip_type,dim_journey_type,dim_totals_currency,dim_status_id,dim_paymentstatus_id,dim_state_id,dim_bookingtype_id,
sum(payment_amount) as payment_amount,sum(adult_count) as adult_count,sum(children_count) as children_count,sum(infants_count) as infants_count, sum(no_of_passengers) as no_of_passengers, sum(total_seat) as total_seat, sum(no_of_legs) as no_of_legs, sum(no_of_segments) as no_of_segments, sum(discount_amount) as discount_amount, sum(booking_window) as booking_window, sum(payment_amount_usd) as payment_amount_usd, sum(discount_amount_usd) as discount_amount_usd, sum(ibv) as ibv, sum(length_of_trip) as length_of_trip, count(distinct(order_no)) as bookings_count
FROM ${hiveDB}.fact_flight_orders_final
GROUP BY
order_type,product_type,dim_group_id,dim_product_category_type,dim_office_id,dim_bookingdate_id,dim_travel_date,dim_arrival_date,dim_store_id,dim_language,dim_origin,dim_destination,dim_flightcode_leg1,dim_airline_leg1,dim_flightcode_leg2,dim_airlinecode_leg2,dim_trip_type,dim_journey_type,dim_totals_currency,dim_status_id,dim_paymentstatus_id,dim_state_id,dim_bookingtype_id
) A
LEFT OUTER JOIN
(
SELECT
order_type,product_type,dim_group_id,dim_product_category_type,dim_office_id,dim_bookingpaid_id,dim_travel_date,dim_arrival_date,dim_store_id,dim_language,dim_origin,dim_destination,dim_flightcode_leg1,dim_airline_leg1,dim_flightcode_leg2,dim_airlinecode_leg2,dim_trip_type,dim_journey_type,dim_totals_currency,dim_status_id,dim_paymentstatus_id,dim_state_id,dim_bookingtype_id,
sum(gbv) as gbv
FROM ${hiveDB}.fact_flight_orders_final
GROUP BY
order_type,product_type,dim_group_id,dim_product_category_type,dim_office_id,dim_bookingpaid_id,dim_travel_date,dim_arrival_date,dim_store_id,dim_language,dim_origin,dim_destination,dim_flightcode_leg1,dim_airline_leg1,dim_flightcode_leg2,dim_airlinecode_leg2,dim_trip_type,dim_journey_type,dim_totals_currency,dim_status_id,dim_paymentstatus_id,dim_state_id,dim_bookingtype_id
) B
ON
A.order_type = B.order_type and A.product_type = B.product_type and A.dim_group_id = B.dim_group_id and A.dim_product_category_type = B.dim_product_category_type and A.dim_office_id = B.dim_office_id and B.dim_bookingpaid_id = A.dim_bookingdate_id and A.dim_travel_date = B.dim_travel_date and A.dim_arrival_date = B.dim_arrival_date and A.dim_store_id = B.dim_store_id and A.dim_language = B.dim_language and A.dim_origin = B.dim_origin and A.dim_destination = B.dim_destination and A.dim_flightcode_leg1 = B.dim_flightcode_leg1 and A.dim_airline_leg1 = B.dim_airline_leg1 and A.dim_flightcode_leg2 = B.dim_flightcode_leg2 and A.dim_airlinecode_leg2 = B.dim_airlinecode_leg2 and A.dim_trip_type = B.dim_trip_type and A.dim_journey_type = B.dim_journey_type and A.dim_totals_currency = B.dim_totals_currency and A.dim_status_id = B.dim_status_id and A.dim_paymentstatus_id = B.dim_paymentstatus_id and A.dim_state_id = B.dim_state_id and A.dim_bookingtype_id = B.dim_bookingtype_id
;

DROP TABLE IF EXISTS ${hiveDB}.fact_daily_aggregated_kpis;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_daily_aggregated_kpis row format delimited fields terminated by '\t' as
select * from
(
select 
order_type, product_type, dim_group_id, dim_office_id, dim_bookingdate_id, dim_store_id, dim_language, 
dim_totals_currency, dim_status_id, dim_paymentstatus_id, dim_state_id, dim_bookingtype_id,
sum(payment_amount) as payment_amount, sum(discount_amount) as discount_amount, sum(payment_amount_usd) as payment_amount_usd,
sum(discount_amount_usd) as discount_amount_usd, sum(ibv) as ibv, sum(bookings_count) as bookings_count, sum(gbv) as gbv
from ${hiveDB}.fact_flight_daily_aggregated_kpis
group by order_type, product_type, dim_group_id, dim_office_id, dim_bookingdate_id, dim_store_id, dim_language, 
dim_totals_currency, dim_status_id, dim_paymentstatus_id, dim_state_id, dim_bookingtype_id
union all
select
order_type, product_type, dim_group_id, office_id as dim_office_id, dim_bookingdate_id, dim_store_id, dim_language_id as dim_language, 
dim_total_currency as dim_totals_currency, dim_status_id, dim_paymentstatus_id, dim_state_id, dim_bookingtype_id,
sum(payment_amount) as payment_amount, sum(discount_amount) as discount_amount, sum(payment_amount_usd) as payment_amount_usd,
sum(discount_amount_usd) as discount_amount_usd, sum(ibv) as ibv, sum(bookings_count) as bookings_count, sum(gbv) as gbv
from ${hiveDB}.fact_hotel_daily_aggregated_kpis
group by order_type, product_type, dim_group_id, office_id, dim_bookingdate_id, dim_store_id, dim_language_id, dim_total_currency,
dim_status_id, dim_paymentstatus_id, dim_state_id, dim_bookingtype_id
) A;

-- ==============================================================
drop TABLE IF EXISTS ${hiveDB}.tmp_all_discounts;
create TABLE IF not exists  ${hiveDB}.tmp_all_discounts as select * from (
    select order_no, dim_promo_id, dim_product_category_type from ${hiveDB}.fact_hotel_discounts where discount_amount < 0 and dim_status_id != 91
    union
    select order_no, dim_promo_id, dim_product_category_type from ${hiveDB}.fact_flight_discounts where discount_amount < 0 and dim_status_id != 91
) A;

drop table if exists ${hiveDB}.tmp_discount_orders;
create table if not exists ${hiveDB}.tmp_discount_orders as
select order_no, max(dim_promo_id) as first_promo_id from (
select da.order_no, da.dim_promo_id from
(
    select order_no, count(*) as total from ${hiveDB}.tmp_all_discounts group by order_no having total = 1
) T1 inner join ${hiveDB}.tmp_all_discounts da on T1.order_no = da.order_no
union
select da.order_no, da.dim_promo_id from
(
    select order_no, count(*) as total from ${hiveDB}.tmp_all_discounts where dim_product_category_type = 'rule' group by order_no having total > 1
) T2 inner join ${hiveDB}.tmp_all_discounts da on T2.order_no = da.order_no and da.dim_product_category_type = 'rule' and lower(da.dim_promo_id) = 'alrajhi'
union
select da.order_no, da.dim_promo_id from
(
    select order_no, count(*) as total from ${hiveDB}.tmp_all_discounts group by order_no having total > 1
) T2 inner join ${hiveDB}.tmp_all_discounts da on T2.order_no = da.order_no and da.dim_product_category_type = 'coupon' and lower(da.dim_promo_id) != 'alrajhi'
) A group by order_no
;


--- ===== Update dim_customer_almosafer table with first transaction date in it ===== ---
DROP TABLE IF EXISTS ${hiveDB}.intermediate_dim_customer_almosafer;
CREATE TABLE ${hiveDB}.intermediate_dim_customer_almosafer 
--row format delimited fields terminated by '\t' 
as
select 
dc.dim_customer_id, max(dc.email) as email, max(dc.firstname) as firstname, max(dc.lastname) as lastname, max(dc.phone) as phone, 
max(dc.title) as title, max(dc.country) as country, max(from_unixtime(unix_timestamp(dc.first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMdd')) as first_order_date,
max(dc.first_order_group) as first_order_group, max(dc.first_product) as first_product 
,max(dc.first_store_id) as first_store_id, max(dc.first_order_no) as first_order_no, max(dc.status) as status,max(do.first_promo_id)as first_promo_id
FROM(
select * from ${hiveDB}.dim_customer_raw where status !='91' 
--and status != '94' 
and first_order_group = 2 
) dc
inner join
(
select dim_customer_id,
min(from_unixtime(unix_timestamp(first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMddhhmmss')) as first_order_date
from ${hiveDB}.dim_customer_raw where status !='91' 
--and status != '94' 
and first_order_group = 2
group by dim_customer_id
) MIN_DATE
ON dc.dim_customer_id = MIN_DATE.dim_customer_id AND 
from_unixtime(unix_timestamp(dc.first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMddhhmmss') = MIN_DATE.first_order_date
left outer join ${hiveDB}.tmp_discount_orders do on dc.first_order_no=do.order_no 
group by dc.dim_customer_id
;
DROP TABLE IF EXISTS ${hiveDB}.dim_customer_almosafer;
ALTER TABLE ${hiveDB}.intermediate_dim_customer_almosafer RENAME TO ${hiveDB}.dim_customer_almosafer;

--- ===== Update dim_customer_tajawal table with first transaction date in it ===== ---
DROP TABLE IF EXISTS ${hiveDB}.intermediate_dim_customer_tajawal;
CREATE TABLE ${hiveDB}.intermediate_dim_customer_tajawal 
--row format delimited fields terminated by '\t' 
as
select 
dc.dim_customer_id, max(dc.email) as email, max(dc.firstname) as firstname, max(dc.lastname) as lastname, max(dc.phone) as phone, 
max(dc.title) as title, max(dc.country) as country, max(from_unixtime(unix_timestamp(dc.first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMdd')) as first_order_date,
max(dc.first_order_group) as first_order_group, max(dc.first_product) as first_product 
,max(dc.first_store_id) as first_store_id, max(dc.first_order_no) as first_order_no, max(dc.status) as status,max(do.first_promo_id) as first_promo_id
FROM(
select * from ${hiveDB}.dim_customer_raw where status !='91' 
--and status != '94' 
and first_order_group = 1
) dc
inner join
(
select dim_customer_id,
min(from_unixtime(unix_timestamp(first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMddhhmmss')) as first_order_date
from ${hiveDB}.dim_customer_raw where status !='91' 
--and status != '94' 
and first_order_group = 1
group by dim_customer_id
) MIN_DATE
ON dc.dim_customer_id = MIN_DATE.dim_customer_id AND 
from_unixtime(unix_timestamp(dc.first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMddhhmmss') = MIN_DATE.first_order_date
left outer join ${hiveDB}.tmp_discount_orders do on dc.first_order_no=do.order_no
group by dc.dim_customer_id
;
DROP TABLE IF EXISTS ${hiveDB}.dim_customer_tajawal;
ALTER TABLE ${hiveDB}.intermediate_dim_customer_tajawal RENAME TO ${hiveDB}.dim_customer_tajawal;

--- ===== Update dim_customer table with first transaction date in it ===== ---
DROP TABLE IF EXISTS ${hiveDB}.intermediate_dim_customer;
CREATE TABLE ${hiveDB}.intermediate_dim_customer row format delimited fields terminated by '\t' as
select 
dc.dim_customer_id, max(dc.email) as email, max(dc.firstname) as firstname, max(dc.lastname) as lastname, max(dc.phone) as phone, 
max(dc.title) as title, max(dc.country) as country, max(from_unixtime(unix_timestamp(dc.first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMdd')) as first_order_date,
max(dc.first_order_group) as first_order_group, max(dc.first_product) as first_product 
,max(dc.first_store_id) as first_store_id, max(dc.first_order_no) as first_order_no, max(dc.status) as status,max(do.first_promo_id) as first_promo_id
FROM(
select * from ${hiveDB}.dim_customer_raw where status !='91' 
--and status != '94' 
) dc
inner join
(
select dim_customer_id,
min(from_unixtime(unix_timestamp(first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMddhhmmss')) as first_order_date
from ${hiveDB}.dim_customer_raw where status !='91' 
--and status != '94'
group by dim_customer_id
) MIN_DATE
ON dc.dim_customer_id = MIN_DATE.dim_customer_id AND 
from_unixtime(unix_timestamp(dc.first_order_date, 'EEE MMM dd hh:mm:ss zzz yyyy'), 'yyyyMMddhhmmss') = MIN_DATE.first_order_date
left outer join ${hiveDB}.tmp_discount_orders do on dc.first_order_no=do.order_no
group by dc.dim_customer_id
;
DROP TABLE IF EXISTS ${hiveDB}.dim_customer;
ALTER TABLE ${hiveDB}.intermediate_dim_customer RENAME TO ${hiveDB}.dim_customer;

--============Addition of dim_channel_id into dim_customer
DROP TABLE IF EXISTS ${hiveDB}.intermediate_dim_customer;
CREATE TABLE ${hiveDB}.intermediate_dim_customer row format delimited fields terminated by '\t' as
select A.*,D.dim_channel_id from ${hiveDB}.dim_customer A 
LEFT OUTER JOIN (select transaction_id,max(dim_channel_id) as dim_channel_id from ${hiveDB}.fact_google_analytics_transactions_final group by transaction_id) D 
ON A.first_order_no=D.transaction_id
;
DROP TABLE IF EXISTS ${hiveDB}.dim_customer;
ALTER TABLE ${hiveDB}.intermediate_dim_customer RENAME TO ${hiveDB}.dim_customer;

--==================
--============Addition of dim_channel_id into dim_customer_almosafer
DROP TABLE IF EXISTS ${hiveDB}.intermediate_dim_customer_almosafer;
CREATE TABLE ${hiveDB}.intermediate_dim_customer_almosafer 
--row format delimited fields terminated by '\t' 
as
select A.*,D.dim_channel_id from ${hiveDB}.dim_customer_almosafer A
LEFT OUTER JOIN (select transaction_id,max(dim_channel_id) as dim_channel_id from ${hiveDB}.fact_google_analytics_transactions_final group by transaction_id) D
ON A.first_order_no=D.transaction_id
;
DROP TABLE IF EXISTS ${hiveDB}.dim_customer_almosafer;
ALTER TABLE ${hiveDB}.intermediate_dim_customer_almosafer RENAME TO ${hiveDB}.dim_customer_almosafer;

--==================Addition of dim_channel_id into dim_customer_tajawal
DROP TABLE IF EXISTS ${hiveDB}.intermediate_dim_customer_tajawal;
CREATE TABLE ${hiveDB}.intermediate_dim_customer_tajawal 
--row format delimited fields terminated by '\t'
as
select A.*,D.dim_channel_id from ${hiveDB}.dim_customer_tajawal A
LEFT OUTER JOIN (select transaction_id,max(dim_channel_id) as dim_channel_id from ${hiveDB}.fact_google_analytics_transactions_final group by transaction_id) D
ON A.first_order_no=D.transaction_id
;
DROP TABLE IF EXISTS ${hiveDB}.dim_customer_tajawal;
ALTER TABLE ${hiveDB}.intermediate_dim_customer_tajawal RENAME TO ${hiveDB}.dim_customer_tajawal;

--====================

drop table if exists ${hiveDB}.tmp_dim_hotel;

create table ${hiveDB}.tmp_dim_hotel  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as select h.dim_hotel_id, h.name, h.address, h.city, h.district, h.country_code, h.star_rating, h.longitude, h.latitude, h.zipcode, max(gh.chain) as chain, max(gh.chain_name) as chain_name from ${hiveDB}.dim_hotel_raw h left outer join ${hiveDB}.dim_hotel_global gh on REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(h.name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') = REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(gh.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') group by h.dim_hotel_id, h.name, h.address, h.city, h.district, h.country_code, h.star_rating, h.longitude, h.latitude, h.zipcode;

Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel;
-- =======================For Altayyar offline data
drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select  dim_hotel_id, max(name) as name,
case when max(address) = '-1' then null else max(address)  end address,
case when max(city) = '-1' then null else max(case when ltrim(rtrim(city)) ='null' then '-1' else city end) end city,
case when max(district) = '-1' then null else max(district) end district ,
case when max(country_code) = '-1' then null else max(case when ltrim(rtrim(country_code))='null' then '-1' else country_code end)end  country_code,
case when max(star_rating) = '-1' then null else max(star_rating) end   star_rating,
case when max(longitude) = '-1' then null else max(longitude)  end longitude ,
case when max(latitude) = '-1' then null else max(latitude)   end latitude,
case when max(zipcode) = '-1' then null else max(zipcode)  end zipcode, 
case when max(chain) = '-1' then null else max(chain)  end chain,
case when max(chain_name) = '-1' then null else max(chain_name) end chain_name from (
Select * from ${hiveDB}.dim_hotel
UNION
select  MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(h.hotel_name),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.hotel_city,',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.hotel_city,',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL',''))) as dim_hotel_id
,h.hotel_name as name,
'-1' as address,
coalesce(h.hotel_city, 'null') as city,
'-1' as district,
'-1' as country_code,
'-1' as star_rating,
'-1' as longitude,
'-1' as latitude,
'-1' as zipcode,
max(gh.chain) as chain,
max(gh.chain_name) as chain_name from ${hiveDB}.tours_hotel h left outer join ${hiveDB}.dim_hotel_global gh on REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(h.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') = REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(gh.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') group by dim_hotel_id,h.hotel_name,h.hotel_city
)T group by dim_hotel_id; 
Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel;
--=======================================
-- =======================For Almosafer 2016 data
drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select  dim_hotel_id, max(name) as name,
case when max(address) = '-1' then null else max(address)  end address,
case when max(city) = '-1' then null else max(case when ltrim(rtrim(city)) ='null' then '-1' else city end) end city,
case when max(district) = '-1' then null else max(district) end district ,
case when max(country_code) = '-1' then null else max(case when ltrim(rtrim(country_code))='null' then '-1' else country_code end) end  country_code,
case when max(star_rating) = '-1' then null else max(star_rating) end   star_rating,
case when max(longitude) = '-1' then null else max(longitude)  end longitude ,
case when max(latitude) = '-1' then null else max(latitude)   end latitude,
case when max(zipcode) = '-1' then null else max(zipcode)  end zipcode,
case when max(chain) = '-1' then null else max(chain)  end chain,
case when max(chain_name) = '-1' then null else max(chain_name) end chain_name from (
Select * from ${hiveDB}.dim_hotel
UNION
select MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(h.hotel_name),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.destination,',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.destination,',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL','')))  as dim_hotel_id
,h.hotel_name as name,
'-1' as address,
coalesce(h.destination, 'null') as city,
'-1' as district,
h.country_code as country_code,
'-1' as star_rating,
'-1' as longitude,
'-1' as latitude,
'-1' as zipcode,
max(gh.chain) as chain,
max(gh.chain_name) as chain_name from ${hiveDB}.fact_almosafer_2016 h left outer join ${hiveDB}.dim_hotel_global gh on REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(h.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') = REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(gh.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') group by dim_hotel_id,h.hotel_name,h.destination,h.country_code
)T group by dim_hotel_id;
Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel;

--=======================================
-- =======================For Eagle travels data
drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select  dim_hotel_id, max(name) as name,
case when max(address) = '-1' then null else max(address)  end address,
case when max(city) = '-1' then null else max(case when ltrim(rtrim(city)) ='null' then '-1' else city end) end city,
case when max(district) = '-1' then null else max(district) end district ,
case when max(country_code) = '-1' then null else max(case when ltrim(rtrim(country_code))='null' then '-1' else country_code end) end  country_code,
case when max(star_rating) = '-1' then null else max(star_rating) end   star_rating,
case when max(longitude) = '-1' then null else max(longitude)  end longitude ,
case when max(latitude) = '-1' then null else max(latitude)   end latitude,
case when max(zipcode) = '-1' then null else max(zipcode)  end zipcode,
case when max(chain) = '-1' then null else max(chain)  end chain,
case when max(chain_name) = '-1' then null else max(chain_name) end chain_name from (
Select * from ${hiveDB}.dim_hotel
UNION
select  MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(h.concept),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.city,',')[0]),'\n|\\*',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.city,',')[0]),'\n|\\*',''),'\t',''),'"',''),' ',''),'-',''),'NULL',''))) as dim_hotel_id,
h.concept as name,
'-1' as address,
coalesce(h.city, 'null') as city,
'-1' as district,
'-1' as country_code,
'-1' as star_rating,
'-1' as longitude,
'-1' as latitude,
'-1' as zipcode,
max(gh.chain) as chain,
max(gh.chain_name) as chain_name from ${hiveDB}.eagle_travels h left outer join ${hiveDB}.dim_hotel_global gh on REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(h.concept), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') = REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(gh.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') group by dim_hotel_id,h.concept,h.city
)T group by dim_hotel_id;
Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel;

-- =======================For call center hotels data
drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select  dim_hotel_id, max(name) as name,
case when max(address) = '-1' then null else max(address)  end address,
case when max(city) = '-1' then null else max(case when ltrim(rtrim(city)) ='null' then '-1' else city end) end city,
case when max(district) = '-1' then null else max(district) end district ,
case when max(country_code) = '-1' then null else max(case when ltrim(rtrim(country_code))='null' then '-1' else country_code end) end  country_code,
case when max(star_rating) = '-1' then null else max(star_rating) end   star_rating,
case when max(longitude) = '-1' then null else max(longitude)  end longitude ,
case when max(latitude) = '-1' then null else max(latitude)   end latitude,
case when max(zipcode) = '-1' then null else max(zipcode)  end zipcode,
case when max(chain) = '-1' then null else max(chain)  end chain,
case when max(chain_name) = '-1' then null else max(chain_name) end chain_name
from (
Select * from ${hiveDB}.dim_hotel
UNION
select  MD5(concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(h.hotel_name),'\n|\&',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.destination,',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(h.destination,',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL',''))) as dim_hotel_id,
h.hotel_name as name,
'-1' as address,
COALESCE(h.destination, 'null') as city,
'-1' as district,
'-1' as country_code,
'-1' as star_rating,
'-1' as longitude,
'-1' as latitude,
'-1' as zipcode,
max(gh.chain) as chain,
max(gh.chain_name) as chain_name
from ${hiveDB}.call_center_hotel_orders h left outer join ${hiveDB}.dim_hotel_global gh on REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(h.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') = REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(gh.hotel_name), ' ', ''), '\t', ''), '\r', ''), 'HOTEL', ''), 'AND', ''), '&', '') group by dim_hotel_id,h.hotel_name,h.destination
)T group by dim_hotel_id;
Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel;

--========================To get clean city names
drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
select
dim_hotel_id,                 
 name,                         
 address,                       
case when city='-1' then 'NA' else regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(split(COALESCE(city, 'null'),',')[0],'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL','') end city,                          
 district,                      
 country_code,                  
 star_rating,                   
 longitude,                     
 latitude,                      
 zipcode,                       
 chain,                         
 chain_name
 from ${hiveDB}.dim_hotel ;
 Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel ;

--=================For dim_hotel_city mapping 

drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select
a.dim_hotel_id,
 a.name,
 a.address,
lower(COALESCE(c.city,a.city)) as city,
 district,
 lower(COALESCE(c.country,a.country_code)) as country_code,
 star_rating,
 longitude,
 latitude,
 zipcode,
 case when regexp_replace(COALESCE(c.chain_code,'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(c.chain_code,'na'), '#|N/A', '') end as chain,
 case when regexp_replace(COALESCE(c.chain_name,'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(c.chain_name,'na'), '#|N/A', '') end as chain_name,
 case when regexp_replace(COALESCE(c.hotel_brand,'na'), '#|N/A', '') = '' then 'na' else regexp_replace(COALESCE(c.hotel_brand,'na'), '#|N/A', '') end as hotel_brand
 from ${hiveDB}.dim_hotel a
 left outer join (select dim_hotel_id, max(city) as city, max(country) as country, max(chain_code) as chain_code, max(chain_name) as chain_name, max(hotel_brand) as hotel_brand from googlesheets.dim_hotel_chain_mapping group by dim_hotel_id) c
 on lower(regexp_replace(a.dim_hotel_id, ' ', '')) = lower(regexp_replace(c.dim_hotel_id, ' ', ''))
 ;

 Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel ;

--=================For clean city mapping 

drop table if exists ${hiveDB}.tmp_dim_hotel;
create table ${hiveDB}.tmp_dim_hotel ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
select
a.dim_hotel_id,
 a.name,
 a.address,
lower(COALESCE(b.cleaned_city_name,a.city)) as city,
 district,
 country_code,
 star_rating,
 longitude,
 latitude,
 zipcode,
 chain,
 chain_name,
 hotel_brand
 from ${hiveDB}.dim_hotel a
 left outer join
 (select regexp_replace(ltrim(rtrim(lower(city_name))), ' ','') as city_name, max(cleaned_city_name) as cleaned_city_name from googlesheets.dim_hotel_city_cleanup group by regexp_replace(ltrim(rtrim(lower(city_name))), ' ','')) b
 on regexp_replace(ltrim(rtrim(lower(a.city))),' ','')=regexp_replace(ltrim(rtrim(lower(b.city_name))),' ','')
 ;

 Drop table if exists ${hiveDB}.dim_hotel;
Alter table ${hiveDB}.tmp_dim_hotel RENAME TO ${hiveDB}.dim_hotel ;


--========================For removing deduplicate records
Drop table if exists ${hiveDB}.dim_hotel_inter;
Create table if not exists ${hiveDB}.dim_hotel_inter ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as select concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(h.name),'\n',''),'\t',''), 'THE ', ''), 'HOTEL', ''), 'OF', ''), ' A ', ''), UPPER(regexp_replace(regexp_replace(regexp_replace(UPPER(split(COALESCE(h.city, 'null'),',')[0]),'\n',''),'\t',''),'"','')),''),' ',''),'"',''),"'",""),',',''),
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(split(COALESCE(h.city, 'null'),',')[0]),'\n',''),'\t',''),'"',''),' ',''),'-',''),'NULL','')) corrected_name,* from ${hiveDB}.dim_hotel h order by corrected_name;
truncate table ${hiveDB}.dim_hotel;

--===================================================Dim  supplier code
 Drop table if exists ${hiveDB}.tmp_dim_supplier;
 Create table ${hiveDB}.tmp_dim_supplier ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as select 
 dim_supplier_id ,                  
 -- supplier_currency,                 
 vendor_name,                       
 vendor_currency,                   
 vendor_id,                       
 "null" as clean_supplier_id
 from ${hiveDB}.dim_supplier_raw;
 Drop table if exists  ${hiveDB}.dim_supplier;
 Alter table ${hiveDB}.tmp_dim_supplier rename to ${hiveDB}.dim_supplier;

 --=Adding Almosafer suppliers
 Drop table if exists ${hiveDB}.tmp_dim_supplier;
 Create table ${hiveDB}.tmp_dim_supplier ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
 select 
 lower(dim_supplier_id) as dim_supplier_id ,                  
 -- supplier_currency,                 
 vendor_name,                       
 vendor_currency,                   
 vendor_id,                       
 "null" as clean_supplier_id
 from (
 select * from ${hiveDB}.dim_supplier 
 union 
 select 
 lower(dim_supplier_id) as dim_supplier_id ,                  
 -- dim_supplier_currency as supplier_currency,                 
 "-1" as vendor_name,                       
 "-1" as vendor_currency,                   
 dim_vendor_id as vendor_id,                       
 "null" as clean_supplier_id from ${hiveDB}.fact_hotel_almosafer_2016 group by dim_supplier_id,dim_vendor_id
 ) A 
 group by 
 dim_supplier_id,
 -- supplier_currency,
 vendor_name,vendor_currency,vendor_id;
 Drop table if exists  ${hiveDB}.dim_supplier;
 Alter table ${hiveDB}.tmp_dim_supplier rename to ${hiveDB}.dim_supplier;

--===For eagle suppliers data 
Drop table if exists ${hiveDB}.tmp_dim_supplier;
 Create table ${hiveDB}.tmp_dim_supplier ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
 select
lower(dim_supplier_id) as dim_supplier_id , 
 -- supplier_currency,                 
 vendor_name,                       
 vendor_currency,                   
 vendor_id,                       
 "null" as clean_supplier_id
  from (
 select * from ${hiveDB}.dim_supplier 
 union 
 select 
 -- lower(dim_supplier_id) as dim_supplier_id ,                  
 lower(supplier) as dim_supplier_id,
 -- dim_supplier_currency as supplier_currency,                 
 -- "-1" as vendor_name,                       
 agency as vendor_name,
 "-1" vendor_currency,                   
 "-1" as vendor_id,                       
 "null" as clean_supplier_id 
 -- from ${hiveDB}.fact_eagle_travels 
 from ${hiveDB}.eagle_travels 
  group by 
  lower(supplier), agency
  -- dim_supplier_id
  -- ,dim_supplier_currency
 ) A 
 group by
 dim_supplier_id,
 -- supplier_currency,
 vendor_name,vendor_currency,vendor_id;
 Drop table if exists  ${hiveDB}.dim_supplier;
 Alter table ${hiveDB}.tmp_dim_supplier rename to ${hiveDB}.dim_supplier;
 --===============For Altayyar Suppliers
Drop table if exists ${hiveDB}.tmp_dim_supplier;
 Create table ${hiveDB}.tmp_dim_supplier ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
 select
lower(dim_supplier_id) as dim_supplier_id , 
-- supplier_currency,
 vendor_name,
 vendor_currency,
 vendor_id,
 "null" as clean_supplier_id
  from (
 select * from ${hiveDB}.dim_supplier
 union
 select
 lower(supplier) as dim_supplier_id ,
 -- currency as supplier_currency,
 vendor_name,
 "null" vendor_currency,
  vendor_id,
 "null" as clean_supplier_id from ${hiveDB}.tours_hotel group by 
 supplier,
 -- currency,
 vendor_id,vendor_name
 ) A group by dim_supplier_id,
 -- supplier_currency,
 vendor_name,vendor_currency,vendor_id;
 Drop table if exists  ${hiveDB}.dim_supplier;
 Alter table ${hiveDB}.tmp_dim_supplier rename to ${hiveDB}.dim_supplier;

--==============Creating clean hotel table
 Drop table if exists ${hiveDB}.tmp_dim_supplier_clean;
 Create table ${hiveDB}.tmp_dim_supplier_clean ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
 select * from(
 select * from googlesheets.tajawal_hotel_suppliers_cleanup
 UNION
 select * from googlesheets.altayar_hotel_supplier_cleanup
 UNION
 select * from googlesheets.eagle_hotel_supplier_cleanup
 ) A;
 Drop table if exists ${hiveDB}.dim_supplier_clean ;
 Alter table ${hiveDB}.tmp_dim_supplier_clean rename to ${hiveDB}.dim_supplier_clean;
--=======================================================

Drop table if exists ${hiveDB}.tmp_dim_supplier;
 Create table ${hiveDB}.tmp_dim_supplier ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as
 select 
 ltrim(rtrim(lower(a.dim_supplier_id))) as dim_supplier_id,                  
--  a.supplier_currency,                 
-- a.vendor_name,                       
-- a.vendor_currency,                   
-- a.vendor_id,
-- case when cast(regexp_replace(COALESCE(a.dim_supplier_id,b.new_supplier_id),'\\(|\\)|\\"|\\t|\\n','') as double ) is null then regexp_replace(COALESCE(b.new_supplier_id,a.dim_supplier_id),'\\(|\\)|\\"|\\t|\\n','') else 'other' end as clean_supplier_id                       
  -- case when max(coalesce(b.new_supplier_id, '-1')) = '-1' then 'na' else max(coalesce(b.new_supplier_id, '-1')) end as clean_supplier_id
  lower(case when max(coalesce(b.new_supplier_id, '-1')) = '-1' then ltrim(rtrim(lower(a.dim_supplier_id))) else max(coalesce(b.new_supplier_id, '-1')) end) as clean_supplier_id
  from 
  (
   select dim_supplier_id, 
   max(case when vendor_name = 'null' then '-1' else vendor_name end) as vendor_name
   , max(case when vendor_currency = 'null' then '-1' else vendor_currency end) as vendor_currency
   , max(case when vendor_id = 'null' then '-1' else vendor_id end) as vendor_id
   from ${hiveDB}.dim_supplier
   group by dim_supplier_id
  )  a left outer join 
  (select max(new_supplier_id) as new_supplier_id,ltrim(rtrim(lower(old_supplier_id))) as old_supplier_id from ${hiveDB}.dim_supplier_clean group by ltrim(rtrim(lower(old_supplier_id)))) b 
  on ltrim(rtrim(lower(b.old_supplier_id)))=ltrim(rtrim(lower(regexp_replace(a.dim_supplier_id,'\\(|\\)|\\"|\\t|\\n',''))))
  group by ltrim(rtrim(lower(a.dim_supplier_id)));
Drop table if exists  ${hiveDB}.dim_supplier;
 Alter table ${hiveDB}.tmp_dim_supplier rename to ${hiveDB}.dim_supplier;

--===================================================

drop table if exists ${hiveDB}.tmp_all_discounts;
drop table if exists ${hiveDB}.tmp_discount_orders;

drop table if exists ${hiveDB}.dim_promo_grouping;
create table  ${hiveDB}.dim_promo_grouping like googlesheets.dim_promo_grouping;
insert into ${hiveDB}.dim_promo_grouping
select * from googlesheets.dim_promo_grouping;

-- DROP TABLE IF EXISTS ${hiveDB}.fact_status_history_intermediate;
-- CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_status_history_intermediate ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
-- AS
-- select 
-- order_no,dim_group_id,order_type,dim_bookingdate_id,dim_store_id,history_status_id,history_text,
-- history_type,history_created_date,history_created_timestamp  ,history_updated_date,
-- history_updated_timestamp,history_product_no,
-- --LEAD(history_timestamp,1,0) over (PARTITION by order_no order by history_timestamp) as end_time,
-- unix_timestamp(LEAD(history_timestamp,1,0) over (PARTITION by order_no order by history_timestamp), 'yyyyMMddHHmmss') - 
-- unix_timestamp(history_timestamp,'yyyyMMddHHmmss') as time_taken,
-- LEAD(history_status_id, 1,0) over (PARTITION by order_no order by history_timestamp) as next_status
-- from
-- (
--  select order_no,dim_group_id,order_type,dim_bookingdate_id,dim_store_id,history_status_id,
--  history_text,history_type,history_created_date,history_created_timestamp, 
--  history_updated_date,history_updated_timestamp,history_product_no,
--  CONCAT(history_created_date,from_unixtime(unix_timestamp(history_created_timestamp, 'hh:mm:ss.S'), 'HHmmss')) history_timestamp 
--  from ${hiveDB}.fact_status_history 
--  order by order_no, 
--  unix_timestamp(CONCAT(history_created_date,from_unixtime(unix_timestamp(
--   history_created_timestamp, 'hh:mm:ss.S'),'HHmmss')),'yyyyMMddHHmmss')
-- ) A;

DROP TABLE IF EXISTS ${hiveDB}.fact_status_history_intermediate;
CREATE TABLE IF NOT EXISTS ${hiveDB}.fact_status_history_intermediate ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
AS
select order_no,dim_group_id,order_type,dim_bookingdate_id,dim_store_id,
history_status_id,history_text,history_type,history_created_date,history_created_timestamp ,
history_updated_date,history_updated_timestamp,history_product_no,
CASE WHEN(unix_timestamp(LEAD(history_timestamp,1,0) over (PARTITION by order_no,history_type order by history_timestamp,history_status_id), 'yyyyMMddHHmmss') -
unix_timestamp(history_timestamp,'yyyyMMddHHmmss')) is NULL THEN 0 
ELSE (unix_timestamp(LEAD(history_timestamp,1,0) over (PARTITION by order_no,history_type order by history_timestamp,history_status_id), 'yyyyMMddHHmmss') -
unix_timestamp(history_timestamp,'yyyyMMddHHmmss')) END as time_taken,
LEAD(history_status_id, 1,0) over (PARTITION by order_no,history_type order by history_timestamp,history_status_id) as next_status
from
(
  select order_no,dim_group_id,order_type,dim_bookingdate_id,dim_store_id,
  history_status_id,history_text,history_type,history_created_date,history_created_timestamp,
  history_updated_date,history_updated_timestamp,history_product_no,
  CONCAT(history_created_date,from_unixtime(unix_timestamp(history_created_timestamp, 'HH:mm:ss.S'), 'HHmmss')) history_timestamp 
  from ${hiveDB}.fact_status_history 
  order by order_no,unix_timestamp(CONCAT(history_created_date,from_unixtime(unix_timestamp(history_created_timestamp, 'hh:mm:ss.S'), 'HHmmss')),'yyyyMMddHHmmss')
)A;

DROP TABLE IF EXISTS ${hiveDB}.fact_status_history;
ALTER TABLE ${hiveDB}.fact_status_history_intermediate RENAME TO ${hiveDB}.fact_status_history;



