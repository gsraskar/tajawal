set hive.mv.files.thread=0;

-- ==== Merge into fact_hotel_orders
Drop table if exists tmp_fact_hotel_orders_incr;
-- Create intermediate table by taking only latest product per order from input data
create table tmp_fact_hotel_orders_incr as select A.* from fact_hotel_orders_incr A inner join (select id, max(dim_bookingupdated_id) as dim_bookingupdated_id from fact_hotel_orders_incr group by id) B on A.id = B.id and A.dim_bookingupdated_id = B.dim_bookingupdated_id;
-- Create final table by considering new data and old data.
drop table if exists tmp_fact_hotel_orders;
Create table tmp_fact_hotel_orders like fact_hotel_orders;
Insert into tmp_fact_hotel_orders 
select A.* from fact_hotel_orders A left outer join (select distinct(id) as id from tmp_fact_hotel_orders_incr) B on A.id = B.id where B.id is null
Union
Select * from tmp_fact_hotel_orders_incr;
Drop table fact_hotel_orders;
Alter table tmp_fact_hotel_orders rename to fact_hotel_orders;

-- ==== Merge into fact_flight_orders
Drop table if exists tmp_fact_flight_orders_incr;
Create table tmp_fact_flight_orders_incr as select A.* from fact_flight_orders_incr A inner join (select id, max(dim_bookingupdated_id) as dim_bookingupdated_id from fact_flight_orders_incr group by id) B on A.id = B.id and A.dim_bookingupdated_id = B.dim_bookingupdated_id;
drop table if exists tmp_fact_flight_orders;
Create table tmp_fact_flight_orders like fact_flight_orders;
Insert into tmp_fact_flight_orders 
select A.* from fact_flight_orders A left outer join (select distinct(id) as id from tmp_fact_flight_orders_incr) B on A.id = B.id where B.id is null
Union
Select * from tmp_fact_flight_orders_incr;
Drop table fact_flight_orders;
Alter table tmp_fact_flight_orders rename to fact_flight_orders;

-- ==== Merge into dim_hotel_raw
Insert into dim_hotel_raw select * from dim_hotel_raw_incremental;

-- ==== Merge into dim_customer_raw
Insert into dim_customer_raw select * from dim_customer_raw_incr;

-- ==== Merge into dim_room
Drop table if exists tmp_dim_room_incr;
Create table tmp_dim_room_incr as select room_id, max(name) as name, max(room_class) as room_class, max(room_type) as room_type, max(room_basis) as room_basis, id, track_id, order_no, max(order_type) as order_type from dim_room_incr group by room_id, id, track_id, order_no; 
drop table if exists tmp_dim_room;
Create table tmp_dim_room like dim_room;
Insert into tmp_dim_room 
select A.* from dim_room A left outer join (select distinct(id) as id from tmp_dim_room_incr) B on A.id = B.id where B.id is null
Union
Select * from tmp_dim_room_incr;
Drop table dim_room;
Alter table tmp_dim_room rename to dim_room;

-- ==== Merge into fact_status_history
Drop table if exists tmp_fact_status_history_incr;
Create table tmp_fact_status_history_incr as select order_no, dim_group_id, order_type, dim_bookingdate_id, dim_store_id, history_status_id, history_text, history_type, history_created_date, history_created_timestamp, history_updated_date, history_updated_timestamp, history_product_no, time_taken, next_status from (select order_no, dim_group_id, order_type, dim_bookingdate_id, dim_store_id, history_status_id, history_text, history_type, history_created_date, history_created_timestamp, history_updated_date, history_updated_timestamp, history_product_no, time_taken, next_status, count(*) as total from fact_status_history_incr group by order_no, dim_group_id, order_type, dim_bookingdate_id, dim_store_id, history_status_id, history_text, history_type, history_created_date, history_created_timestamp, history_updated_date, history_updated_timestamp, history_product_no, time_taken, next_status) A;
drop table if exists tmp_fact_status_history;
Create table tmp_fact_status_history like fact_status_history;
Insert into tmp_fact_status_history 
select A.* from fact_status_history A left outer join (select distinct(order_no) as order_no from tmp_fact_status_history_incr) B on A.order_no = B.order_no where B.order_no is null
Union
Select * from tmp_fact_status_history_incr;
Drop table fact_status_history;
Alter table tmp_fact_status_history rename to fact_status_history;

-- === Merge into dim_supplier
Insert into tajawal_uat_bi.dim_supplier_raw select * from tajawal_uat_bi.dim_supplier_raw_incr;

-- === Merge into fact_package_orders
Drop table if exists tajawal_uat_bi.tmp_fact_package_orders_incr;
Create table tajawal_uat_bi.tmp_fact_package_orders_incr as select A.* from tajawal_uat_bi.fact_package_orders_incr A inner join (select id, max(dim_bookingupdated_id) as dim_bookingupdated_id from tajawal_uat_bi.fact_package_orders_incr group by id) B on A.id = B.id and A.dim_bookingupdated_id = B.dim_bookingupdated_id;
Drop table if exists tajawal_uat_bi.tmp_fact_package_orders;
Create table tajawal_uat_bi.tmp_fact_package_orders like tajawal_uat_bi.fact_package_orders;
Insert into tajawal_uat_bi.tmp_fact_package_orders 
select A.* from tajawal_uat_bi.fact_package_orders A left outer join (select distinct(id) as id from tajawal_uat_bi.tmp_fact_package_orders_incr) B on A.id = B.id where B.id is null
Union
Select * from tajawal_uat_bi.tmp_fact_package_orders_incr;
Drop table tajawal_uat_bi.fact_package_orders;
Alter table tajawal_uat_bi.tmp_fact_package_orders rename to tajawal_uat_bi.fact_package_orders;

-- === Merge into fact_package_flights
Drop table if exists tajawal_uat_bi.tmp_fact_package_flight_incr;
Create table tajawal_uat_bi.tmp_fact_package_flight_incr as select A.* from tajawal_uat_bi.fact_package_flight_incr A inner join (select id, max(dim_bookingupdated_id) as dim_bookingupdated_id from tajawal_uat_bi.fact_package_flight_incr group by id) B on A.id = B.id and A.dim_bookingupdated_id = B.dim_bookingupdated_id;
Drop table if exists tajawal_uat_bi.tmp_fact_package_flight;
Create table tajawal_uat_bi.tmp_fact_package_flight like tajawal_uat_bi.fact_package_flight;
Insert into tajawal_uat_bi.tmp_fact_package_flight 
select A.* from tajawal_uat_bi.fact_package_flight A left outer join (select distinct(id) as id from tajawal_uat_bi.tmp_fact_package_flight_incr) B on A.id = B.id where B.id is null
Union
Select * from tajawal_uat_bi.tmp_fact_package_flight_incr;
Drop table tajawal_uat_bi.fact_package_flight;
Alter table tajawal_uat_bi.tmp_fact_package_flight rename to tajawal_uat_bi.fact_package_flight;

-- === Merge into fact_segment_details
Drop table if exists tajawal_uat_bi.tmp_fact_segment_details_incr;
Create table tajawal_uat_bi.tmp_fact_segment_details_incr as select id, order_no, order_type, segment_id, departure, arrival, duration, cabin_class, cabin_code, leg_id from (select id, order_no, order_type, segment_id, departure, arrival, duration, cabin_class, cabin_code, leg_id, count(*) from tajawal_uat_bi.fact_segment_details_incr group by id, order_no, order_type, segment_id, departure, arrival, duration, cabin_class, cabin_code, leg_id) A;
Drop table if exists tajawal_uat_bi.tmp_fact_segment_details;
Create table tajawal_uat_bi.tmp_fact_segment_details like tajawal_uat_bi.fact_segment_details;
Insert into tajawal_uat_bi.tmp_fact_segment_details 
select A.* from tajawal_uat_bi.fact_segment_details A left outer join (select distinct(id) as id from tajawal_uat_bi.tmp_fact_segment_details_incr) B on A.id = B.id where B.id is null
Union
Select * from tajawal_uat_bi.tmp_fact_segment_details_incr;
Drop table tajawal_uat_bi.fact_segment_details;
Alter table tajawal_uat_bi.tmp_fact_segment_details rename to tajawal_uat_bi.fact_segment_details;

-- == Merge into fact_vat_orders
Drop table if exists tajawal_uat_bi.tmp_fact_vat_orders_incr;
Create table tajawal_uat_bi.tmp_fact_vat_orders_incr as select id, track_id, order_no, order_type, total_vat from (select id, track_id, order_no, order_type, total_vat, count(*) from tajawal_uat_bi.fact_vat_orders_incr group by id, track_id, order_no, order_type, total_vat) A;
Drop table if exists tajawal_uat_bi.tmp_fact_vat_orders;
Create table tajawal_uat_bi.tmp_fact_vat_orders like tajawal_uat_bi.fact_vat_orders;
Insert into tajawal_uat_bi.tmp_fact_vat_orders 
select A.* from tajawal_uat_bi.fact_vat_orders A left outer join (select distinct(id) as id from tajawal_uat_bi.tmp_fact_vat_orders_incr) B on A.id = B.id where B.id is null
Union
Select * from tajawal_uat_bi.tmp_fact_vat_orders_incr;
Drop table tajawal_uat_bi.fact_vat_orders;
Alter table tajawal_uat_bi.tmp_fact_vat_orders rename to tajawal_uat_bi.fact_vat_orders;

-- == Merge into fact_insurance_orders
Drop table if exists tajawal_uat_bi.tmp_fact_insurance_orders_incr;
Create table tajawal_uat_bi.tmp_fact_insurance_orders_incr as select A.* from tajawal_uat_bi.fact_insurance_orders_incr A inner join (select id, max(dim_bookingupdated_id) as dim_bookingupdated_id from tajawal_uat_bi.fact_insurance_orders_incr group by id) B on A.id = B.id and A.dim_bookingupdated_id = B.dim_bookingupdated_id;
Drop table if exists tajawal_uat_bi.tmp_fact_insurance_orders;
Create table tajawal_uat_bi.tmp_fact_insurance_orders like tajawal_uat_bi.fact_vat_orders;
Insert into tmp_fact_insurance_orders 
select A.* from tajawal_uat_bi.fact_insurance_orders A left outer join (select distinct(id) as id from tajawal_uat_bi.tmp_fact_insurance_orders_incr) B on A.id = B.id where B.id is null
Union
Select * from tajawal_uat_bi.tmp_fact_insurance_orders_incr;
Drop table tajawal_uat_bi.fact_insurance_orders;
Alter table tajawal_uat_bi.tmp_fact_insurance_orders rename to tajawal_uat_bi.fact_insurance_orders;

-- === Merge into fact_crm
Insert into tajawal_uat_bi.fact_crm select * from tajawal_uat_bi.fact_crm_incr;
