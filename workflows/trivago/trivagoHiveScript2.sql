set hive.mv.files.thread=0;
insert into ${hiveDB}.fact_trivago_hotel_data select * from ${hiveDB}.fact_trivago_hotel_data_intermediate;
insert into ${hiveDB}.fact_trivago_pos_data select * from ${hiveDB}.fact_trivago_pos_data_intermediate;
