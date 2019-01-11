insert into ${hiveDB}.fact_bing_data select * from ${hiveDB}.fact_bing_data_intermediate;
