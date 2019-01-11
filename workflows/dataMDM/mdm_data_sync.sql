--Sync dim_mobile_adnetwork_cost_metric to mobile_adnetwork_final
--drop table if exists googlesheets.mobile_adnetwork_final_intermediate;
--create table googlesheets.mobile_adnetwork_final_intermediate like  googlesheets.mobile_adnetwork_final_test;
--insert into googlesheets.mobile_adnetwork_final_intermediate select * from ${hiveDB}.dim_mobile_adnetwork_cost_metric_mdm;
--Drop table if exists googlesheets.mobile_adnetwork_final_test;
--Alter table googlesheets.mobile_adnetwork_final_intermediate RENAME TO googlesheets.mobile_adnetwork_final_test;

--Sysc dim_spend_adjust_network_channels_mdm to dim_spend_adjust_network_channels 
--drop table if exists ${hiveDB}.dim_spend_adjust_network_channels_intermediate;
--create table ${hiveDB}.dim_spend_adjust_network_channels_intermediate like ${hiveDB}.dim_spend_adjust_network_channels;
--insert into ${hiveDB}.dim_spend_adjust_network_channels_intermediate select * from ${hiveDB}.dim_spend_adjust_network_channels_mdm;
--drop table if exists ${hiveDB}.dim_spend_adjust_network_channels;
--alter table ${hiveDB}.dim_spend_adjust_network_channels_intermediate rename to ${hiveDB}.dim_spend_adjust_network_channels;

--Sysc dim_spend_channels_mdm to dim_spend_channels 
--drop table if exists ${hiveDB}.dim_spend_channels_intermediate;
--create table ${hiveDB}.dim_spend_channels_intermediate like ${hiveDB}.dim_spend_channels;
--insert into ${hiveDB}.dim_spend_channels_intermediate select * from ${hiveDB}.dim_spend_channels_mdm;
--drop table if exists ${hiveDB}.dim_spend_channels;
--alter table ${hiveDB}.dim_spend_channels_intermediate rename to ${hiveDB}.dim_spend_channels;
