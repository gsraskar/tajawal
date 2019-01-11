
Drop table if exists bi.dim_store;
create table if not exists bi.dim_store(store_id varchar(200),store_name varchar(200),group_id varchar(200),group_name varchar(200),currency varchar(200),site varchar(200),type varchar(200),amd_office varchar(200))
;
Drop table if exists bi.dim_payment_status;
create table if not exists bi.dim_payment_status(payment_status_id varchar(200),payment_status varchar(200))
;
Drop table if exists bi.dim_status;
create table if not exists bi.dim_status(status_id varchar(200),status varchar(200))
;

Drop table if exists bi.dim_groups;
create table if not exists bi.dim_groups(group_id varchar(200),group_name varchar(200))
;
