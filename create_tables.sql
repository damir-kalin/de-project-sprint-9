create schema if not exists cdm;

drop table if exists cdm.user_product_counters cascade;
create table if not exists cdm.user_product_counters (
	id serial not null primary key,
	user_id uuid not null,
	product_id uuid not null,
	product_name varchar(150) not null,
	order_cnt int not null check (order_cnt >= 0),
	constraint uq_user_product_counters_user_id_product_id unique(user_id, product_id)
);

drop table if exists cdm.user_category_counters;
create table if not exists cdm.user_category_counters (
	id serial not null primary key,
	user_id uuid not null,
	category_id uuid not null,
	category_name varchar(150) not null,
	order_cnt int not null check (order_cnt >= 0),
	constraint uq_user_category_counters_user_id_product_id unique(user_id, category_id)
);

drop table if exists stg.order_events;
create table if not exists stg.order_events(
	id serial not null primary key,
	object_id int not null,
	object_type varchar(50) not null,
	sent_dttm timestamp not null,
	payload json not null,
	constraint uq_order_events_object_id unique(object_id)
);


drop table if exists dds.h_user cascade;
create table if not exists dds.h_user(
	h_user_pk uuid not null primary key,
	user_id varchar not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint uq_h_user_user_id unique(user_id)
);

drop table if exists dds.h_product cascade;
create table if not exists dds.h_product(
	h_product_pk uuid not null primary key,
	product_id varchar not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint uq_h_product_product_id unique(product_id)
);



drop table if exists dds.h_category cascade;
create table if not exists dds.h_category(
	h_category_pk uuid not null primary key,
	category_name varchar not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint uq_h_category_category_name unique(category_name)
);

drop table if exists dds.h_restaurant cascade;
create table if not exists dds.h_restaurant(
	h_restaurant_pk uuid not null primary key,
	restaurant_id varchar not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint uq_h_restaurant_restaurant_id unique(restaurant_id)
);


drop table if exists dds.h_order cascade;
create table if not exists dds.h_order(
	h_order_pk uuid not null primary key,
	order_id integer not null,
	order_dt timestamp not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint uq_h_order_order_id_order_dt unique(order_id)
);

drop table if exists dds.l_order_product cascade;
create table if not exists dds.l_order_product(
	hk_order_product_pk uuid not null primary key,
	h_order_pk uuid not null,
	h_product_pk uuid not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint fk_l_order_product_h_order_pk foreign key (h_order_pk) references dds.h_order(h_order_pk),
	constraint fk_l_order_product_h_product_pk foreign key (h_product_pk) references dds.h_product(h_product_pk),
	constraint uq_l_order_product_h_order_pk_h_product_pk unique(h_order_pk, h_product_pk) 
);

drop table if exists dds.l_product_restaurant cascade;
create table if not exists dds.l_product_restaurant(
	hk_product_restaurant_pk uuid not null primary key,
	h_product_pk uuid not null,
	h_restaurant_pk uuid not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint fk_l_product_restaurant_h_product_pk foreign key (h_product_pk) references dds.h_product(h_product_pk),
	constraint fk_l_product_restaurant_h_restaurant_pk foreign key (h_restaurant_pk) references dds.h_restaurant(h_restaurant_pk),
	constraint uq_l_product_restaurant_h_product_pk_h_restaurant_pk unique(h_product_pk, h_restaurant_pk)
);


drop table if exists dds.l_product_category cascade;
create table if not exists dds.l_product_category(
	hk_product_category_pk uuid not null primary key,
	h_product_pk uuid not null,
	h_category_pk uuid not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint fk_l_product_category_h_product_pk foreign key (h_product_pk) references dds.h_product(h_product_pk),
	constraint fk_l_product_category_h_category_pk foreign key (h_category_pk) references dds.h_category(h_category_pk),
	constraint uq_l_product_category_h_product_pk_h_category_pk unique(h_product_pk, h_category_pk)
);


drop table if exists dds.l_order_user cascade;
create table if not exists dds.l_order_user(
	hk_order_user_pk uuid not null primary key,
	h_order_pk uuid not null,
	h_user_pk uuid not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	constraint fk_l_order_user_h_user_pk foreign key (h_user_pk) references dds.h_user(h_user_pk),
	constraint fk_l_order_user_h_order_pk foreign key (h_order_pk) references dds.h_order(h_order_pk),
	constraint uq_l_order_user_h_order_pk_h_user_pk unique(h_order_pk, h_user_pk)
);


drop table if exists dds.s_user_names cascade;
create table if not exists dds.s_user_names(
	h_user_pk uuid not null,
	username varchar(150) not null,
	userlogin varchar(150) not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'users-system-redis',
	hk_user_names_hashdiff uuid not null primary key,
	constraint fk_s_user_names_h_user_pk foreign key (h_user_pk) references dds.h_user(h_user_pk),
	constraint uq_s_user_names_h_user_pk unique(h_user_pk)
);

drop table if exists dds.s_product_names cascade;
create table if not exists dds.s_product_names(
	h_product_pk uuid not null,
	name varchar(150) not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'restaraunts-system-redis',
	hk_product_names_hashdiff uuid not null primary key,
	constraint fk_s_product_names_h_product_pk foreign key (h_product_pk) references dds.h_product(h_product_pk),
	constraint uq_s_product_names_h_product_pk unique(h_product_pk)
);

drop table if exists dds.s_restaurant_names cascade;
create table if not exists dds.s_restaurant_names(
	h_restaurant_pk uuid not null,
	name varchar(150) not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'restaraunts-system-redis',
	hk_restaurant_names_hashdiff uuid not null primary key,
	constraint fk_s_restaurant_names_h_restaurant_pk foreign key (h_restaurant_pk) references dds.h_restaurant(h_restaurant_pk),
	constraint uq_s_restaurant_names_h_restaurant_pk unique(h_restaurant_pk)
);

drop table if exists dds.s_order_cost cascade;
create table if not exists dds.s_order_cost(
	h_order_pk uuid not null,
	cost decimal(19, 5) not null,
	payment decimal(19, 5) not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'restaraunts-system-redis',
	hk_order_cost_hashdiff uuid not null primary key,
	constraint fk_s_rorder_cost_h_order_pk foreign key (h_order_pk) references dds.h_order(h_order_pk),
	constraint uq_s_order_cost_h_order_pk unique(h_order_pk)
);

drop table if exists dds.s_order_status cascade;
create table if not exists dds.s_order_status(
	h_order_pk uuid not null,
	status varchar(100) not null,
	load_dt timestamp not null default now(),
	load_src varchar(50) not null default 'orders-system-kafka',
	hk_order_status_hashdiff uuid not null primary key,
	constraint fk_s_order_status_h_order_pk foreign key (h_order_pk) references dds.h_order(h_order_pk),
	constraint uq_s_order_status_h_order_pk unique(h_order_pk)
);


delete from stg.order_events cascade;

delete from dds.s_order_cost cascade;
delete from dds.s_order_status cascade;
delete from dds.s_product_names cascade;
delete from dds.s_restaurant_names cascade;
delete from dds.s_user_names cascade;
delete from dds.l_order_product cascade;
delete from dds.l_order_user cascade;
delete from dds.l_product_category cascade;
delete from dds.l_product_restaurant cascade;
delete from dds.h_category cascade;
delete from dds.h_order cascade;
delete from dds.h_product cascade;
delete from dds.h_restaurant cascade;
delete from dds.h_user cascade;

delete from cdm.user_category_counters cascade;
delete from cdm.user_product_counters cascade;


select * from stg.order_events oe;

select * from dds.h_category;
select * from dds.h_order;
select * from dds.h_product;
select * from dds.h_restaurant;
select * from dds.h_user;

select * from dds.l_order_product;
select * from dds.l_order_user;
select * from dds.l_product_category;
select * from dds.l_product_restaurant;

select * from dds.s_order_cost;
select * from dds.s_order_status;
select * from dds.s_product_names;
select * from dds.s_restaurant_names;
select * from dds.s_user_names;

select * from cdm.user_category_counters ucc;
select * from cdm.user_product_counters upc;




