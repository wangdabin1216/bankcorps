drop table if exists etl_meta_tables;
create table etl_meta_tables( 
 tab_name varchar(128),col_name varchar(128),col_type varchar(128),col_seq int,default_val varchar(32),tab_cn_name varchar(2048),col_cn_name varchar(2048),is_pk int,key_name varchar(256),remark varchar(128) 
 ); 

drop table if exists etl_meta_args;
create table etl_meta_args( 
 parameter varchar(256),para_type varchar(128),para_val varchar(512),remark varchar(512) 
 ); 

drop table if exists etl_meta_type_convert;
create table etl_meta_type_convert( 
 db_type varchar(128),col_type varchar(128),tar_db_type varchar(128),tar_col_type varchar(128),length int,default_val varchar(1024),format varchar(1024),trans_mode int 
 ); 

drop table if exists etl_load_system;
create table etl_load_system( 
 sys varchar(128),sys_name varchar(128),db_type varchar(128),db_version varchar(128),flag int,mode varchar(128),db_name varchar(128),schema_name varchar(128),charset varchar(1024),ip_addr varchar(512),port varchar(128),user varchar(128),password varchar(128),remark varchar(2048),is_compress varchar(128),is_fix_length varchar(128),is_exist_check_file varchar(128),is_exist_ready_file varchar(128) 
 ); 

drop table if exists etl_load_table;
create table etl_load_table( 
 sys varchar(128),tab_name varchar(128),tab_cn_name varchar(128),cond varchar(512),if_flag varchar(128),tab_type varchar(128),chain_type varchar(128),collect_mode varchar(128),file_name varchar(128),check_file varchar(128),file_format varchar(128),encode_format varchar(128),col_separator varchar(128),record_separator varchar(128),trans_path varchar(1024),remark varchar(1024),is_col_separator int,partition_set varchar(128),is_valid int,is_create int 
 ); 

drop table if exists etl_load_column;
create table etl_load_column( 
 sys varchar(128),tab_name varchar(128),col_seq int,col_name varchar(128),col_type varchar(128),col_cn_name varchar(256),is_pk int,is_null int,default_val varchar(128),trans_statement varchar(1024),is_dk int,mapping_col_type varchar(256),is_sk int 
 ); 

drop table if exists etl_trans_desc;
create table etl_trans_desc( 
 sys varchar(128),tab_name varchar(128),tab_cn_name varchar(128),func_desc varchar(128),comp_pers varchar(128),comp_date varchar(128),modif_record varchar(3000) 
 ); 

drop table if exists etl_trans_table;
create table etl_trans_table( 
 sys varchar(128),tab_name varchar(256),tab_cn_name varchar(128),chain_type varchar(128),if_mark varchar(128),tab_type varchar(128),format_type varchar(128),partition_set varchar(128),data_size varchar(128),col_num int,is_valid int,is_create int 
 ); 

drop table if exists etl_trans_column;
create table etl_trans_column( 
 sys varchar(128),tab_name varchar(128),col_seq varchar(128),col_name varchar(128),mapping_col_type varchar(128),col_cn_name varchar(256),is_pk int,is_not_null int,is_dk int,is_sk int,default_val varchar(128),data_dict varchar(129) 
 ); 

drop table if exists etl_trans_mapping;
create table etl_trans_mapping( 
 tab_name varchar(128),group_id int,group_desc varchar(128),part_id int,part_desc varchar(128),aim_db_name varchar(128),aim_tab_name varchar(128),aim_tab_cn_name varchar(256),part_col_id int,aim_col_name varchar(128),aim_col_cn_name varchar(256),aim_col_type varchar(128),sour_sys varchar(128),sour_db_name varchar(128),sour_tab_name varchar(128),sour_tab_cn_name varchar(256),sour_col_name varchar(128),sour_col_cn_name varchar(256),sour_col_type varchar(128),mapping varchar(128),main_tab varchar(128),main_tab_oth varchar(128),minor_tab varchar(128),minor_tab_oth varchar(128),part_type varchar(128),join_type varchar(128),on_term varchar(128),on_term_desc varchar(256),where_term varchar(128),where_term_desc varchar(256),var_name varchar(128),var_value varchar(128),var_desc varchar(256) 
 ); 

drop table if exists etl_serve_view;
create table etl_serve_view( 
 view_name varchar(256),view_schema varchar(256),view_chn varchar(256),tab_name varchar(256),constriant varchar(2048),mapping_mode varchar(256),status varchar(128),is_create varchar(128) 
 ); 

drop table if exists etl_serve_view_mapping;
create table etl_serve_view_mapping( 
 view_name varchar(128),col_no varchar(128),col_name varchar(128),col_chn varchar(128),map_col varchar(128),fun_meth varchar(128),des_type varchar(128) 
 ); 

drop table if exists etl_serve_unload;
create table etl_serve_unload( 
 table_name varchar(128),file_seq varchar(128),sep varchar(128),fields varchar(2048),cond varchar(128),status varchar(128),is_create varchar(128) 
 ); 

drop table if exists etl_serve_distribute;
create table etl_serve_distribute( 
 system_flag varchar(128),tar_file_seq varchar(128),tar_file_name varchar(128),tar_file_check_name varchar(128),tar_feed_ways varchar(128),tar_file_code varchar(128),tar_file_source_type varchar(128),source_name varchar(128),source_file_name varchar(128),source_file_path varchar(512),source_file_code varchar(128),status varchar(128),is_create varchar(128) 
 ); 

drop table if exists etl_check_quality_rules;
create table etl_check_quality_rules( 
 rule_no varchar(128),rule_expr varchar(512),rule_desc varchar(512) 
 ); 

drop table if exists etl_check_cols_rule;
create table etl_check_cols_rule( 
 table_name varchar(128),col_name varchar(128),rule_no varchar(128),std_result varchar(512) 
 ); 

drop table if exists etl_trans_std_dict;
create table etl_trans_std_dict( 
 lp_code varchar(128),data_std_code varchar(128),std_dict_type varchar(128),std_dict_type_name varchar(1024),std_dict_value varchar(128),std_dict_value_name varchar(1024),rem varchar(1024),start_date DATE,due_date DATE 
 ); 

drop table if exists etl_trans_colm_dict;
create table etl_trans_colm_dict( 
 lp_code varchar(128),table_name varchar(128),column_name varchar(128),std_dict_type varchar(128),src_dict_type varchar(128),src_system_no varchar(128),one_to_one_sign varchar(128),start_date DATE,due_date DATE 
 ); 

drop table if exists etl_trans_dict_mapp;
create table etl_trans_dict_mapp( 
 lp_code varchar(128),std_dict_type varchar(128),std_dict_value varchar(128),src_dict_type varchar(128),src_dict_type_name varchar(128),src_dict_value varchar(128),src_dict_value_name varchar(128),src_system_no varchar(128),mappg_type varchar(128),start_date DATE,due_date DATE 
 ); 

