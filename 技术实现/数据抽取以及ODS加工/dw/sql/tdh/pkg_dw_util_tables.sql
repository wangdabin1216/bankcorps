--CC通用库系统表初始化程序--
--哑元表 dual
DROP TABLE IF EXISTS cc.dual;
create table cc.dual(
 a string
)TBLPROPERTIES ("cache" = "SSD");
--插入初始化数据
insert into cc.dual select  'A' from system.dual;


--存储过程日志表 dw_sm_trlg
DROP TABLE IF EXISTS cc.dw_sm_trlg;
create table cc.dw_sm_trlg(
  key struct<unix_time:int,log_object:string,log_seq:int> comment '主键:unix_time+对象+序号',
  system_flag string  comment '系统标识',
  begin_time timestamp comment '操作的开始时间',
  end_time timestamp comment '操作的结束时间',
  time_cost  int comment '操作耗时',
  pro_name string comment '存储过程的名字',
  log_object string comment '操作对象的名字',
  log_action string comment '操作的动作',
  row_count int comment '处理的行数',
  log_code string comment '错误的代码',
  log_desc string comment '错误的描述',
  etl_date date comment '处理日期',
  status string comment '表处理的状态'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'='|', 
  'serialization.format'='1',    
  'hbase.columns.mapping'=':key,f0:system_flag,f0:begin_time,f0:end_time,f0:time_cost,f0:pro_name,f0:log_object,f0:log_action,f0:row_count,f0:log_code,f0:log_desc,f0:etl_date,f0:status')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_trlg'); 

--历史处理配置表 dw_sm_hist
DROP TABLE IF EXISTS cc.dw_sm_hist;
CREATE  TABLE cc.dw_sm_hist(                                                                                                         
   table_name string COMMENT '表的名字',
   system_flag string COMMENT '表的系统标识',
   table_hs_name string COMMENT '表的历史表名',
   fields string COMMENT '表的字段序列',
   keys string COMMENT '表的主键',
   region_type string COMMENT '分区类型',
   trans_type  string COMMENT '表的处理方式:chain or add',                                                                                                      
   hist_field   string COMMENT 'add插入过滤条件字段',
   sync_type string COMMENT '同步级别') 
 ROW FORMAT SERDE 
   'org.apache.hadoop.hive.hbase.HBaseSerDe'                                                                                             
 STORED BY 
   'org.apache.hadoop.hive.hbase.HBaseStorageHandler'                                                                                    
 WITH SERDEPROPERTIES ( 'hbase.columns.mapping'=':key,f0:system_flag,f0:table_hs_name,f0:fields,f0:keys,f0:region_type,f0:trans_type,f0:hist_field,f0:sync_type')
 TBLPROPERTIES ('hbase.table.name'='dw_sm_hist');

 DROP TABLE IF EXISTS cc.dw_sm_hist_ext;
 CREATE  TABLE cc.dw_sm_hist_ext(                                                                                                         
   table_name string COMMENT '表的名字',
   system_flag string COMMENT '表的系统标识',
   table_hs_name string COMMENT '表的历史表名',
   fields string COMMENT '表的字段序列',
   keys string COMMENT '表的主键',
   region_type string COMMENT '分区类型',
   trans_type  string COMMENT '表的处理方式:chain or add',                                                                                                      
   hist_field   string COMMENT 'add插入过滤条件字段',
   sync_type string COMMENT '同步级别'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/dw/cc/dw_sm_hist_ext';

DROP TABLE IF EXISTS cc.dw_sm_history;
CREATE  TABLE cc.dw_sm_history(
  table_name string DEFAULT NULL COMMENT '表的名字', 
  system_flag string DEFAULT NULL COMMENT '表的系统标识', 
  table_hs_name string DEFAULT NULL COMMENT '表的历史表名', 
  fields string DEFAULT NULL COMMENT '表的字段序列', 
  keys string DEFAULT NULL COMMENT '表的主键', 
  straty string DEFAULT NULL COMMENT '表的处理方式', 
  chain_field string DEFAULT NULL COMMENT 'all类型表的处理日期', 
  field_num int DEFAULT NULL COMMENT '表的字段个数', 
  is_sync string DEFAULT NULL COMMENT '是否进行同步'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f0:system_flag,f0:table_hs_name,f0:fields,f0:keys,f0:straty,f0:chain_field,f0:field_num,f0:is_sync', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_history');

DROP TABLE IF EXISTS cc.dw_sm_sync;
CREATE  TABLE cc.dw_sm_sync(
  key struct<table_name:string,etl_date:date,data_source_system:string> DEFAULT NULL COMMENT 'key', 
  table_name string DEFAULT NULL COMMENT '表名', 
  etl_date date DEFAULT NULL COMMENT '数据日期', 
  etl_time timestamp DEFAULT NULL COMMENT '操作时间', 
  sync_prior string DEFAULT NULL COMMENT '同步级别', 
  status int DEFAULT NULL COMMENT '状态'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'='|', 
  'hbase.columns.mapping'=':key,f0:table_name,f0:etl_date,f0:etl_time,f0:sync_prior,f0:status', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_sync');

DROP TABLE IF EXISTS cc.dw_sm_unload_ext;
CREATE  TABLE cc.dw_sm_unload_ext(
  table_name string  COMMENT '供数的表名',
  file_seq string  COMMENT '文件序号',
  separator string  COMMENT '字段分隔符', 
  fields string  COMMENT '供数的字段', 
  condition string  COMMENT '约束条件',    
  status string  COMMENT '0不供数1供数2已废弃',
  is_create string  COMMENT '0不部署1部署'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/dw/cc/dw_sm_unload_ext';

DROP TABLE IF EXISTS cc.dw_sm_unload;
CREATE  TABLE cc.dw_sm_unload(
  key struct<table_name:string,file_seq:string>  COMMENT '主键',
  table_name string  COMMENT '供数的表名',
  file_seq string  COMMENT '文件序号',
  separator string  COMMENT '字段分隔符', 
  fields string  COMMENT '供数的字段', 
  condition string  COMMENT '约束条件',    
  status string  COMMENT '0不供数1供数2已废弃',
  is_create string  COMMENT '0不部署1部署'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'='|', 
   'hbase.columns.mapping'=':key,f0:table_name,f0:file_seq,f0:separator,f0:fie lds,f0:condition,f0:status,f0:is_create', 
  'serialization.format'='1')
TBLPROPERTIES ('hbase.table.name'='dw_sm_unload');

--数据检核配置表
DROP TABLE IF EXISTS cc.dw_sm_check_quality_rules_ext;
CREATE  TABLE cc.dw_sm_check_quality_rules_ext(
  rule_no string DEFAULT NULL COMMENT '校验规则编号', 
  rule_expr string DEFAULT NULL COMMENT '校验规则表达式', 
  rule_desc string DEFAULT NULL COMMENT '校验规则描述'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ('field.delim'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION  '/dw/cc/dw_sm_check_quality_rules_ext';

DROP TABLE IF EXISTS cc.dw_sm_check_quality_rules;
CREATE  TABLE cc.dw_sm_check_quality_rules(
  rule_no string DEFAULT NULL COMMENT '校验规则编号', 
  rule_expr string DEFAULT NULL COMMENT '校验规则表达式', 
  rule_desc string DEFAULT NULL COMMENT '校验规则描述'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'='|', 
  'hbase.columns.mapping'=':key,f0:rule_expr,f0:rule_desc', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_check_quality_rules');

--数据检核字段规则配置表
DROP TABLE IF EXISTS cc.dw_sm_check_cols_rule_ext;
CREATE  TABLE cc.dw_sm_check_cols_rule_ext(
  table_name string DEFAULT NULL COMMENT '表名', 
  col_name string DEFAULT NULL COMMENT '字段名', 
  rule_no string DEFAULT NULL COMMENT '校验规则编号', 
  std_result string DEFAULT NULL COMMENT '阈值'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/dw/cc/dw_sm_check_cols_rule_ext';

DROP TABLE IF EXISTS cc.dw_sm_check_cols_rule;
CREATE  TABLE cc.dw_sm_check_cols_rule(
  key struct<table_name:string,col_name:string,rule_no:string> DEFAULT NULL, 
  table_name string DEFAULT NULL COMMENT '表名', 
  col_name string DEFAULT NULL COMMENT '字段名', 
  rule_no string DEFAULT NULL COMMENT '校验规则编号', 
  std_result string DEFAULT NULL COMMENT '阈值'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'='|', 
  'hbase.columns.mapping'=':key,f0:table_name,f0:col_name,f0:rule_no,f0:std_result', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_check_cols_rule');

--数据检核结果表
DROP TABLE IF EXISTS cc.dw_sm_check_result;
CREATE  TABLE cc.dw_sm_check_result(
  key struct<check_date:date,table_name:string,col_name:string,rule_no:string> DEFAULT NULL, 
  check_date date DEFAULT NULL COMMENT '检查日期', 
  table_name string DEFAULT NULL COMMENT '表名', 
  col_name string DEFAULT NULL COMMENT '字段名', 
  rule_no string DEFAULT NULL COMMENT '规则编号', 
  rule_desc string DEFAULT NULL COMMENT '规则描述', 
  check_result decimal(18,2) DEFAULT NULL COMMENT '校验结果,校验结果=不匹配记录数/模型总记录数', 
  std_result decimal(18,2) DEFAULT NULL COMMENT '标准结果', 
  error_level string DEFAULT NULL COMMENT '错误级别', 
  check_diff decimal(18,2) DEFAULT NULL COMMENT '校验差值,校验差值=(校验结果 - 标准结果(阈值))*100', 
  table_row_count int DEFAULT NULL COMMENT '表记录数', 
  check_incf_row_count int DEFAULT NULL COMMENT '校验不匹配记录数'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'='|', 
  'hbase.columns.mapping'=':key,f0:check_date,f0:table_name,f0:col_name,f0:rule_no,f0:rule_desc,f0:check_result,f0:std_result,f0:error_level,f0:check_diff,f0:table_row_count,f0:check_incf_row_count', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_check_result');

--生成视图映射表
DROP TABLE IF EXISTS cc.dw_sm_serve_view_ext;
CREATE  TABLE cc.dw_sm_serve_view_ext(
  view_name string DEFAULT NULL COMMENT '视图名',
  view_schema string DEFAULT NULL COMMENT '库名', 
  view_chn string DEFAULT NULL COMMENT '视图中文名', 
  table_name string DEFAULT NULL COMMENT '表名', 
  condition string DEFAULT NULL COMMENT '约束条件', 
  map_type string DEFAULT NULL COMMENT '映射类型',
  valid_state string DEFAULT NULL COMMENT '有效状态',
  deploy_sign string DEFAULT NULL COMMENT '部署标识'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/dw/cc/dw_sm_serve_view_ext';

DROP TABLE IF EXISTS cc.dw_sm_serve_view;
CREATE  TABLE cc.dw_sm_serve_view(
  view_name string DEFAULT NULL COMMENT '视图名',
  view_schema string DEFAULT NULL COMMENT '库名', 
  view_chn string DEFAULT NULL COMMENT '视图中文名', 
  table_name string DEFAULT NULL COMMENT '表名', 
  condition string DEFAULT NULL COMMENT '约束条件', 
  map_type string DEFAULT NULL COMMENT '映射类型',
  valid_state string DEFAULT NULL COMMENT '有效状态',
  deploy_sign string DEFAULT NULL COMMENT '部署标识'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f0:view_schema,f0:view_chn,f0:table_name,f0:condition,f0:map_type,f0:valid_state,f0:deploy_sign', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_serve_view');

--生成视图字段映射规则表
DROP TABLE IF EXISTS cc.dw_sm_serve_view_mapping_ext;
CREATE  TABLE cc.dw_sm_serve_view_mapping_ext(
  view_name string DEFAULT NULL COMMENT '视图名', 
  col_no string DEFAULT NULL COMMENT '字段序号', 
  col_name string DEFAULT NULL COMMENT '字段名称', 
  col_chn string DEFAULT NULL COMMENT '字段中文名', 
  map_col string DEFAULT NULL COMMENT '映射字段', 
  fun_meth string DEFAULT NULL COMMENT '函数/方法', 
  des_type string DEFAULT NULL COMMENT '脱敏规则'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/dw/cc/dw_sm_serve_view_mapping_ext';

DROP TABLE IF EXISTS cc.dw_sm_serve_view_mapping;
CREATE  TABLE cc.dw_sm_serve_view_mapping(
  key struct<view_name:string,col_name:string> DEFAULT NULL COMMENT '主键:视图名+字段名称', 
  view_name string DEFAULT NULL COMMENT '视图名', 
  col_no string DEFAULT NULL COMMENT '字段序号', 
  col_name string DEFAULT NULL COMMENT '字段名称', 
  col_chn string DEFAULT NULL COMMENT '字段中文名', 
  map_col string DEFAULT NULL COMMENT '映射字段', 
  fun_meth string DEFAULT NULL COMMENT '函数/方法', 
  des_type string DEFAULT NULL COMMENT '脱敏规则'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f0:view_name,f0:col_no,f0:col_name,f0:col_chn,f0:map_col,f0:fun_meth,f0:des_type', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_serve_view_mapping');

--字典表
DROP TABLE IF EXISTS cc.f_cm_df_column_dict_ext;
CREATE  TABLE cc.f_cm_df_column_dict_ext(
  lp_code varchar(128) DEFAULT NULL, 
  table_name varchar(128) DEFAULT NULL, 
  column_name varchar(128) DEFAULT NULL, 
  std_dict_type varchar(128) DEFAULT NULL, 
  src_dict_type varchar(128) DEFAULT NULL, 
  one_to_one_ide_code varchar(128) DEFAULT NULL, 
  start_date varchar(30) DEFAULT NULL, 
  due_date varchar(30) DEFAULT NULL
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/dw/cc/f_cm_df_column_dict_ext';

DROP TABLE IF EXISTS cc.f_cm_df_dict_mapp_ext;
CREATE  TABLE cc.f_cm_df_dict_mapp_ext(
  lp_code varchar(128) DEFAULT NULL, 
  std_dict_type varchar(128) DEFAULT NULL, 
  std_dict_value varchar(128) DEFAULT NULL, 
  src_dict_type varchar(128) DEFAULT NULL, 
  src_dict_type_name varchar(128) DEFAULT NULL, 
  src_dict_value varchar(128) DEFAULT NULL, 
  src_dict_value_name varchar(128) DEFAULT NULL, 
  src_system_no varchar(128) DEFAULT NULL, 
  mappg_type varchar(128) DEFAULT NULL, 
  start_date varchar(30) DEFAULT NULL, 
  due_date varchar(30) DEFAULT NULL
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/dw/cc/f_cm_df_dict_mapp_ext';

DROP TABLE IF EXISTS cc.f_cm_df_std_dict_ext;
CREATE  TABLE cc.f_cm_df_std_dict_ext(
  lp_code varchar(128) DEFAULT NULL, 
  data_std_code varchar(128) DEFAULT NULL, 
  std_dict_type varchar(128) DEFAULT NULL, 
  std_dict_type_name varchar(1024) DEFAULT NULL, 
  std_dict_value varchar(128) DEFAULT NULL, 
  std_dict_value_name varchar(1024) DEFAULT NULL, 
  rem varchar(1024) DEFAULT NULL, 
  start_date varchar(30) DEFAULT NULL, 
  due_date varchar(30) DEFAULT NULL
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION  '/dw/cc/f_cm_df_std_dict_ext';

DROP TABLE IF EXISTS fmi.f_cm_df_std_dict;
CREATE  TABLE fmi.f_cm_df_std_dict(
LP_CODE STRING DEFAULT NULL COMMENT '法人代码',
DATA_STD_CODE STRING DEFAULT NULL COMMENT '数标编码',
STD_DICT_TYPE STRING DEFAULT NULL COMMENT '标准字典类别',
STD_DICT_TYPE_NAME STRING DEFAULT NULL COMMENT '标准字典类别名称',
STD_DICT_VALUE STRING DEFAULT NULL COMMENT '标准字典值',
STD_DICT_VALUE_NAME STRING DEFAULT NULL COMMENT '标准字典值名称',
REM STRING DEFAULT NULL COMMENT '备注',
START_DATE DATE DEFAULT NULL COMMENT '起始日期',
DUE_DATE DATE DEFAULT NULL COMMENT '到期日期'
)
COMMENT '标准字典信息'
CLUSTERED BY (STD_DICT_TYPE) INTO 11 BUCKETS
STORED AS ORC TBLPROPERTIES('transactional'='true');

DROP TABLE IF EXISTS fmi.f_cm_df_dict_mapp;
CREATE  TABLE fmi.f_cm_df_dict_mapp(
LP_CODE STRING DEFAULT NULL COMMENT '法人代码',
STD_DICT_TYPE STRING DEFAULT NULL COMMENT '标准字典类别',
STD_DICT_VALUE STRING DEFAULT NULL COMMENT '标准字典值',
SRC_DICT_TYPE STRING DEFAULT NULL COMMENT '源字典类别',
SRC_DICT_TYPE_NAME STRING DEFAULT NULL COMMENT '源字典类别名称',
SRC_DICT_VALUE STRING DEFAULT NULL COMMENT '源字典值',
SRC_DICT_VALUE_NAME STRING DEFAULT NULL COMMENT '源字典值名称',
SRC_SYSTEM_NO STRING DEFAULT NULL COMMENT '源系统号',
MAPPG_TYPE STRING DEFAULT NULL COMMENT '映射类型',
START_DATE DATE DEFAULT NULL COMMENT '起始日期',
DUE_DATE DATE DEFAULT NULL COMMENT '到期日期'
)
COMMENT '字典对照信息'
CLUSTERED BY (STD_DICT_TYPE) INTO 11 BUCKETS
STORED AS ORC TBLPROPERTIES('transactional'='true');

DROP TABLE IF EXISTS fmi.f_cm_df_column_dict;
CREATE  TABLE fmi.f_cm_df_column_dict(
LP_CODE STRING DEFAULT NULL COMMENT '法人代码',
TABLE_NAME STRING DEFAULT NULL COMMENT '表名',
COLUMN_NAME STRING DEFAULT NULL COMMENT '字段名',
STD_DICT_TYPE STRING DEFAULT NULL COMMENT '标准字典类别',
SRC_DICT_TYPE STRING DEFAULT NULL COMMENT '源字典类别',
ONE_TO_ONE_IDE_CODE STRING DEFAULT NULL COMMENT '一对一标识码',
START_DATE DATE DEFAULT NULL COMMENT '起始日期',
DUE_DATE DATE DEFAULT NULL COMMENT '到期日期 '
)
COMMENT '表字段字典信息'
CLUSTERED BY (TABLE_NAME) INTO 11 BUCKETS
STORED AS ORC TBLPROPERTIES('transactional'='true');
