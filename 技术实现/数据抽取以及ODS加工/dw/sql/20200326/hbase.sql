
CREATE  TABLE cc.dw_sm_hist(
  table_name string DEFAULT NULL COMMENT '表的名字', 
  system_flag string DEFAULT NULL COMMENT '表的系统标识', 
  table_hs_name string DEFAULT NULL COMMENT '表的历史表名', 
  fields string DEFAULT NULL COMMENT '表的字段序列', 
  keys string DEFAULT NULL COMMENT '表的主键', 
  region_type string DEFAULT NULL COMMENT '分区类型', 
  trans_type string DEFAULT NULL COMMENT '表的处理方式:chain or add', 
  hist_field string DEFAULT NULL COMMENT 'add插入过滤条件字段', 
  sync_type string DEFAULT NULL COMMENT '同步级别'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f0:system_flag,f0:table_hs_name,f0:fields,f0:keys,f0:region_type,f0:trans_type,f0:hist_field,f0:sync_type', 
  'serialization.format'='1')
LOCATION
  'hdfs://nameservice1/inceptor1/user/hive/warehouse/cc.db/dw/dw_sm_hist'
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_hist', 
  'transient_lastDdlTime'='1583217282')
  
  

CREATE  TABLE default.dw_sm_hist_test(
  table_name string DEFAULT NULL COMMENT '表的名字', 
  system_flag string DEFAULT NULL COMMENT '表的系统标识', 
  table_hs_name string DEFAULT NULL COMMENT '表的历史表名', 
  fields string DEFAULT NULL COMMENT '表的字段序列', 
  keys string DEFAULT NULL COMMENT '表的主键', 
  region_type string DEFAULT NULL COMMENT '分区类型', 
  trans_type string DEFAULT NULL COMMENT '表的处理方式:chain or add', 
  hist_field string DEFAULT NULL COMMENT 'add插入过滤条件字段', 
  sync_type string DEFAULT NULL COMMENT '同步级别'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f:q1,f:q2,f:q3,f:q4,f:q5,f:q6,f:q7,f:q8', 
  'serialization.format'='1')
LOCATION
  'hdfs://nameservice1/inceptor1/user/hive/warehouse/default.db/dw/dw_sm_hist_test'
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_hist_test', 
  'transient_lastDdlTime'='1585730337')

  
  CREATE  TABLE default.dw_sm_hist_test2(
  table_name string DEFAULT NULL COMMENT '表的名字', 
  system_flag string DEFAULT NULL COMMENT '表的系统标识', 
  table_hs_name string DEFAULT NULL COMMENT '表的历史表名', 
  fields string DEFAULT NULL COMMENT '表的字段序列', 
  keys string DEFAULT NULL COMMENT '表的主键', 
  region_type string DEFAULT NULL COMMENT '分区类型', 
  trans_type string DEFAULT NULL COMMENT '表的处理方式:chain or add', 
  hist_field string DEFAULT NULL COMMENT 'add插入过滤条件字段', 
  sync_type string DEFAULT NULL COMMENT '同步级别'
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f:q1,f:q2,f:q3,f:q4,f:q5,f:q6,f:q7,f:q8', 
  'serialization.format'='1')
LOCATION
  'hdfs://nameservice1/inceptor1/user/hive/warehouse/default.db/dw/dw_sm_hist_test2'
TBLPROPERTIES (
  'hbase.table.name'='dw_sm_hist_test2', 
  'transient_lastDdlTime'='1585730380')

  CREATE  TABLE default.dw_sm_hist_test4(
  table_name string DEFAULT NULL COMMENT '表的名字', 
  system_flag string DEFAULT NULL COMMENT '表的系统标识', 
  table_hs_name string DEFAULT NULL COMMENT '表的历史表名', 
  fields string DEFAULT NULL COMMENT '表的字段序列', 
  keys string DEFAULT NULL COMMENT '表的主键', 
  region_type string DEFAULT NULL COMMENT '分区类型', 
  trans_type string DEFAULT NULL COMMENT '表的处理方式:chain or add', 
  hist_field string DEFAULT NULL COMMENT 'add插入过滤条件字段', 
  sync_type string DEFAULT NULL COMMENT '同步级别'
)
ROW FORMAT SERDE 
  'io.transwarp.hyperdrive.serde.HyperdriveSerDe' 
STORED BY 
  'io.transwarp.hyperdrive.HyperdriveStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f0:system_flag,f0:table_hs_name,f0:fields,f0:keys,f0:region_type,f0:trans_type,f0:hist_field,f0:sync_type', 
  'hbase.columns.type'='string,string,string,string,string,string,string,string,string', 
  'serialization.format'='1')
LOCATION
  'hdfs://nameservice1/inceptor1/user/hive/warehouse/default.db/dw/default.dw_sm_hist_test4@hyperdrive.stargate'
TBLPROPERTIES (
  'hyperdrive.virtual.column'='_vc', 
  'hbase.table.name'='dw_sm_hist_test4', 
  'hyperdrive.virtual.family'='f0', 
  'transient_lastDdlTime'='1586331447')

  
  CREATE  TABLE default.dw_sm_hist_test5(
  table_name string DEFAULT NULL COMMENT '表的名字', 
  system_flag string DEFAULT NULL COMMENT '表的系统标识', 
  table_hs_name string DEFAULT NULL COMMENT '表的历史表名', 
  fields string DEFAULT NULL COMMENT '表的字段序列', 
  keys string DEFAULT NULL COMMENT '表的主键', 
  region_type string DEFAULT NULL COMMENT '分区类型', 
  trans_type string DEFAULT NULL COMMENT '表的处理方式:chain or add', 
  hist_field string DEFAULT NULL COMMENT 'add插入过滤条件字段', 
  sync_type string DEFAULT NULL COMMENT '同步级别'
)
ROW FORMAT SERDE 
  'io.transwarp.hyperdrive.serde.HyperdriveSerDe' 
STORED BY 
  'io.transwarp.hyperdrive.HyperdriveStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,f:q1,f:q2,f:q3,f:q4,f:q5,f:q6,f:q7,f:q8', 
  'hbase.columns.type'='string,string,string,string,string,string,string,string,string', 
  'serialization.format'='1')
LOCATION
  'hdfs://nameservice1/inceptor1/user/hive/warehouse/default.db/dw/default.dw_sm_hist_test5@hyperdrive.stargate'
TBLPROPERTIES (
  'hyperdrive.virtual.column'='_vc', 
  'hbase.table.name'='dw_sm_hist_test5', 
  'hyperdrive.virtual.family'='f', 
  'transient_lastDdlTime'='1586331487')
