CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_MB_ACC_INT_DET_SPL
(IRL_SEQ_NO VARCHAR(50) comment '序号'
  , SYSTEM_ID VARCHAR(20) comment '系统标识'
  , INTERNAL_KEY DECIMAL(15) comment '账户标识符'
  , INT_CLASS VARCHAR(10) comment '利息分类'
  , START_DATE VARCHAR(8) comment '开始日期'
  , END_DATE VARCHAR(8) comment '结束日期'
  , PERI_SPLIT_ID VARCHAR(10) comment '周期分段ID'
  , PERI_SEQ_NO VARCHAR(5) comment '周期分段序号'
  , AMT_SPLIT_ID VARCHAR(10) comment '金额分段ID'
  , AMT_SEQ_NO VARCHAR(5) comment '金额分段序号'
  , NEAR_AMT DECIMAL(17, 2) comment '阶梯金额'
  , ACCR_AMT DECIMAL(17, 2) comment '计提金额'
  , NEAR_PERIOD_TYPE VARCHAR(1) comment '分段周期类型'
  , NEAR_PERIOD VARCHAR(5) comment '分段周期'
  , ACCR_DAYS VARCHAR(10) comment '计息天数'
  , ACTUAL_RATE DECIMAL(15, 8) comment '行内利率'
  , FLOAT_RATE DECIMAL(15, 8) comment '浮动利率'
  , REAL_RATE DECIMAL(15, 8) comment '执行利率'
  , ACCT_SPREAD_RATE DECIMAL(15, 8) comment '分户浮动百分点'
  , ACCT_PERCENT_RATE DECIMAL(5, 2) comment '分户浮动百分比'
  , ACCT_FIXED_RATE DECIMAL(15, 8) comment '分户固定值'
  , INT_TYPE VARCHAR(3) comment '利率类型'
  , AMT_SPLIT_MODE VARCHAR(1) comment ''
  , PERI_SPLIT_MODE VARCHAR(1) comment ''
  , YEAR_BASIS VARCHAR(3) comment ''
  , MONTH_BASIS VARCHAR(3) comment ''
  , RECAL_METHOD VARCHAR(1) comment '重算方式'
  , TRAN_TIMESTAMP VARCHAR(17) comment '交易时间戳'
  , TRAN_TIME DECIMAL(11) comment '交易时间'
  , ROUTER_KEY VARCHAR(100) comment '分库路由关键字')
COMMENT '利息明细分段表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/mb_acc_int_det_spl';

CREATE TABLE IF NOT EXISTS omi.ORC_MB_ACC_INT_DET_SPL
(PRJT_NAME VARCHAR(20) COMMENT '项目名称'
  , INTERNAL_KEY DECIMAL(15) COMMENT '账户标识符'
  , TOTAL_AMOUNT_PREV DECIMAL(17, 2) COMMENT '总金额'
  , REAL_RATE DECIMAL(15, 8) comment '执行利率'
  , ETL_DATE VARCHAR(8) COMMENT '数据插入日期')
COMMENT '众邦联合贷款中间表'
CLUSTERED BY(INTERNAL_KEY)
INTO 11 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");


