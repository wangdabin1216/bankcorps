--中间表表更
CREATE TABLE IF NOT EXISTS omi.MID_MB_ACCT_BALANCE_TEST
(etl_date VARCHAR(8) COMMENT '数据日期YYYYMMDD'
  , prjt_name VARCHAR(20) COMMENT '项目名称'
  , total_amount_prev DECIMAL(17, 2) COMMENT '上日总金额'
  ,bal DECIMAL(22,2) COMMENT '借据余额')
COMMENT '上日存款余额中间表'
CLUSTERED BY(prdt_name)
INTO 10 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

INSERT INTO omi.mid_mb_acct_balance_test
SELECT
    data_date
     , '理财魔方项目' AS prjt_name
     , total_amount_prev
     , NULL AS bal
FROM
    omi.mid_mb_acct_balance;

SELECT * FROM omi.mid_mb_acct_balance_test;
DROP TABLE omi.mid_mb_acct_balance;

CREATE TABLE IF NOT EXISTS omi.MID_MB_ACCT_BALANCE
(etl_date VARCHAR(8) COMMENT '数据日期YYYYMMDD'
  , prjt_name VARCHAR(20) COMMENT '项目名称'
  , total_amount_prev DECIMAL(17, 2) COMMENT '上日总金额'
  ,bal DECIMAL(22,2) COMMENT '借据余额')
COMMENT '贷款存款余额中间表'
CLUSTERED BY(prjt_name)
INTO 10 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

INSERT INTO omi.mid_mb_acct_balance
SELECT
    etl_date
     , prjt_name
     , total_amount_prev
     , bal
FROM
    omi.mid_mb_acct_balance_test;
SELECT * FROM omi.mid_mb_acct_balance;
DROP TABLE omi.mid_mb_acct_balance_test;

--备份历史月报
CREATE TABLE IF NOT EXISTS omi.rpt_partner_fee_month_bk LIKE omi.rpt_partner_fee_month;
INSERT INTO omi.rpt_partner_fee_month_bk SELECT * FROM omi.rpt_partner_fee_month;
DROP TABLE omi.rpt_partner_fee_month;
--创建月报表
CREATE TABLE IF NOT EXISTS omi.RPT_PARTNER_FEE_MONTH
(RPT_NO VARCHAR(32) COMMENT '项目编号'
  , PRJT_NAME VARCHAR(60) COMMENT '项目名称'
  , SERVICE_FEE DECIMAL(22, 2) COMMENT '收费金额'
  , BAL DECIMAL(22, 2) COMMENT '贷款余额'
  , AV_BAL DECIMAL(22,2) COMMENT '日均贷款余额'
  , TOTAL_AMOUNT_PREV DECIMAL(22,2) COMMENT '有效存款余额'
  , AV_AMOUNT_PREV DECIMAL(22,2) COMMENT '日均有效存款余额'
  ,RATE DECIMAL(5,3) COMMENT '费率'
  ,AV_LN_RATE DECIMAL(22,2) COMMENT '利率平均值'
  ,COUNT_MON VARCHAR(6) COMMENT '统计月份YYYYMM'
  ,UPDT_DATE VARCHAR(8) COMMENT '报表统计日期YYYYMMDD'
  ,REMARK VARCHAR(2000) COMMENT '备注'
  )
COMMENT '月结合作方服务费统计表'
CLUSTERED BY(PRJT_NAME)
INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

--沣邦变更
DELETE FROM omi.orc_pro_relation WHERE prdt_name IN ('沣邦汽车电子存款户','沣邦汽车(二期)电子存款户');
UPDATE omi.orc_pro_relation SET prjt_name='沣邦汽车（消费）' WHERE prdt_name='沣邦汽车（消费）';
UPDATE omi.orc_pro_relation SET prjt_name='沣邦汽车（经营）' WHERE prdt_name='沣邦汽车（经营）';

--删除不再使用的表
DROP TABLE omi.ext_ln_auth_info;
DROP TABLE omi.ext_ln_loan_plan_reg;
DROP TABLE omi.ext_ln_reg;
DROP TABLE omi.ext_ln_reg_hst;
