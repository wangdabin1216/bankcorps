!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_JIEJWD_HS   (i_acct_date IN DATE) IS
/************************************************************************
过程中文名  借据维度表
功能描述：  存放客户的借据维度信息
编写人：    MSY
编写日期：  2020-03-09
修改记录：   1.  2020-03-09   by   msy      程序创建完成
*************************************************************************/
--通用变量
l_trlg        cc.pkg_dw_util.r_trlg; --日志变量组
v_acct_date   DATE; --操作日期
v_object_name STRING; --操作对象
v_count       INT; --源表记录数
v_begin_time  TIMESTAMP; --整个过程的开始时间
warn_exception  EXCEPTION; --声明警告的异常变量
error_exception EXCEPTION; --声明错误的异常变量
v_system_flag STRING; --定义系统标识的变量
v_partid      STRING;
o_log_code STRING;
o_log_desc STRING;
BEGIN
--0   处理准备
--0.1  设置环境
set_env('transaction.type', 'inceptor');
--0.2  初始化变量
v_acct_date   := i_acct_date;
v_object_name := 'DMM.PRO_RM_JIEJWD_HS';
v_begin_time  := systimestamp;
v_system_flag := 'DMM';
v_partid      := to_char(v_acct_date, 'yyyyMM');
--0.3   日志变量组的初始化
l_trlg.pro_name    := 'DMM.PRO_RM_JIEJWD_HS';
l_trlg.log_object  := 'DMM.RM_JIEJWD_HS';
l_trlg.etl_date    := v_acct_date;
l_trlg.system_flag := v_system_flag;
--0.4   写处理开始的日志
l_trlg.log_desc   := v_object_name || ' 处理开始';
l_trlg.log_action := 'Begin';
l_trlg.log_seq    := 0;
l_trlg.begin_time := systimestamp;
cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
--1     转换处理
--1.1   转换前 删除数据
delete from DMM.RM_JIEJWD_HS where data_dt = v_acct_date;
--1.2创建临时表
EXECUTE IMMEDIATE ('drop table if exists DMM.RM_JIEJWD_HS_TEMP');

EXECUTE IMMEDIATE ("CREATE TABLE DMM.RM_JIEJWD_HS_TEMP(
BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
DUE_NO STRING NOT NULL COMMENT '借据号',
CONT_NO STRING COMMENT '合同编号',
CIF_NO STRING COMMENT '互金客户号',
PRDT_NO  STRING COMMENT '产品编号',
BASE_ACCT_NO STRING COMMENT '核心账号',
ACCT_SEQ_NO       STRING COMMENT '核心账户序列号',
CMISLOAN_NO       STRING COMMENT '核心借据号',
CLIENT_NO         STRING COMMENT '核心客户号',
PROD_TYPE         STRING COMMENT '核心产品类型',
REVERSAL          STRING COMMENT '冲正标识',
REPAY_TYPE        STRING  COMMENT '还款方式',
REPAY_PERIOD      STRING  COMMENT '还款周期',
REPAY_DAY_TYP     STRING  COMMENT '还款日方式',
GRACE_DAYS        DECIMAL(17,0)  COMMENT '宽限期天数',
LN_USE            STRING  COMMENT '贷款用途',
REPAY_SOURCE      STRING  COMMENT '还款来源',
BEG_DATE          DATE COMMENT '发放日期',
END_DATE          DATE COMMENT '到期日期',
STAGE_NO          STRING  COMMENT '期数',
LN_RATE           DECIMAL(17,2)  COMMENT '执行利率',
OVER_RATE         DECIMAL(17,2)  COMMENT '逾期利率',
CURR_OVERDAYS     DECIMAL(17,0)  COMMENT '当前逾期天数',
CURR_OVERMONTHS   DECIMAL(17,0)  COMMENT '当前逾期期数',
LAST_PAY_DATE     DATE  COMMENT '最近一次还款日期'
) 
COMMENT '借据维度临时表' 
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true')
;")
--1.3 数据处理
--处理借据信息


begin
l_trlg.log_desc   := v_object_name || '-1.1将客户的部分借据信息插入目标临时表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_JIEJWD_HS_TEMP(
 DATA_DT	      --统计日期
,DUE_NO	          --借据号
,CONT_NO	      --合同编号
,CIF_NO	          --互金客户号
,PRDT_NO	      --产品编号
,BASE_ACCT_NO     --核心账号
,ACCT_SEQ_NO	  --核心账户序列号
,CMISLOAN_NO	  --核心借据号
,CLIENT_NO	      --核心客户号
,PROD_TYPE	      --核心产品类型
,REVERSAL	      --冲正标识
,REPAY_TYPE	      --还款方式
,REPAY_PERIOD     --还款周期
,REPAY_DAY_TYP    --还款日方式
,GRACE_DAYS	      --宽限期天数
,LN_USE	          --贷款用途
,REPAY_SOURCE	  --还款来源
,BEG_DATE	      --发放日期
,END_DATE	      --到期日期
,STAGE_NO	      --期数
,LN_RATE	      --执行利率
,OVER_RATE	      --逾期利率
,CURR_OVERDAYS	  --当前逾期天数
,CURR_OVERMONTHS  --当前逾期期数
,LAST_PAY_DATE	  --最近一次还款日期
,BEGNDT           --开始日期
,OVERDT           --结束日期
)
SELECT DISTINCT
  A.DATA_DT ,       --统计日期
  A.DUE_NO ,        --借据号
  A.CONT_NO ,       --合同编号
  A.CIF_NO ,        --互金客户号
  A.PRDT_NO ,       --产品编号
  A.BASE_ACCT_NO ,  --核心账号
  A.ACCT_SEQ_NO ,   --核心账户序列号
  A.CMISLOAN_NO ,   --核心借据号
  A.CLIENT_NO ,     --核心客户号
  A.PROD_TYPE ,     --核心产品类型
  A.REVERSAL ,      --冲正标识
  A.REPAY_TYPE ,    --还款方式
  A.REPAY_PERIOD ,  --还款周期
  A.REPAY_DAY_TYP , --还款日方式
  A.GRACE_DAYS ,    --宽限期天数
  A.LN_USE ,        --贷款用途
  A.REPAY_SOURCE ,  --还款来源
  A.BEG_DATE ,      --发放日期
  A.END_DATE ,      --到期日期
  CASE
    WHEN D2.MAX_STAGE_NO1 IS NOT NULL
    AND E.MAX_STAGE_NO2  IS NOT NULL
    THEN D2.MAX_STAGE_NO1 + E.MAX_STAGE_NO2	
    WHEN D2.MAX_STAGE_NO1 IS NOT NULL
    AND E.MAX_STAGE_NO2 IS NULL
    THEN D2.MAX_STAGE_NO1
    ELSE E.MAX_STAGE_NO2
  END AS STAGE_NO, --期数
  A.LN_RATE ,      --执行利率
  A.OVER_RATE ,    --逾期利率
  CASE
    WHEN D.INTERNAL_KEY IS NOT NULL
    THEN D.OVER_DAYS
    ELSE 0
  END, --当前逾期天数
  CASE
    WHEN D.OVER_DAYS BETWEEN 0 AND 30
    THEN 1
    WHEN D.OVER_DAYS BETWEEN 31 AND 60
    THEN 2
    WHEN D.OVER_DAYS BETWEEN 61 AND 90
    THEN 3
    WHEN D.OVER_DAYS > 90
    THEN 4
    ELSE 1
  END                      AS OVER_MONTHS,  --当前逾期期数
  D1.MAX_FINAL_SETTLE_DATE AS MAX_PAY_DATE, --最近一次还款日期
  A.BEGNDT ,                         --开始日期
  A.OVERDT                           --结束日期
FROM
  DMM.RM_JIEJHK_PUBLIC A
  /*借据还款公共表*/
LEFT JOIN
(
  SELECT DISTINCT
    D.INTERNAL_KEY,
    D.OVER_DAYS
  FROM
    (
      SELECT
        D.INTERNAL_KEY,
		DATEDIFF(v_acct_date,DATE(MIN(D.DUE_DATE) OVER (PARTITION BY D.INTERNAL_KEY))) AS OVER_DAYS
      FROM
        OMI.CB_MB_INVOICE_HS D
      WHERE
        D.FULLY_SETTLED='N'
      AND D.BEGNDT    <= v_acct_date
      AND D.OVERDT     > v_acct_date
	  AND D.PARTID = v_partid  
    )
    D
)
D
/*单据表：取逾期天数*/
ON A.INTERNAL_KEY = D.INTERNAL_KEY
/*账户标识符*/
LEFT JOIN
(
  SELECT DISTINCT
    D1.INTERNAL_KEY,
    D1.MAX_FINAL_SETTLE_DATE
  FROM
    (
      SELECT
        D1.INTERNAL_KEY,
        MAX(D1.FINAL_SETTLE_DATE) OVER (PARTITION BY D1.INTERNAL_KEY) AS MAX_FINAL_SETTLE_DATE
      FROM
        OMI.CB_MB_INVOICE_HS D1
      WHERE
        D1.FULLY_SETTLED='Y'
      AND D1.BEGNDT    <= v_acct_date
      AND D1.OVERDT     > v_acct_date
	  AND D1.PARTID = v_partid
    )
    D1
)
D1
/*单据表：取最近还款日期*/
ON A.INTERNAL_KEY = D1.INTERNAL_KEY
/*账户标识符*/
LEFT JOIN
(
  SELECT DISTINCT
    D2.INTERNAL_KEY,
    D2.MAX_STAGE_NO1
  FROM
    (
      SELECT
        D2.INTERNAL_KEY,
        MAX(TO_NUMBER(D2.STAGE_NO)) OVER (PARTITION BY D2.INTERNAL_KEY) AS MAX_STAGE_NO1
      FROM
        OMI.CB_MB_INVOICE_HS D2
      WHERE
        D2.TRAN_DATE    <= v_acct_date
	  AND D2.PARTID = '202003'  
    )
    D2
)
D2
/*单据表：取期次*/
ON A.INTERNAL_KEY = D2.INTERNAL_KEY
/*账户标识符*/
LEFT JOIN
(
  SELECT DISTINCT
    E.INTERNAL_KEY,
    E.MAX_STAGE_NO2
  FROM
    (
      SELECT
        E.INTERNAL_KEY,
        MAX(TO_NUMBER(E.STAGE_NO)) OVER (PARTITION BY E.INTERNAL_KEY) AS MAX_STAGE_NO2
      FROM
        OMI.CB_MB_ACCT_SCHEDULE_DETAIL_HS E
      WHERE
        E.BEGNDT  <= v_acct_date
      AND E.OVERDT > v_acct_date
	  AND E.PARTID = v_partid
    )
    E
)
E
/*账户计划明细表*/
ON A.INTERNAL_KEY = E.INTERNAL_KEY
/*账户标识符*/
WHERE
    A.DATA_DT = v_acct_date
;


l_trlg.row_count := SQL%ROWCOUNT;
cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
EXCEPTION
WHEN OTHERS THEN
l_trlg.log_code := SQLCODE();
l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
RAISE error_exception;
end;

--处理全部借据信息


begin
l_trlg.log_desc   := v_object_name || '-2.1将客户的部分借据信息插入目标临时表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_JIEJWD_HS(
 DATA_DT	      --统计日期
,DUE_NO	          --借据号
,CONT_NO	      --合同编号
,CIF_NO	          --互金客户号
,PRDT_NO	      --产品编号
,BASE_ACCT_NO     --核心账号
,ACCT_SEQ_NO	  --核心账户序列号
,CMISLOAN_NO	  --核心借据号
,CLIENT_NO	      --核心客户号
,PROD_TYPE	      --核心产品类型
,REVERSAL	      --冲正标识
,STATUS	          --借据状态
,REPAY_TYPE	      --还款方式
,REPAY_PERIOD     --还款周期
,REPAY_DAY_TYP    --还款日方式
,GRACE_DAYS	      --宽限期天数
,LN_USE	          --贷款用途
,REPAY_SOURCE	  --还款来源
,BEG_DATE	      --发放日期
,END_DATE	      --到期日期
,STAGE_NO	      --期数
,LN_RATE	      --执行利率
,OVER_RATE	      --逾期利率
,CURR_OVERDAYS	  --当前逾期天数
,CURR_OVERMONTHS  --当前逾期期数
,LAST_PAY_DATE	  --最近一次还款日期
,HIS_MAX_PAYDAYS  --历史最高逾期天数
,HIS_MAX_PAYMONTHS--历史最高逾期期数
,FIRST_OVER_DATE  --首次逾期日期
,BEGNDT     --开始日期
,OVERDT     --结束日期
)
SELECT
  A.DATA_DT , 
  A.DUE_NO , 
  A.CONT_NO ,
  A.CIF_NO ,
  A.PRDT_NO ,
  A.BASE_ACCT_NO ,
  A.ACCT_SEQ_NO ,
  A.CMISLOAN_NO ,
  A.CLIENT_NO ,
  A.PROD_TYPE ,
  A.REVERSAL ,
  CASE
    WHEN C.ZDYE > 0
    THEN '逾期'
    WHEN C.ZDYE = 0
    THEN '结清'
    ELSE '正常'
  END AS STATUS, --借据状态
  A.REPAY_TYPE ,
  A.REPAY_PERIOD ,
  A.REPAY_DAY_TYP ,
  A.GRACE_DAYS ,
  A.LN_USE ,
  A.REPAY_SOURCE ,
  A.BEG_DATE ,
  A.END_DATE ,
  A.STAGE_NO ,
  A.LN_RATE ,
  A.OVER_RATE ,
  A.CURR_OVERDAYS ,
  A.CURR_OVERMONTHS ,
  A.LAST_PAY_DATE ,
  CASE WHEN A.CURR_OVERDAYS > B.CURR_OVERDAYS
    THEN A.CURR_OVERDAYS
    ELSE B.CURR_OVERDAYS
  END AS HIS_MAX_PAYDAYS, --历史最高逾期天数
  CASE WHEN A.CURR_OVERMONTHS > B.CURR_OVERMONTHS
    THEN A.CURR_OVERMONTHS
    ELSE B.CURR_OVERMONTHS
  END AS HIS_MAX_PAYMONTHS, --历史最高逾期期数
  CASE
    WHEN B.DUE_NO IS NULL
    THEN DATE('19000101')
    WHEN B.FIRST_OVER_DATE = DATE('19000101')
    THEN A.DATA_DT
    ELSE B.FIRST_OVER_DATE
  END AS FIRST_OVER_DATE ,--首次逾期日期
  v_acct_date , --开始日期
  v_acct_date + 1 --结束日期
FROM
  DMM.RM_JIEJWD_HS_TEMP A
LEFT JOIN DMM.RM_JIEJWD_HS B
ON
  A.DUE_NO    = B.DUE_NO
AND B.DATA_DT = A.DATA_DT - 1
LEFT JOIN DMM.RM_JIEJZB_HS C
ON
  A.DUE_NO    = C.DUE_NO
AND C.DATA_DT = v_acct_date
WHERE
  A.DATA_DT = v_acct_date ;

l_trlg.row_count := SQL%ROWCOUNT;
cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
EXCEPTION
WHEN OTHERS THEN
l_trlg.log_code := SQLCODE();
l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
RAISE error_exception;
end;

--9   返回值处理
--9.0   所有代码执行成功，为返回值赋值
o_log_code := cc.pkg_dw_util.log_code_ok;
o_log_desc := cc.pkg_dw_util.log_desc_ok;
EXCEPTION
WHEN warn_exception THEN
o_log_code := l_trlg.log_code;
o_log_desc := l_trlg.log_desc;
cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
put_line(l_trlg.log_code);
WHEN error_exception THEN
o_log_code := l_trlg.log_code;
o_log_desc := l_trlg.log_desc;
cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
put_line(l_trlg.log_code);
WHEN OTHERS THEN
l_trlg.log_code := SQLCODE();
l_trlg.log_desc := SQLERRM();
o_log_code      := l_trlg.log_code;
o_log_desc      := l_trlg.log_desc;
cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
put_line(l_trlg.log_code);
END;
/
