!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_HKZB_HS   (i_acct_date IN DATE) IS
/************************************************************************
过程中文名：还款指标表
功能描述：  存放客户的还款指标信息
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
v_object_name := 'DMM.PRO_RM_HKZB_HS';
v_begin_time  := systimestamp;
v_system_flag := 'DMM';
v_partid      := to_char(v_acct_date, 'yyyyMM');
--0.3   日志变量组的初始化
l_trlg.pro_name    := 'DMM.PRO_RM_HKZB_HS';
l_trlg.log_object  := 'DMM.RM_HKZB_HS';
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
delete from DMM.RM_HKZB_HS where data_dt = v_acct_date;
--1.2创建临时表
EXECUTE IMMEDIATE ('drop table if exists DMM.RM_HKZB_HS_TEMP');

EXECUTE IMMEDIATE ("CREATE TABLE DMM.RM_HKZB_HS_TEMP(
BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
CIF_NO STRING COMMENT '互金客户号',
DUE_NO STRING NOT NULL COMMENT '借据号',
STAGE_NO STRING COMMENT '期次',
PRDT_NO STRING  COMMENT '产品编号',
Y_SUM DECIMAL(17,2) COMMENT '应还总额',
Y_SUM_PRI DECIMAL(17,2) COMMENT '应还本金',
Y_SUM_INT DECIMAL(17,2) COMMENT '应还利息',
S_SUM     DECIMAL(17,2) COMMENT '实还总额',
S_SUM_PRI DECIMAL(17,2) COMMENT '实还本金',
S_SUM_INT DECIMAL(17,2) COMMENT '实还利息',
D_SUM     DECIMAL(17,2) COMMENT '已到期总额',
D_SUM_PRI DECIMAL(17,2) COMMENT '已到期本金',
D_SUM_INT DECIMAL(17,2) COMMENT '已到期利息',
W_SUM     DECIMAL(17,2) COMMENT '未到期总额',
W_SUM_PRI DECIMAL(17,2) COMMENT '未到期本金',
W_SUM_INT DECIMAL(17,2) COMMENT '未到期利息'
) 
COMMENT '还款指标临时表'
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true')
;")
--1.3 数据处理
--处理还款信息


begin
l_trlg.log_desc   := v_object_name || '-1.1将客户已出账的还款信息插入临时表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_HKZB_HS_TEMP(
 DATA_DT   --统计日期
,CIF_NO	   --互金客户号
,DUE_NO	   --借据号
,STAGE_NO  --期次
,PRDT_NO   --产品编号
,Y_SUM	   --应还总额
,Y_SUM_PRI --应还本金
,Y_SUM_INT --应还利息
,S_SUM	   --实还总额
,S_SUM_PRI --实还本金
,S_SUM_INT --实还利息
,D_SUM	   --已到期总额
,D_SUM_PRI --已到期本金
,D_SUM_INT --已到期利息
,W_SUM	   --未到期总额
,W_SUM_PRI --未到期本金
,W_SUM_INT --未到期利息
,BEGNDT     --开始日期
,OVERDT     --结束日期
)  
SELECT
  A.DATA_DT , --统计日期
  A.CIF_NO ,    --互金客户号
  A.DUE_NO ,    --借据号
  TO_NUMBER(C.STAGE_NO),     --期次
  A.PRDT_NO,                 --产品编号
  C.ALL_SUM1 AS ALL_SUM,     --应还总额
  C.Y_SUM_PRI1 AS Y_SUM_PRI, --应还本金
  C.Y_SUM_INT1 AS Y_SUM_INT, --应还利息
  C.S_SUM ,       --实还总额
  C.S_SUM_PRI,    --实还本金
  C.S_SUM_INT,    --实还利息
  C.D_SUM,        --已到期总额
  C.D_SUM_PRI,    --已到期本金
  C.D_SUM_INT,    --已到期利息
  0 AS W_SUM,     --未到期总额
  0 AS W_SUM_PRI, --未到期本金
  0 AS W_SUM_INT, --未到期利息
  A.BEGNDT ,   --开始日期
  A.OVERDT --结束日期
FROM
  DMM.RM_JIEJHK_PUBLIC A
  /*借据还款公共表*/
LEFT JOIN
(
  SELECT DISTINCT
    C.INTERNAL_KEY,
    C.STAGE_NO,
    C.ALL_SUM1,
    C.Y_SUM_PRI1,
    C.Y_SUM_INT1,
    C.S_SUM,
    C.S_SUM_PRI,
    C.S_SUM_INT,
    C.D_SUM,
    C.D_SUM_PRI,
    C.D_SUM_INT
  FROM
    (
      SELECT
        C.INTERNAL_KEY,
        C.STAGE_NO,
        SUM(C.BILLED_AMT) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS ALL_SUM1,
        SUM(
          CASE
            WHEN C.AMT_TYPE = 'PRI'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS Y_SUM_PRI1,
        SUM(
          CASE
            WHEN C.AMT_TYPE <> 'PRI'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS Y_SUM_INT1,
        SUM(
          CASE
            WHEN C.FINAL_SETTLE_DATE <= v_acct_date
            AND C.FULLY_SETTLED      ='Y'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS S_SUM,
        SUM(
          CASE
            WHEN C.AMT_TYPE         = 'PRI'
            AND C.FINAL_SETTLE_DATE <= v_acct_date
            AND C.FULLY_SETTLED     ='Y'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS S_SUM_PRI,
        SUM(
          CASE
            WHEN C.AMT_TYPE        <> 'PRI'
            AND C.FINAL_SETTLE_DATE <= v_acct_date
            AND C.FULLY_SETTLED     ='Y'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS S_SUM_INT,
        SUM(
          CASE
            WHEN C.DUE_DATE <= v_acct_date
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS D_SUM,
        SUM(
          CASE
            WHEN C.DUE_DATE <= v_acct_date
            AND C.AMT_TYPE  = 'PRI'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS D_SUM_PRI,
        SUM(
          CASE
            WHEN C.DUE_DATE <= v_acct_date
            AND C.AMT_TYPE <> 'PRI'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO) AS D_SUM_INT
      FROM
        OMI.CB_MB_INVOICE_HS C
      WHERE
        C.BEGNDT  <= v_acct_date
      AND C.OVERDT > v_acct_date
      AND C.PARTID = v_partid
    )
    C
)
C
/*单据表*/
ON A.INTERNAL_KEY = C.INTERNAL_KEY
/*账户标识符*/
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

begin
l_trlg.log_desc   := v_object_name || '-1.2将客户未出账的还款信息插入临时表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_HKZB_HS_TEMP(
 DATA_DT   --统计日期
,CIF_NO	   --互金客户号
,DUE_NO	   --借据号
,STAGE_NO  --期次
,PRDT_NO   --产品编号
,Y_SUM	   --应还总额
,Y_SUM_PRI --应还本金
,Y_SUM_INT --应还利息
,S_SUM	   --实还总额
,S_SUM_PRI --实还本金
,S_SUM_INT --实还利息
,D_SUM	   --已到期总额
,D_SUM_PRI --已到期本金
,D_SUM_INT --已到期利息
,W_SUM	   --未到期总额
,W_SUM_PRI --未到期本金
,W_SUM_INT --未到期利息
,BEGNDT     --开始日期
,OVERDT     --结束日期
)  
SELECT
  A.DATA_DT , --统计日期
  A.CIF_NO , --互金客户号
  A.DUE_NO , --借据号
  CASE
    WHEN C.MAX_STAGE_NO IS NOT NULL
    AND D.STAGE_NO  IS NOT NULL
    THEN TO_NUMBER(C.MAX_STAGE_NO + D.STAGE_NO)
    WHEN C.MAX_STAGE_NO IS NULL
    AND D.STAGE_NO IS NOT NULL
    THEN TO_NUMBER(D.STAGE_NO)
  END AS STAGE_NO, --期次
  A.PRDT_NO, --产品编号
  D.ALL_SUM2   AS ALL_SUM, --应还总额
  D.Y_SUM_PRI2 AS Y_SUM_PRI, --应还本金
  D.Y_SUM_INT2 AS Y_SUM_INT, --应还利息
  0 AS S_SUM ,    --实还总额
  0 AS S_SUM_PRI, --实还本金
  0 AS S_SUM_INT, --实还利息
  0 AS D_SUM,     --已到期总额
  0 AS D_SUM_PRI, --已到期本金
  0 AS D_SUM_INT, --已到期利息
  D.W_SUM,     --未到期总额
  D.W_SUM_PRI, --未到期本金
  D.W_SUM_INT, --未到期利息
  A.BEGNDT, --开始日期
  A.OVERDT  --结束日期
FROM
  DMM.RM_JIEJHK_PUBLIC A
  /*借据还款公共表*/
LEFT JOIN
(
  SELECT DISTINCT
    C.INTERNAL_KEY,
    C.MAX_STAGE_NO
  FROM
    (
      SELECT
        C.INTERNAL_KEY,
        MAX(TO_NUMBER(C.STAGE_NO)) OVER (PARTITION BY C.INTERNAL_KEY) AS MAX_STAGE_NO
      FROM
        OMI.CB_MB_INVOICE_HS C
      WHERE
        C.BEGNDT  <= v_acct_date
      AND C.OVERDT > v_acct_date
      AND C.PARTID = v_partid
    )
    C
)
C
/*单据表*/
ON A.INTERNAL_KEY = C.INTERNAL_KEY
/*账户标识符*/
LEFT JOIN
(
  SELECT DISTINCT
    D.INTERNAL_KEY,
    D.STAGE_NO,
    D.ALL_SUM2,
    D.Y_SUM_PRI2,
    D.Y_SUM_INT2,
    D.W_SUM,
    D.W_SUM_PRI,
    D.W_SUM_INT
  FROM
    (
      SELECT
        D.INTERNAL_KEY,
        D.STAGE_NO,
        SUM(D.SCHED_AMT) OVER (PARTITION BY D.INTERNAL_KEY,D.STAGE_NO) AS ALL_SUM2,
        SUM(
          CASE
            WHEN D.AMT_TYPE = 'PRI'
            THEN D.SCHED_AMT
            ELSE 0
          END) OVER (PARTITION BY D.INTERNAL_KEY,D.STAGE_NO) AS Y_SUM_PRI2 ,
        SUM(
          CASE
            WHEN D.AMT_TYPE <> 'PRI'
            THEN D.SCHED_AMT
            ELSE 0
          END) OVER (PARTITION BY D.INTERNAL_KEY,D.STAGE_NO) AS Y_SUM_INT2 ,
        SUM(
          CASE
            WHEN D.END_DATE > v_acct_date
            THEN D.SCHED_AMT
            ELSE 0
          END) OVER (PARTITION BY D.INTERNAL_KEY,D.STAGE_NO) AS W_SUM ,
        SUM(
          CASE
            WHEN D.END_DATE > v_acct_date
            AND D.AMT_TYPE  = 'PRI'
            THEN D.SCHED_AMT
            ELSE 0
          END) OVER (PARTITION BY D.INTERNAL_KEY,D.STAGE_NO) AS W_SUM_PRI ,
        SUM(
          CASE
            WHEN D.END_DATE > v_acct_date
            AND D.AMT_TYPE <> 'PRI'
            THEN D.SCHED_AMT
            ELSE 0
          END) OVER (PARTITION BY D.INTERNAL_KEY,D.STAGE_NO) AS W_SUM_INT
      FROM
        OMI.CB_MB_ACCT_SCHEDULE_DETAIL_HS D
      WHERE
        D.BEGNDT  <= v_acct_date
      AND D.OVERDT > v_acct_date
      AND D.PARTID = v_partid
    )
    D
)
D
/*账户计划明细表*/
ON A.INTERNAL_KEY = D.INTERNAL_KEY
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


begin
l_trlg.log_desc   := v_object_name || '-1.3去除重复数据，将结果数据插入目标表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;


INSERT INTO DMM.RM_HKZB_HS(
 DATA_DT   --统计日期
,CIF_NO	   --互金客户号
,DUE_NO	   --借据号
,STAGE_NO  --期次
,PRDT_NO   --产品编号
,Y_SUM	   --应还总额
,Y_SUM_PRI --应还本金
,Y_SUM_INT --应还利息
,S_SUM	   --实还总额
,S_SUM_PRI --实还本金
,S_SUM_INT --实还利息
,D_SUM	   --已到期总额
,D_SUM_PRI --已到期本金
,D_SUM_INT --已到期利息
,W_SUM	   --未到期总额
,W_SUM_PRI --未到期本金
,W_SUM_INT --未到期利息
,BEGNDT     --开始日期
,OVERDT     --结束日期
)  
SELECT
 A.DATA_DT   --统计日期
,A.CIF_NO	 --互金客户号
,A.DUE_NO	 --借据号
,A.STAGE_NO  --期次
,A.PRDT_NO   --产品编号
,A.Y_SUM	 --应还总额
,A.Y_SUM_PRI --应还本金
,A.Y_SUM_INT --应还利息
,A.S_SUM	 --实还总额
,A.S_SUM_PRI --实还本金
,A.S_SUM_INT --实还利息
,A.D_SUM	 --已到期总额
,A.D_SUM_PRI --已到期本金
,A.D_SUM_INT --已到期利息
,A.W_SUM	 --未到期总额
,A.W_SUM_PRI --未到期本金
,A.W_SUM_INT --未到期利息
,A.BEGNDT    --开始日期
,A.OVERDT    --结束日期
FROM
  (
    SELECT
      DATA_DT ,
      CIF_NO ,
      DUE_NO ,
      STAGE_NO ,
      PRDT_NO ,
      Y_SUM ,
      Y_SUM_PRI ,
      Y_SUM_INT ,
      S_SUM ,
      S_SUM_PRI ,
      S_SUM_INT ,
      D_SUM ,
      D_SUM_PRI ,
      D_SUM_INT ,
      W_SUM ,
      W_SUM_PRI ,
      W_SUM_INT ,
      BEGNDT ,
      OVERDT ,
      ROW_NUMBER() OVER (PARTITION BY CIF_NO,DUE_NO,STAGE_NO) AS RN
    FROM
      DMM.RM_HKZB_HS_TEMP
	WHERE DATA_DT = v_acct_date
  )
  A
WHERE
  A.RN=1;

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
