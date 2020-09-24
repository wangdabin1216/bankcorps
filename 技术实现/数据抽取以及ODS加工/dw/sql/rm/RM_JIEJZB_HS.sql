!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_JIEJZB_HS   (i_acct_date IN DATE) IS
/************************************************************************
过程中文名：借据指标表
功能描述：  存放客户的借据信息
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
v_object_name := 'DMM.PRO_RM_JIEJZB_HS';
v_begin_time  := systimestamp;
v_system_flag := 'DMM';
v_partid      := to_char(v_acct_date, 'yyyyMM');
--0.3   日志变量组的初始化
l_trlg.pro_name    := 'DMM.PRO_RM_JIEJZB_HS';
l_trlg.log_object  := 'DMM.RM_JIEJZB_HS';
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
delete from DMM.RM_JIEJZB_HS where data_dt = v_acct_date;
--1.2创建临时表
--1.3 数据处理
--处理借据信息


begin
l_trlg.log_desc   := v_object_name || '-1.1将客户的借据信息插入目标表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_JIEJZB_HS(
 DATA_DT  --统计日期
,CIF_NO	  --客户号
,DUE_NO	  --借据号
,PRDT_NO  --产品编号
,DUE_COUNT--贷款笔数
,DUE_AMT  --贷款金额
,ZDYE	  --在贷余额
,ZDYEBJ	  --在贷余额本金
,ZDYELX	  --在贷余额利息
,YQYE	  --逾期余额
,YQBJ	  --逾期本金
,BEGNDT     --开始日期
,OVERDT     --结束日期
)  
SELECT
  A.DATA_DT ,   --统计日期
  A.CIF_NO ,    --客户号
  A.DUE_NO ,    --借据号
  A.PRDT_NO ,   --产品编号
  A.COUNT_NUM,  --贷款笔数
  A.DUE_AMT ,   --贷款金额
  CASE
    WHEN A.SUM_ALL IS NOT NULL
    THEN A.SUM_ALL
    ELSE 0
  END AS SUM_ALL, --在贷余额
  CASE
    WHEN A.SUM_PRI IS NOT NULL
    THEN A.SUM_PRI
    ELSE 0
  END AS SUM_PRI, --在贷余额本金
  CASE
    WHEN A.SUM_INT IS NOT NULL
    THEN A.SUM_PRI
    ELSE 0
  END AS SUM_INT, --在贷余额利息
  CASE
    WHEN A.MAX_DUE_DATE IS NOT NULL
    AND A.MAX_DUE_DATE  <= v_acct_date
    THEN A.SUM_ALL
    ELSE 0
  END, --逾期余额
  CASE
    WHEN A.MAX_DUE_DATE IS NOT NULL
    AND A.MAX_DUE_DATE  <= v_acct_date
    THEN A.SUM_PRI
    ELSE 0
  END ,           --逾期本金
  A.BEGNDT ,      --开始日期
  A.OVERDT        --结束日期
FROM    
(SELECT
  A.DATA_DT,
  A.CIF_NO ,
  A.DUE_NO ,
  A.PRDT_NO ,
  A.DUE_AMT ,
  A.BEGNDT,
  A.OVERDT,
  C.MAX_DUE_DATE ,
  COUNT(DISTINCT A.INTERNAL_KEY) AS COUNT_NUM,
  SUM(
    CASE
      WHEN C.INTERNAL_KEY IS NOT NULL
	  AND D.INTERNAL_KEY IS NOT NULL
      THEN C.INVOICE_SUM_ALL+D.DETAIL_SUM_ALL
      ELSE C.INVOICE_SUM_ALL
    END) AS SUM_ALL,
  SUM(
    CASE
      WHEN C.INTERNAL_KEY IS NOT NULL
	  AND D.INTERNAL_KEY IS NOT NULL
      THEN C.INVOICE_SUM_PRI+D.DETAIL_SUM_PRI
      ELSE C.INVOICE_SUM_PRI
    END) AS SUM_PRI,
  SUM(
    CASE
      WHEN C.INTERNAL_KEY IS NOT NULL
	  AND D.INTERNAL_KEY IS NOT NULL
      THEN C.INVOICE_SUM_INT+D.DETAIL_SUM_INT
      ELSE C.INVOICE_SUM_INT
    END) AS SUM_INT
FROM
  DMM.RM_JIEJHK_PUBLIC A
  /*借据还款公共表*/
LEFT JOIN
(
  SELECT DISTINCT
    C.INTERNAL_KEY ,
    C.MAX_DUE_DATE,
    C.INVOICE_SUM_ALL,
    C.INVOICE_SUM_PRI,
    C.INVOICE_SUM_INT
  FROM
    (
      SELECT
        C.INTERNAL_KEY,
        MAX(C.DUE_DATE) OVER (PARTITION BY C.INTERNAL_KEY)   AS MAX_DUE_DATE,
        SUM(C.BILLED_AMT) OVER (PARTITION BY C.INTERNAL_KEY) AS INVOICE_SUM_ALL,
        SUM(
          CASE
            WHEN C.AMT_TYPE = 'PRI'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY) AS INVOICE_SUM_PRI,
        SUM(
          CASE
            WHEN C.AMT_TYPE <> 'PRI'
            THEN C.BILLED_AMT
            ELSE 0
          END) OVER (PARTITION BY C.INTERNAL_KEY) AS INVOICE_SUM_INT
      FROM
        OMI.CB_MB_INVOICE_HS C
      WHERE
        C.FULLY_SETTLED='N'
      AND C.BEGNDT    <= V_ACCT_DATE
      AND C.OVERDT     > V_ACCT_DATE
      AND C.PARTID     = v_partid
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
    D.DETAIL_SUM_ALL,
    D.DETAIL_SUM_PRI,
    D.DETAIL_SUM_INT
  FROM
    (
      SELECT
        D.INTERNAL_KEY,
        SUM(D.SCHED_AMT) OVER (PARTITION BY D.INTERNAL_KEY) AS DETAIL_SUM_ALL,
        SUM(
          CASE
            WHEN D.AMT_TYPE = 'PRI'
            THEN D.SCHED_AMT
            ELSE 0
          END) OVER (PARTITION BY D.INTERNAL_KEY) AS DETAIL_SUM_PRI,
        SUM(
          CASE
            WHEN D.AMT_TYPE <> 'PRI'
            THEN D.SCHED_AMT
            ELSE 0
          END) OVER (PARTITION BY D.INTERNAL_KEY) AS DETAIL_SUM_INT
      FROM
        OMI.CB_MB_ACCT_SCHEDULE_DETAIL_HS D
      WHERE
        D.BEGNDT  <= V_ACCT_DATE
      AND D.OVERDT > V_ACCT_DATE
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
GROUP BY
  A.DATA_DT,
  A.CIF_NO ,
  A.DUE_NO ,
  A.PRDT_NO ,
  A.DUE_AMT ,
  A.BEGNDT,
  A.OVERDT,
  C.MAX_DUE_DATE ) A ;

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
