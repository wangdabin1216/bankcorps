!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_KHZB_HS   (i_acct_date IN DATE) IS
/************************************************************************
过程中文名：客户指标表
功能描述：  存放客户指标信息
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
v_object_name := 'DMM.PRO_RM_KHZB_HS';
v_begin_time  := systimestamp;
v_system_flag := 'DMM';
v_partid      := to_char(v_acct_date, 'yyyyMM');
--0.3   日志变量组的初始化
l_trlg.pro_name    := 'DMM.PRO_RM_KHZB_HS';
l_trlg.log_object  := 'DMM.RM_KHZB_HS';
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
EXECUTE IMMEDIATE ('delete from ' || l_trlg.log_object ||' where data_dt = '||l_trlg.etl_date);
--1.2创建临时表
--1.3 数据处理
--处理借据信息


begin
l_trlg.log_desc   := v_object_name || '-1.1将客户指标信息插入目标表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_KHZB_HS(
 DATA_DT	    --统计日期
,CIF_NO	        --互金客户号
,CLIENT_NO	    --核心客户号
,PRDT_NUM	    --持有产品数量
,LIMIT_AMT	    --授信额度
,CURR_USE_AMT	--当前可用额度
,SUM_APPLY_NUM	--累计授信申请次数
,SUM_CHECK_NUM	--累计授信核准次数
,SUM_REJECT_NUM	--累计授信拒绝次数
,SUM_DUE_NUM	--累计贷款次数
,SUM_DUE_AMT	--累计贷款金额
,ZDYE	        --在贷余额
,ZDBJ	        --在贷本金
,ZDLX	        --在贷利息
,ZDBS	        --在贷笔数
,ZDCPS	        --在贷产品数
,YQBS	        --逾期笔数
,YQBJ	        --逾期本金
,JQBS	        --结清笔数
,JQBJ	        --结清本金
,ZCZDBS	        --正常在贷笔数
,ZCZDBJ	        --正常在贷本金
,BEGNDT            --开始日期
,OVERDT            --结束日期
)  
SELECT
  v_acct_date , --统计日期
  B.CIF_NO ,    --互金客户号
  A.REL_CIF_NO ,--核心客户号
  COUNT(DISTINCT D.PRDT_NO) AS COUNT_PRDT ,--持有产品数量
  SUM(
    CASE
      WHEN D.LIMIT_AMT IS NOT NULL
      THEN D.LIMIT_AMT
      ELSE 0
  END) AS SUM_AMT, --授信额度 
  SUM(D.LIMIT_AMT) - SUM(E.ZDYEBJ) AS CURR_USE_AMT, --当前可用额度
  SUM(D.JJL)                AS SUM_APPLY_NUM,--累计授信申请次数
  SUM(D.CHECK_NUM)          AS SUM_CHECK_NUM,--累计授信核准次数
  SUM(D.REJECT_NUM)         AS SUM_REJECT_NUM, --累计授信拒绝次数
  COUNT(DISTINCT E.DUE_NO)  AS SUM_DUE_NUM,--累计贷款次数
  SUM(E.DUE_AMT)            AS SUM_DUE_AMT,--累计贷款金额
  SUM(E.ZDYE)               AS ZDYE,--在贷余额
  SUM(E.ZDYEBJ)             AS ZDBJ,--在贷本金
  SUM(E.ZDYELX)             AS ZDLX,--在贷利息
  SUM(
    CASE
      WHEN E.ZDYE >0
      THEN 1
      ELSE 0
    END) AS ZDBS,--在贷笔数
  COUNT(DISTINCT(
    CASE
      WHEN E.ZDYE >0
      THEN E.PRDT_NO
    END)) AS ZDCPS,--在贷产品数
  SUM(
    CASE
      WHEN E.YQYE >0
      THEN 1
      ELSE 0
    END)      AS YQBS,--逾期笔数
  SUM(E.YQBJ) AS YQBJ,--逾期本金
  CASE
    WHEN H.CIF_NO IS NOT NULL
    THEN H.JQBS
    ELSE 0
  END AS JQBS,--结清笔数
  CASE
    WHEN H.CIF_NO IS NOT NULL
    THEN H.JQBJ
    ELSE 0
  END AS JQBJ,--结清本金
  SUM(
    CASE
      WHEN E.ZDYE >0
      AND E.YQYE  = 0
      THEN 1
      ELSE 0
    END) AS ZCZDBS,--正常在贷笔数
  SUM(
    CASE
      WHEN E.ZDYE >0
      AND E.YQYE  = 0
      THEN E.ZDYEBJ
      ELSE 0
    END) AS ZCZDBJ, --正常在贷本金
  v_acct_date, --开始日期
  v_acct_date + 1 --结束日期
FROM
  OMI.IB_ECIF_CERT_INFO_HS B
  /*互金系统客户信息*/
LEFT JOIN OMI.IB_ECIF_CIF_NO_REL_HS A
ON
  B.CIF_NO   = A.CIF_NO
AND A.REL_SYS='CORE'
AND A.BEGNDT <= v_acct_date
AND A.OVERDT  > v_acct_date
AND A.PARTID = v_partid
LEFT JOIN DMM.RM_JINJZB_HS D
ON
  B.CIF_NO = D.CIF_NO
AND D.BEGNDT <= v_acct_date
AND D.OVERDT  > v_acct_date
--AND D.PARTID = v_partid
LEFT JOIN DMM.RM_JIEJZB_HS E
ON
  B.CIF_NO = E.CIF_NO
AND E.BEGNDT <= v_acct_date
AND E.OVERDT  > v_acct_date
--AND E.PARTID = v_partid
LEFT JOIN
  (
    SELECT
      F.CIF_NO,
      COUNT(DISTINCT F.DUE_NO) AS JQBS,
      SUM(G.Y_SUM_PRI)         AS JQBJ
    FROM
      DMM.RM_JIEJWD_HS F
    INNER JOIN DMM.RM_HKZB_HS G
    ON
      F.CIF_NO   = G.CIF_NO
    AND F.DUE_NO = G.DUE_NO
	AND G.BEGNDT <= v_acct_date
    AND G.OVERDT  > v_acct_date
    --AND G.PARTID = v_partid
    WHERE
	  F.BEGNDT <= v_acct_date
    AND F.OVERDT  > v_acct_date
    --AND F.PARTID = v_partid
    AND F.STATUS = '结清'
    GROUP BY
      F.CIF_NO
  )
  H ON B.CIF_NO = H.CIF_NO
WHERE 
  B.BEGNDT  <= v_acct_date
AND B.OVERDT > v_acct_date
AND B.PARTID = v_partid
AND B.CIF_NO IS NOT NULL
GROUP BY
  B.CIF_NO ,
  A.REL_CIF_NO,
  JQBS,
  JQBJ ;

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
