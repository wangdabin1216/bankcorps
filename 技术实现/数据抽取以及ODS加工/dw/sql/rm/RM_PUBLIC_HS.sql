!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_PUBLIC_HS   (i_acct_date IN DATE) IS
/************************************************************************
过程中文名：公共部分加工
功能描述：  进件表、借据表、还款表 等公共部分加工
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
v_object_name := 'DMM.PRO_RM_PUBLIC_HS';
v_begin_time  := systimestamp;
v_system_flag := 'DMM';
v_partid      := to_char(v_acct_date, 'yyyyMM');
--0.3   日志变量组的初始化
l_trlg.pro_name    := 'DMM.PRO_RM_PUBLIC_HS';
l_trlg.log_object  := 'DMM.RM_JINJ_PUBLIC';
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
delete from DMM.RM_JINJ_PUBLIC where data_dt = v_acct_date;
delete from DMM.RM_JIEJHK_PUBLIC where data_dt = v_acct_date;
--1.2创建临时表
--1.3 数据处理
--处理客户进件信息


begin
l_trlg.log_desc   := v_object_name || '-1.1将客户进件指标和进件维度公共部分插入目标表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_JINJ_PUBLIC(
 DATA_DT    --统计日期
,APPLY_DATE --申请日期
,CIF_NO     --客户号
,APPLY_NO   --申请件编号
,PRDT_NO    --产品编号
,APPR_RESULT--审批结果
,APPR_LIMIT --申请额度
,CERT_TYPE  --证件类型
,CERT_NO    --证件号码
,PTNER_ID   --合作方
,TX_DATE    --进件日期
,APP_DATE   --审批日期
,BEGNDT     --开始日期
,OVERDT     --结束日期
)  
SELECT
  V_ACCT_DATE ,  --统计日期
  A.TX_DATE ,    --申请日期
  B.CIF_NO ,     --客户号
  A.CREDIT_APPLY_NO ,--申请件编号
  A.PRDT_NO ,    --产品编号
  A.APPR_RESULT, --审批结果 
  A.APPR_LIMIT , --申请额度
  A.CERT_TYPE,   --证件类型
  A.CERT_NO,     --证件号码
  A.PTNER_ID ,   --合作方
  A.TX_DATE ,    --进件日期
  A.APP_DATE,    --审批日期
  V_ACCT_DATE ,  --开始日期
  V_ACCT_DATE + 1 --结束日期
FROM
  OMI.IB_LN_CREDIT_APPLY_MST_HS A
  /*授信申请主表*/
LEFT JOIN OMI.IB_ECIF_CERT_INFO_HS B
ON
  A.CERT_TYPE = B.CERT_TYPE
  /*证件类型*/
AND A.CERT_NO = B.CERT_NO
  /*证件号码*/
AND B.BEGNDT <= v_acct_date
AND B.OVERDT  > v_acct_date
AND B.PARTID = v_partid
WHERE
  A.BEGNDT  <= v_acct_date
AND A.OVERDT > v_acct_date
AND A.PARTID = v_partid;

l_trlg.row_count := SQL%ROWCOUNT;
cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
EXCEPTION
WHEN OTHERS THEN
l_trlg.log_code := SQLCODE();
l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
RAISE error_exception;
end;



begin
l_trlg.log_desc   := v_object_name || '-1.2将客户借据指标、借据维度、还款指标、还款维度公共部分插入目标表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_JIEJHK_PUBLIC(
 DATA_DT          --统计日期
,CIF_NO	          --互金客户号
,DUE_NO	          --借据号
,PRDT_NO          --产品编号
,CONT_NO	      --合同编号
,DUE_AMT          --贷款金额
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
,LN_RATE	      --执行利率
,OVER_RATE	      --逾期利率
,INTERNAL_KEY     --账户标志符
,BEGNDT           --开始日期
,OVERDT           --结束日期
)  
SELECT
  v_acct_date ,     --统计日期
  A.CIF_NO ,        --互金客户号
  A.DUE_NO ,        --借据号
  A.PRDT_NO ,       --产品编号
  A.CONT_NO ,       --合同编号
  A.DUE_AMT ,       --贷款金额
  B.BASE_ACCT_NO ,  --核心账号
  B.ACCT_SEQ_NO ,   --核心账户序列号
  B.CMISLOAN_NO ,   --核心借据号
  B.CLIENT_NO ,     --核心客户号
  B.PROD_TYPE ,     --核心产品类型
  B.REVERSAL ,      --冲正标识
  C.REPAY_TYPE ,    --还款方式
  C.REPAY_PERIOD ,  --还款周期
  C.REPAY_DAY_TYP , --还款日方式
  C.GRACE_DAYS ,    --宽限期天数
  C.LN_USE ,        --贷款用途
  C.REPAY_SOURCE ,  --还款来源
  A.BEG_DATE ,      --发放日期
  A.END_DATE ,      --到期日期
  A.LN_RATE ,      --执行利率
  A.OVER_RATE ,    --逾期利率
  D.INTERNAL_KEY,  --账户标志符
  v_acct_date ,    --开始日期
  v_acct_date + 1  --结束日期
FROM
  OMI.IB_LN_DUE_MST_HS A
  /*贷款借据主表*/
LEFT JOIN OMI.CB_MB_DRAWDOWN_HS B
  /*贷款发放表*/
ON
  A.ACCT_NO= B.BASE_ACCT_NO
  /*核心贷款账号*/
AND A.GRANT_NO = B.ACCT_SEQ_NO
  /*核心贷款发放序号*/
AND B.BEGNDT <= v_acct_date
AND B.OVERDT  > v_acct_date
AND B.PARTID = v_partid
AND B.REVERSAL <> 'Y'
  /*冲正标志 Y:已冲正*/
LEFT JOIN OMI.IB_LN_CONT_MST_HS C
  /*贷款合同主表*/
ON
  A.CONT_NO  = C.CONT_NO
AND C.BEGNDT <= v_acct_date
AND C.OVERDT  > v_acct_date
AND C.PARTID = v_partid
LEFT JOIN OMI.CB_MB_ACCT_HS D
  /*账户基本信息表*/
ON
  A.ACCT_NO= D.BASE_ACCT_NO
  /*核心贷款账号*/
AND A.GRANT_NO = D.ACCT_SEQ_NO
  /*核心贷款发放序号*/
AND D.BEGNDT <= v_acct_date
AND D.OVERDT  > v_acct_date
AND D.PARTID = v_partid  
WHERE
     A.BEGNDT <= v_acct_date
 AND A.OVERDT  > v_acct_date
 AND A.PARTID = v_partid;

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
