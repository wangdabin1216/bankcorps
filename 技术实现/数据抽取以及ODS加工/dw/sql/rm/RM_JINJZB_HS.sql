!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_JINJZB_HS   (i_acct_date IN DATE) IS
/************************************************************************
过程中文名：进件指标表
功能描述：  存放客户的借据申请信息
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
v_object_name := 'DMM.PRO_RM_JINJZB_HS';
v_begin_time  := systimestamp;
v_system_flag := 'DMM';
v_partid      := to_char(v_acct_date, 'yyyyMM');
--0.3   日志变量组的初始化
l_trlg.pro_name    := 'DMM.PRO_RM_JINJZB_HS';
l_trlg.log_object  := 'DMM.RM_JINJZB_HS';
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
delete from DMM.RM_JINJZB_HS where data_dt = v_acct_date;
--1.2创建临时表
--1.3 数据处理
--处理客户进件信息


begin
l_trlg.log_desc   := v_object_name || '-1.1将客户的进件信息插入目标表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_JINJZB_HS(
 DATA_DT    --统计日期
,APPLY_DATE --申请日期
,CIF_NO     --客户号
,APPLY_NO   --申请件编号
,PRDT_NO    --产品编号
,JJL        --进件量
,CHECK_NUM  --核准件量
,REJECT_NUM --拒绝件量
,CANCEL_NUM --取消件量
,D_COUNT    --待审件量
,APPR_LIMIT --申请额度
,LIMIT_AMT  --授信额度
,BEGNDT     --开始日期
,OVERDT     --结束日期
)  
SELECT 
  A.DATA_DT,    --统计日期
  A.APPLY_DATE, --申请日期
  A.CIF_NO,     --客户号
  A.APPLY_NO,   --申请件编号
  A.PRDT_NO,    --产品编号
  '1' ,         --进件量
  CASE
    WHEN A.APPR_RESULT='0'
    THEN 1
    ELSE 0
  END ,         --核准件量
  CASE
    WHEN A.APPR_RESULT='1'
    THEN 1
    ELSE 0
  END ,         --拒绝件量
  '0' ,         --取消件量
  CASE
    WHEN A.APPR_RESULT IN('2','9')
    THEN 1
    ELSE 0
  END ,         --待审件量
  A.APPR_LIMIT , --申请额度
  CASE
    WHEN B.CERT_NO IS NOT NULL
    THEN B.LIMIT_AMT
    ELSE 0
  END ,      --授信额度
  A.BEGNDT , --开始日期
  A.OVERDT   --结束日期
FROM
  DMM.RM_JINJ_PUBLIC A
  /*进件公共表*/
LEFT JOIN OMI.IB_LN_CIF_LIMIT_HS B
  /*客户额度表*/
ON
  A.CERT_TYPE = B.CERT_TYPE
  /*证件类型*/
AND A.CERT_NO = B.CERT_NO
  /*证件号码*/
AND A.PRDT_NO = B.PRDT_NO
  /*产品编码*/
AND B.BEGNDT <= v_acct_date
AND B.OVERDT  > v_acct_date
AND B.PARTID = v_partid
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
