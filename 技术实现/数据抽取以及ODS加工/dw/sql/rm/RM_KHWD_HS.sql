!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_KHWD_HS   (i_acct_date IN DATE) IS
/************************************************************************
过程中文名：客户维度表
功能描述：  存放客户维度信息
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
v_object_name := 'DMM.PRO_RM_KHWD_HS';
v_begin_time  := systimestamp;
v_system_flag := 'DMM';
v_partid      := to_char(v_acct_date, 'yyyyMM');
--0.3   日志变量组的初始化
l_trlg.pro_name    := 'DMM.PRO_RM_KHWD_HS';
l_trlg.log_object  := 'DMM.RM_KHWD_HS';
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
l_trlg.log_desc   := v_object_name || '-1.1将客户维度信息插入目标表';
l_trlg.log_action := 'Insert';
l_trlg.log_seq    := l_trlg.log_seq + 1;
l_trlg.begin_time := systimestamp;

INSERT INTO DMM.RM_KHWD_HS(
 DATA_DT	        --统计日期
,CLIENT_NO	        --核心客户号
,CIF_NO	            --互金客户号
,CERT_NO	        --证件号
,CLIENT_TYPE	    --客户类型
,CATEGORY_TYPE	    --客户细分类型
,CH_CLIENT_NAME	    --客户姓名
,ADDRESS	        --地址
,INTERNAL_IND	    --是否为内部客户
,TRAN_STATUS	    --客户交易状态
,CLIENT_INDICATOR   --客户标识
,SOURCE_TYPE	    --渠道
,TEMP_CLIENT	    --是否为临时客户
,RESIDENT_STATUS    --居住状态
,RACE	            --种族
,BIRTH_DATE	        --出生日期
,SEX	            --性别
,MARITAL_STATUS	    --婚姻状况
,OCCUPATION_CODE    --职业
,RESIDENT	        --居住类型
,QUALIFICATION	    --职称
,EDUCATION	        --学历
,MON_SALARY	        --月收入
,POST	            --职务
,MAX_DEGREE	        --最高学位
,YEARLY_INCOME	    --年收入
,EMPLOYER_INDUSTRY  --行业
,FIRST_APPLY_DT	    --首次申请日期
,FIRST_SX_DT	    --首次授信日期
,FIRST_FK_DT	    --首次放款日期
,FIRST_OVER_DT	    --首次逾期日期
,FIRST_APPLY_PRD_NO	--首次申请产品编号
,FIRST_SX_PRD_NO	--首次授信产品编号
,FIRST_FK_PRD_NO	--首次放款产品编号
,FIRST_OVER_PRD_NO	--首次逾期产品编号
,HIS_MAX_SXED	    --历史最高授信额度
,HIS_MAX_JKJE	    --历史最高借款金额
,MAX_OVER_DAYS	    --当前最高逾期天数
,HIS_MAX_OVER_DAYS	--历史最高逾期天数
,LATE_OVER_MONTHS	--最近一次逾期月份
,BEGNDT            --开始日期
,OVERDT            --结束日期
)  
SELECT
  v_acct_date	        --统计日期
,T.REL_CIF_NO	        --核心客户号
,B.CIF_NO	            --互金客户号
,B.CERT_NO	            --证件号
,A.CLIENT_TYPE	        --客户类型
,A.CATEGORY_TYPE	    --客户细分类型
,A.CH_CLIENT_NAME	    --客户姓名
,A.ADDRESS	            --地址
,A.INTERNAL_IND	        --是否为内部客户
,A.TRAN_STATUS	        --客户交易状态
,A.CLIENT_INDICATOR     --客户标识
,A.SOURCE_TYPE	        --渠道
,A.TEMP_CLIENT	        --是否为临时客户
,C.RESIDENT_STATUS      --居住状态
,C.RACE	                --种族
,C.BIRTH_DATE	        --出生日期
,C.SEX	                --性别
,C.MARITAL_STATUS	    --婚姻状况
,C.OCCUPATION_CODE      --职业
,C.RESIDENT	            --居住类型
,C.QUALIFICATION	    --职称
,C.EDUCATION	        --学历
,C.MON_SALARY	        --月收入
,C.POST	                --职务
,C.MAX_DEGREE	        --最高学位
,C.YEARLY_INCOME	    --年收入
,C.EMPLOYER_INDUSTRY    --行业
,D.FIRST_APPLY_DT	    --首次申请日期
,D.FIRST_SX_DT	        --首次授信日期
,E.FIRST_FK_DT	        --首次放款日期
,E.FIRST_OVER_DT	    --首次逾期日期
,D.FIRST_APPLY_PRD_NO	--首次申请产品编号
,D.FIRST_SX_PRD_NO	    --首次授信产品编号
,E.FIRST_FK_PRD_NO	    --首次放款产品编号
,E.FIRST_OVER_PRD_NO	--首次逾期产品编号
,D.HIS_MAX_SXED	        --历史最高授信额度
,F.HIS_MAX_JKJE	        --历史最高借款金额
,E.MAX_OVER_DAYS	    --当前最高逾期天数
,E.HIS_MAX_OVER_DAYS	--历史最高逾期天数
,E.LATE_OVER_MONTHS	    --最近一次逾期月份
,v_acct_date            --开始日期
,v_acct_date + 1        --结束日期
FROM
 OMI.IB_ECIF_CERT_INFO_HS B
  /*互金系统客户信息*/
LEFT JOIN OMI.IB_ECIF_CIF_NO_REL_HS T
ON
  B.CIF_NO   = T.CIF_NO
AND T.REL_SYS='CORE'
AND T.BEGNDT <= v_acct_date
AND T.OVERDT  > v_acct_date
AND T.PARTID = v_partid
LEFT JOIN OMI.CB_CIF_CLIENT_HS A
  /*核心系统客户信息表*/
ON
  T.REL_CIF_NO         =A.CLIENT_NO
AND A.BEGNDT <= v_acct_date
AND A.OVERDT  > v_acct_date
AND A.PARTID = v_partid
LEFT JOIN OMI.CB_CIF_CLIENT_INDVL_HS C
  /*核心个人客户信息表*/
ON
  A.CLIENT_NO         =C.CLIENT_NO
AND C.BEGNDT <= v_acct_date
AND C.OVERDT  > v_acct_date
AND C.PARTID = v_partid
LEFT JOIN
(
  SELECT DISTINCT
    D.CIF_NO,
    D.FIRST_APPLY_DT,
    D.FIRST_SX_DT,
    D.HIS_MAX_SXED,
    D.FIRST_APPLY_PRD_NO,
    D.FIRST_SX_PRD_NO
  FROM
    (
      SELECT
        D.CIF_NO,
        D.FIRST_APPLY_DT,
        D.FIRST_SX_DT,
        D.HIS_MAX_SXED,
        MAX(D.FIRST_APPLY_PRD_NO) OVER (PARTITION BY D.CIF_NO) AS FIRST_APPLY_PRD_NO,
        MAX(D.FIRST_SX_PRD_NO) OVER (PARTITION BY D.CIF_NO) AS FIRST_SX_PRD_NO
      FROM
        (
          SELECT
            T.CIF_NO,
            T.FIRST_APPLY_DT,
            T.FIRST_SX_DT,
            T.HIS_MAX_SXED,
            CASE
              WHEN T.APPLY_DATE = T.FIRST_APPLY_DT
              THEN T.PRDT_NO
            END AS FIRST_APPLY_PRD_NO,
            CASE
              WHEN T.APPLY_DATE = T.FIRST_SX_DT
              THEN T.PRDT_NO
            END AS FIRST_SX_PRD_NO
          FROM
            (
              SELECT
                D.CIF_NO,
                D.APPLY_DATE,
                D.PRDT_NO,
                MIN(D.APPLY_DATE) OVER (PARTITION BY D.CIF_NO) AS FIRST_APPLY_DT,
                MIN(
                  CASE
                    WHEN D.CHECK_NUM = 1
                    THEN D.APPLY_DATE
                  END) OVER (PARTITION BY D.CIF_NO) AS FIRST_SX_DT,
                MAX(
                  CASE
                    WHEN D.CHECK_NUM = 1
                    THEN D.LIMIT_AMT
                  END) OVER (PARTITION BY D.CIF_NO) AS HIS_MAX_SXED
              FROM
                DMM.RM_JINJZB_HS D
              WHERE
                D.DATA_DT = v_acct_date
            )
            T
        )
        D
    )
    D
)
D ON B.CIF_NO = D.CIF_NO
LEFT JOIN
(
  SELECT DISTINCT
    E.CIF_NO,
    E.FIRST_FK_DT,
    E.FIRST_OVER_DT,
    E.MAX_OVER_DAYS,
    E.HIS_MAX_OVER_DAYS,
    E.LATE_OVER_MONTHS,
    E.FIRST_FK_PRD_NO,
    E.FIRST_OVER_PRD_NO
  FROM
    (
      SELECT
        E.CIF_NO,
        E.FIRST_FK_DT,
        E.FIRST_OVER_DT,
        E.MAX_OVER_DAYS,
        E.HIS_MAX_OVER_DAYS,
        E.LATE_OVER_MONTHS,
        MAX(E.FIRST_FK_PRD_NO) OVER (PARTITION BY E.CIF_NO)   AS FIRST_FK_PRD_NO,
        MAX(E.FIRST_OVER_PRD_NO) OVER (PARTITION BY E.CIF_NO) AS FIRST_OVER_PRD_NO
      FROM
        (
          SELECT
            T.CIF_NO,
            T.FIRST_FK_DT,
            T.FIRST_OVER_DT,
            T.MAX_OVER_DAYS,
            T.HIS_MAX_OVER_DAYS,
            SUBSTR(TO_CHAR(T.LATE_OVER_MONTHS,'YYYYMMDD'),5,2) AS LATE_OVER_MONTHS,
            CASE
              WHEN T.BEG_DATE = T.FIRST_FK_DT
              THEN T.PRDT_NO
            END AS FIRST_FK_PRD_NO,
            CASE
              WHEN T.DATA_DT = T.FIRST_OVER_DT
              THEN T.PRDT_NO
            END AS FIRST_OVER_PRD_NO
          FROM
            (
              SELECT
                E.CIF_NO,
                E.PRDT_NO,
                E.BEG_DATE,
                E.DATA_DT,
                MIN(E.BEG_DATE) OVER (PARTITION BY E.CIF_NO) AS FIRST_FK_DT,
                MIN(
                  CASE
                    WHEN E.STATUS = '逾期'
                    THEN E.DATA_DT
                  END) OVER (PARTITION BY E.CIF_NO)               AS FIRST_OVER_DT,
                MAX(E.CURR_OVERDAYS) OVER (PARTITION BY E.CIF_NO) AS MAX_OVER_DAYS,
                MAX(E.HIS_MAX_PAYDAYS) OVER (PARTITION BY E.CIF_NO) AS HIS_MAX_OVER_DAYS,
                MAX(
                  CASE
                    WHEN E.CURR_OVERDAYS>0
                    THEN E.DATA_DT
                  END ) OVER (PARTITION BY E.CIF_NO) AS LATE_OVER_MONTHS
              FROM
                DMM.RM_JIEJWD_HS E
              WHERE
                E.DATA_DT = v_acct_date
            )
            T
        )
        E
    )
    E
)
E ON B.CIF_NO = E.CIF_NO 
LEFT JOIN
(
  SELECT DISTINCT
    F.CIF_NO,
    F.HIS_MAX_JKJE
  FROM
    (
      SELECT
        CIF_NO,
        MAX(DUE_AMT) OVER (PARTITION BY CIF_NO) AS HIS_MAX_JKJE
      FROM
        DMM.RM_JIEJZB_HS
      WHERE
        DATA_DT = v_acct_date
    )
    F
)
F ON B.CIF_NO = F.CIF_NO
WHERE  B.BEGNDT <= v_acct_date
AND B.OVERDT  > v_acct_date
AND B.PARTID = v_partid
AND B.CIF_NO IS NOT NULL;

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
