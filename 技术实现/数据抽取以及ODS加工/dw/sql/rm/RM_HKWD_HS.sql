!set plsqlUseSlash true	
CREATE OR REPLACE PROCEDURE   DMM.PRO_RM_HKWD_HS   (i_acct_date IN DATE) IS
	/************************************************************************
	过程中文名：还款维度表
	功能描述：  存放客户的还款维度信息
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
	v_object_name := 'DMM.PRO_RM_HKWD_HS';
	v_begin_time  := systimestamp;
	v_system_flag := 'DMM';
	v_partid      := to_char(v_acct_date, 'yyyyMM');
	--0.3   日志变量组的初始化
	l_trlg.pro_name    := 'DMM.PRO_RM_HKWD_HS';
	l_trlg.log_object  := 'DMM.RM_HKWD_HS';
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
	delete from DMM.RM_HKWD_HS where data_dt = v_acct_date;
	--1.2创建临时表
	EXECUTE IMMEDIATE ('drop table if exists DMM.RM_HKWD_HS_TEMP');

	EXECUTE IMMEDIATE ("CREATE TABLE DMM.RM_HKWD_HS_TEMP(
	BEGNDT DATE COMMENT '生效日期',
	OVERDT DATE COMMENT '失效日期' , 
	DATA_DT DATE COMMENT '统计日期',
	DUE_NO STRING NOT NULL COMMENT '借据号',
	STAGE_NO STRING COMMENT '期次',
	CONT_NO  STRING COMMENT '合同编号',
	CIF_NO   STRING COMMENT '互金客户号',
	PRDT_NO  STRING COMMENT '产品编号',
	BASE_ACCT_NO STRING COMMENT '核心账号',
	ACCT_SEQ_NO STRING COMMENT '核心账户序列号',
	CMISLOAN_NO STRING COMMENT '核心借据号',
	CLIENT_NO   STRING COMMENT '核心客户号',
	PROD_TYPE   STRING COMMENT '核心产品类型',
	DUE_DATE DATE COMMENT '应还日期',
	GRACE_PERIOD_DATE DATE COMMENT '宽限日期',
	PAY_FLAG STRING COMMENT '还款标志',
	PAY_DATE DATE COMMENT '实还日期',
	OVER_DAYS DECIMAL(17,2) COMMENT '逾期天数',
	OVER_MONTHS DECIMAL(17,2) COMMENT '逾期期数',
	HIS_MAX_OVERDAYS DECIMAL(17,2) COMMENT '历史最高逾期天数',
	HIS_MAX_OVERMONTHS DECIMAL(17,2) COMMENT '历史最高逾期期数'
	) 
	COMMENT '还款维度临时表'
	CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
	STORED AS ORC 
	TBLPROPERTIES ('transactional'='true')
	;")
	--1.3 数据处理
	--处理还款信息


	begin
	l_trlg.log_desc   := v_object_name || '-1.1将客户已出账的还款维度信息插入临时表';
	l_trlg.log_action := 'Insert';
	l_trlg.log_seq    := l_trlg.log_seq + 1;
	l_trlg.begin_time := systimestamp;

	INSERT INTO DMM.RM_HKWD_HS_TEMP(
	 DATA_DT	       --统计日期
	,DUE_NO	           --借据号
	,STAGE_NO	       --期次
	,CONT_NO	       --合同编号
	,CIF_NO	           --互金客户号
	,PRDT_NO	       --产品编号
	,BASE_ACCT_NO      --核心账号
	,ACCT_SEQ_NO       --核心账户序列号
	,CMISLOAN_NO       --核心借据号
	,CLIENT_NO	       --核心客户号
	,PROD_TYPE	       --核心产品类型
	,DUE_DATE	       --应还日期
	,GRACE_PERIOD_DATE --宽限日期
	,PAY_FLAG	       --还款标志
	,PAY_DATE	       --实还日期
	,OVER_DAYS	       --逾期天数
	,OVER_MONTHS	   --逾期期数
	,HIS_MAX_OVERDAYS  --历史最高逾期天数
	,HIS_MAX_OVERMONTHS--历史最高逾期期数
	,BEGNDT            --开始日期
	,OVERDT            --结束日期
	) 
	SELECT DISTINCT
	  A.DATA_DT , --统计日期
	  A.DUE_NO ,    --借据号
	  TO_NUMBER(C.STAGE_NO) , --期次
	  A.CONT_NO ,      --合同编号
	  A.CIF_NO ,       --互金客户号
	  A.PRDT_NO ,      --产品编号
	  A.BASE_ACCT_NO , --核心账号
	  A.ACCT_SEQ_NO ,  --核心账户序列号
	  A.CMISLOAN_NO ,  --核心借据号
	  A.CLIENT_NO ,    --核心客户号
	  A.PROD_TYPE ,    --核心产品类型
	  C.DUE_DATE ,  --应还日期
	  C.GRACE_PERIOD_DATE , --宽限日期
	  CASE
		WHEN C.FULLY_SETTLED = 'N'
		THEN '逾期'
		WHEN C.FULLY_SETTLED = 'Y'
		THEN '结清'
	  END AS PAY_FLAG, --还款标志
	  CASE
		WHEN C.FULLY_SETTLED = 'Y'
		THEN C.FINAL_SETTLE_DATE
	  END AS PAY_DATE, --实还日期
	  CASE
		WHEN C.INTERNAL_KEY IS NOT NULL
		THEN C.OVER_DAYS
		ELSE 0
	  END, --逾期天数
		  CASE
			WHEN C.OVER_DAYS <= 30
			THEN 1
			WHEN C.OVER_DAYS BETWEEN 31 AND 60
			THEN 2
			WHEN C.OVER_DAYS BETWEEN 61 AND 90
			THEN 3
			WHEN C.OVER_DAYS > 90
			THEN 4
	  END AS OVER_MONTHS, --逾期期数
	  CASE
		WHEN C.FULLY_SETTLED = 'N'
		THEN C.OVER_DAYS
		WHEN C.FULLY_SETTLED = 'Y'
		THEN C.MAX_OVER_DAYS
	  END AS HIS_MAX_OVERDAYS, --历史最高逾期天数
	  CASE
		WHEN C.FULLY_SETTLED='N'
		THEN
		  CASE
			WHEN C.OVER_DAYS <= 30
			THEN 1
			WHEN C.OVER_DAYS BETWEEN 31 AND 60
			THEN 2
			WHEN C.OVER_DAYS BETWEEN 61 AND 90
			THEN 3
			WHEN C.OVER_DAYS > 90
			THEN 4
		  END
		WHEN C.FULLY_SETTLED='Y'
		THEN
		  CASE
			WHEN C.MAX_OVER_DAYS <= 30
			THEN 1
			WHEN C.MAX_OVER_DAYS BETWEEN 31 AND 60
			THEN 2
			WHEN C.MAX_OVER_DAYS BETWEEN 61 AND 90
			THEN 3
			WHEN C.MAX_OVER_DAYS > 90
			THEN 4
		  END
	  END AS HIS_MAX_OVERMONTHS, --历史最高逾期期数
	  A.BEGNDT, --开始日期
	  A.OVERDT --结束日期
	FROM
	  DMM.RM_JIEJHK_PUBLIC A
	  /*借据还款公共表*/
	LEFT JOIN
	(
		SELECT
		  C.INTERNAL_KEY,
		  C.FULLY_SETTLED,
		  C.STAGE_NO,
		  C.DUE_DATE,
		  C.GRACE_PERIOD_DATE,
		  C.FINAL_SETTLE_DATE,
		  C.OVER_DAYS,
		  C.MAX_OVER_DAYS,
		  ROW_NUMBER() OVER (PARTITION BY C.INTERNAL_KEY,C.STAGE_NO ORDER BY DECODE(C.AMT_TYPE,'PRI',2,'INT',1,0) DESC,C.DUE_DATE DESC, c.GRACE_PERIOD_DATE DESC) AS RN_NUM
		FROM
		  (
			SELECT
			  C.INTERNAL_KEY,
			  C.FULLY_SETTLED,
			  C.STAGE_NO,
			  C.DUE_DATE,
			  C.GRACE_PERIOD_DATE,
			  C.FINAL_SETTLE_DATE,
			  C.AMT_TYPE,
			  CASE
				WHEN C.FULLY_SETTLED = 'N'
				THEN DATEDIFF(v_acct_date,DATE(C.DUE_DATE))
				ELSE 0
			  END AS OVER_DAYS,
			  CASE
				WHEN C.FULLY_SETTLED = 'Y'
				THEN DATEDIFF(DATE(C.FINAL_SETTLE_DATE),DATE(C.DUE_DATE))
				ELSE 0
			  END AS MAX_OVER_DAYS
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
	AND C.RN_NUM = 1
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
	l_trlg.log_desc   := v_object_name || '-1.2将客户未出账的还款维度信息插入临时表';
	l_trlg.log_action := 'Insert';
	l_trlg.log_seq    := l_trlg.log_seq + 1;
	l_trlg.begin_time := systimestamp;

	INSERT INTO DMM.RM_HKWD_HS_TEMP(
	 DATA_DT	       --统计日期
	,DUE_NO	           --借据号
	,STAGE_NO	       --期次
	,CONT_NO	       --合同编号
	,CIF_NO	           --互金客户号
	,PRDT_NO	       --产品编号
	,BASE_ACCT_NO      --核心账号
	,ACCT_SEQ_NO       --核心账户序列号
	,CMISLOAN_NO       --核心借据号
	,CLIENT_NO	       --核心客户号
	,PROD_TYPE	       --核心产品类型
	,DUE_DATE	       --应还日期
	,GRACE_PERIOD_DATE --宽限日期
	,PAY_FLAG	       --还款标志
	,PAY_DATE	       --实还日期
	,OVER_DAYS	       --逾期天数
	,OVER_MONTHS	   --逾期期数
	,HIS_MAX_OVERDAYS  --历史最高逾期天数
	,HIS_MAX_OVERMONTHS--历史最高逾期期数
	,BEGNDT            --开始日期
	,OVERDT            --结束日期
	)  
	SELECT DISTINCT
	  A.DATA_DT , --统计日期
	  A.DUE_NO , --借据号
	  CASE
		WHEN C.MAX_STAGE_NO IS NOT NULL
		AND D.STAGE_NO  IS NOT NULL
		THEN TO_NUMBER(C.MAX_STAGE_NO + D.STAGE_NO)
		WHEN C.MAX_STAGE_NO IS NULL
		AND D.STAGE_NO IS NOT NULL
		THEN TO_NUMBER(D.STAGE_NO)
	  END AS STAGE_NO, --期次
	  A.CONT_NO , --合同编号
	  A.CIF_NO ,  --互金客户号
	  A.PRDT_NO , --产品编号
	  A.BASE_ACCT_NO ,--核心账号
	  A.ACCT_SEQ_NO , --核心账户序列号
	  A.CMISLOAN_NO , --核心借据号
	  A.CLIENT_NO ,   --核心客户号
	  A.PROD_TYPE ,   --核心客户号
	  D.END_DATE ,    --应还日期  
	  NULL ,    --宽限日期
	  '未到期' ,--还款标志
	  NULL ,    --实还日期
	  0 AS OVER_DAYS , --逾期天数
	  0 AS OVER_MONTHS,--逾期期数
	  0 AS MAX_OVER_DAYS ,--历史最高逾期天数
	  0 AS MAX_OVER_MONTHS ,--历史最高逾期期数
	  A.BEGNDT, --开始日期
	  A.OVERDT --结束日期
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
		  D.END_DATE,
		  ROW_NUMBER() OVER (PARTITION BY D.INTERNAL_KEY,D.STAGE_NO ORDER BY D.END_DATE DESC) AS RN_NUM
		FROM
		  OMI.CB_MB_ACCT_SCHEDULE_DETAIL_HS D
		WHERE
		  D.BEGNDT <= v_acct_date
		  AND D.OVERDT  > v_acct_date
		  AND D.PARTID = v_partid
	  )
	  D
	  /*账户计划明细表*/
	ON
	  A.INTERNAL_KEY = D.INTERNAL_KEY
	  /*账户标识符*/
	AND D.RN_NUM = 1
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
	l_trlg.log_desc   := v_object_name || '-1.3去除重复数据，将结果数据插入目标表';
	l_trlg.log_action := 'Insert';
	l_trlg.log_seq    := l_trlg.log_seq + 1;
	l_trlg.begin_time := systimestamp;


	INSERT INTO DMM.RM_HKWD_HS(
	 DATA_DT	       --统计日期
	,DUE_NO	           --借据号
	,STAGE_NO	       --期次
	,CONT_NO	       --合同编号
	,CIF_NO	           --互金客户号
	,PRDT_NO	       --产品编号
	,BASE_ACCT_NO      --核心账号
	,ACCT_SEQ_NO       --核心账户序列号
	,CMISLOAN_NO       --核心借据号
	,CLIENT_NO	       --核心客户号
	,PROD_TYPE	       --核心产品类型
	,DUE_DATE	       --应还日期
	,GRACE_PERIOD_DATE --宽限日期
	,PAY_FLAG	       --还款标志
	,PAY_DATE	       --实还日期
	,OVER_DAYS	       --逾期天数
	,OVER_MONTHS	   --逾期期数
	,HIS_MAX_OVERDAYS  --历史最高逾期天数
	,HIS_MAX_OVERMONTHS--历史最高逾期期数
	,BEGNDT            --开始日期
	,OVERDT            --结束日期
	)  
	SELECT
	 A.DATA_DT	         --统计日期
	,A.DUE_NO	         --借据号
	,A.STAGE_NO	         --期次
	,A.CONT_NO	         --合同编号
	,A.CIF_NO	         --互金客户号
	,A.PRDT_NO	         --产品编号
	,A.BASE_ACCT_NO      --核心账号
	,A.ACCT_SEQ_NO       --核心账户序列号
	,A.CMISLOAN_NO       --核心借据号
	,A.CLIENT_NO	     --核心客户号
	,A.PROD_TYPE	     --核心产品类型
	,A.DUE_DATE	         --应还日期
	,A.GRACE_PERIOD_DATE --宽限日期
	,A.PAY_FLAG	         --还款标志
	,A.PAY_DATE	         --实还日期
	,A.OVER_DAYS	     --逾期天数
	,A.OVER_MONTHS	     --逾期期数
	,A.HIS_MAX_OVERDAYS  --历史最高逾期天数
	,A.HIS_MAX_OVERMONTHS--历史最高逾期期数
	,A.BEGNDT            --开始日期
	,A.OVERDT            --结束日期
	FROM
	  (
		SELECT
		   DATA_DT
		  ,DUE_NO
		  ,STAGE_NO
		  ,CONT_NO
		  ,CIF_NO
		  ,PRDT_NO
		  ,BASE_ACCT_NO
		  ,ACCT_SEQ_NO
		  ,CMISLOAN_NO
		  ,CLIENT_NO
		  ,PROD_TYPE
		  ,DUE_DATE
		  ,GRACE_PERIOD_DATE
		  ,PAY_FLAG
		  ,PAY_DATE
		  ,OVER_DAYS
		  ,OVER_MONTHS
		  ,HIS_MAX_OVERDAYS
		  ,HIS_MAX_OVERMONTHS
		  ,BEGNDT
		  ,OVERDT
		  ,ROW_NUMBER() OVER (PARTITION BY CIF_NO,DUE_NO,STAGE_NO) AS RN
		FROM
		  DMM.RM_HKWD_HS_TEMP
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
