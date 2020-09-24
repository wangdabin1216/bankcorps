!set plsqlUseSlash true

CREATE OR REPLACE PROCEDURE omi.pro_orc_sys_log(
i_id IN INT
,str_procname IN STRING
,str_etl_date IN STRING
,str_msg IN STRING
) IS
/**************************************************************************************************
过程中文名：日志记录存储过程
功能描述：记录日志
参数：
	i_id 日志级别：1-log 2-debug 3-error
	str_procname 存储过程名称
	str_etl_date etl日期
	str_msg 日志信息
编写人： dingkai
编写日期：2019-10-14
**************************************************************************************************/
	  str_level STRING;
	  p_procname STRING;
	  p_msg STRING;
	  p_etl_date STRING;
	  v_sql STRING;
BEGIN
	IF i_id =1 THEN
		str_level := "log";
	ELSEIF i_id =2 THEN
		str_level := "debug";
	ELSEIF i_id =3 THEN
		str_level := "error";
	ELSE
		str_level  := 'error';
    	p_procname := 'p_public_writelog';
    	p_msg      := 'writelog_error';
    END IF;
    p_etl_date := str_etl_date;
	p_procname := str_procname;
	p_msg      := str_msg;
	INSERT INTO omi.orc_sys_log
    (s_time, etl_date, s_level, s_procname, s_msg)
  	VALUES
    (to_char(SYSDATE),p_etl_date,str_level,p_procname, p_msg);
END;
/

CREATE OR REPLACE FUNCTION omi.fun_get_date(p_acct_date IN DATE DEFAULT SYSDATE,
                        p_date_type IN STRING) RETURN DATE IS 
    /*------------------------------------------------------------------------------------
    --函数中文名：特定日期获取函数
    --功能描述：获取特定方式的日期
    --说明：  
    --     'Y'为年底日期， 'H'为半年日期, 'Q'为季末日期,
    --     'M'为月末日期,  'T'为旬末日期, 'D'为当前日期,
    --     'MB'为月初日期, 'LMB'为上月初, 'QB'为季初日期,
    --     'LQB'为上季初日期,'YB'为年初日期,'LYB'为上年初日期,
    --     'LM'为上月月末,'LY'为上年底日期, 'LQ'为上季末日期,
    --     'LH'为前半年末日期,'LT'为上旬末日期,'LD'为上日日期,
    --     'ND'为下日日期,'NM'为下月今日日期,'NY'为明年今日日期，
    --     'LYT'为滚动年开始累计日期，'LQT'为滚动季度开始累计日期
    --     'LMT'为滚动月开始累计日期，'LYD'为去年同期日期，
    --     'LQD'为上季度同期日期，'LMD'为上月同期日期
    --          'HYB'为半年年初
    --编写人：   
    --编写日期： 
    --修改记录： 
    -----------------------------------------------------------------------------------*/
    v_end_date DATE;
    v_year     STRING;
    v_month    STRING;
    v_day      STRING;
    v_pmod     INT;
  BEGIN
    --取输入日期的年月日信息
    v_year  := to_char(p_acct_date, 'yyyy');
    v_month := to_char(p_acct_date, 'MM');
    v_day   := to_char(p_acct_date, 'dd');
    IF p_date_type = 'Y' THEN
      --年底日期
      v_end_date := to_date(v_year || '-12-31');
    ELSIF p_date_type = 'H' THEN
      --半年日期
      IF INT(pmod(INT(v_month), 6)) = 0 THEN
        v_pmod := 6;
      ELSE
        v_pmod := INT(pmod(INT(v_month), 6));
      END IF;
      v_end_date := to_date(last_day(add_months(p_acct_date, 6 - v_pmod)));
    
    ELSIF p_date_type = 'Q' THEN
      --季末
      IF INT(pmod(INT(v_month), 3)) = 0 THEN
        v_pmod := 3;
      ELSE
        v_pmod := INT(pmod(INT(v_month), 3));
      END IF;
      v_end_date := to_date(last_day(add_months(p_acct_date, 3 - v_pmod)) );
    ELSIF p_date_type = 'M' THEN
      --月末
      v_end_date := to_date(last_day(p_acct_date) );
    ELSIF p_date_type = 'T' THEN
      --旬末
      IF v_day <= '10' THEN
        v_end_date := to_date(v_year || '-' || v_month || '-10'); --输入为上旬
      ELSIF v_day <= '20' THEN
        v_end_date := to_date(v_year || '-' || v_month || '-20'); --输入为中旬
      ELSE
        v_end_date := to_date(last_day(p_acct_date) ); --输入为下旬
      END IF;
    
    ELSIF p_date_type = 'D' THEN
      --日
      v_end_date := to_date(p_acct_date);
    
    ELSIF p_date_type = 'MB' THEN
      --月初
      v_end_date := to_date(add_months(CAST(last_day(p_acct_date) AS date) + 1, -1));
    
    ELSIF p_date_type = 'LMB' THEN
      --上月初
      v_end_date := to_date(add_months(CAST(last_day(p_acct_date) AS date ) + 1, -2));
    
    ELSIF p_date_type = 'QB' THEN
      --季初
      IF INT(pmod(v_month, 3)) = 0 THEN
        v_end_date := to_date(add_months(last_day(p_acct_date) + 1, -3));
      ELSE
      
        v_end_date := to_date(add_months(last_day(p_acct_date) + 1,
                                 -int(pmod(v_month, 3))));
      END IF;
    
    ELSIF p_date_type = 'LQB' THEN
      --上季初
      IF INT(pmod(v_month, 3)) = 0 THEN
        v_end_date := to_date(add_months(last_day(p_acct_date) + 1, -6));
      ELSE
        v_end_date := to_date(add_months(last_day(p_acct_date) + 1,
                                 -int(pmod(v_month, 3)) - 3));
      END IF;
    
    ELSIF p_date_type = 'YB' THEN
      --年初
      v_end_date := to_date(v_year || '-01-01' );
    ELSIF p_date_type = 'HYB' THEN
      --半年年初
          IF INT(v_month) > 6 THEN
                v_end_date := to_date(v_year || '-07-01' );
          ELSE
                v_end_date := to_date(v_year || '-01-01' );
          END IF;
          
    ELSIF p_date_type = 'LYB' THEN
      --上年初
      v_end_date := to_date(to_char(INT(v_year) - 1) || '-01-01' );
    
    ELSIF p_date_type = 'LM' THEN
      --前月月末
      v_end_date := to_date(last_day(add_months(p_acct_date, -1)) );
    
    ELSIF p_date_type = 'LY' THEN
      --去年底日期
      v_end_date := to_date(to_char(INT(v_year) - 1) || '-12' || '-31' );
    
    ELSIF p_date_type = 'LQ' THEN
      --前季末日期
      IF INT(pmod(to_number(v_month), 3)) = 0 THEN
        v_pmod := 3;
      ELSE
        v_pmod := INT(pmod(to_number(v_month), 3));
      END IF;
      v_end_date := to_date(last_day(add_months(p_acct_date, -v_pmod)));
    
    ELSIF p_date_type = 'LH' THEN
      --前半年末日期
      IF INT(pmod(to_number(v_month), 6)) = 0 THEN
        v_pmod := 6;
      ELSE
        v_pmod := INT(pmod(to_number(v_month), 6));
      END IF;
      v_end_date := to_date(last_day(add_months(p_acct_date, -v_pmod)));
    
    ELSIF p_date_type = 'LT' THEN
      --上旬末日期
      IF v_day <= '10' THEN
        v_end_date := to_date(last_day(add_months(p_acct_date, -1))); --输入为上旬
      ELSIF v_day <= '20' THEN
        v_end_date := to_date(v_year || '-' || v_month || '-10'); --输入为中旬
      ELSE
        v_end_date := to_date(v_year || '-' || v_month || '-20'); --输入为下旬
      END IF;
    
    ELSIF p_date_type = 'LD' THEN
      --上一日
      v_end_date := to_date(p_acct_date - 1);
    
    ELSIF p_date_type = 'ND' THEN
      --下一日
      v_end_date := to_date(p_acct_date + 1);
    
    ELSIF p_date_type = 'NM' THEN
      --下月今日
      v_end_date := to_date(add_months(p_acct_date, 1));
    
    ELSIF p_date_type = 'NY' THEN
      --明年今日
      v_end_date := to_date(add_months(p_acct_date, 12));
    ELSIF p_date_type = 'LYT' THEN
      --滚动年开始日期
      v_end_date := to_date(cast(add_months(p_acct_date, -12) as date) + 1);
    ELSIF p_date_type = 'LQT' THEN
      --滚动季度开始日期
      v_end_date := to_date(cast(add_months(p_acct_date, -3) as date ) + 1);
    ELSIF p_date_type = 'LMT' THEN
      --滚动月份开始日期
      v_end_date := to_date(cast(add_months(p_acct_date, -1) as date) + 1);
    ELSIF p_date_type = 'LYD' THEN
      --上年同期日期
      v_end_date := to_date(add_months(p_acct_date, -12) );
    ELSIF p_date_type = 'LQD' THEN
      --上季度同期日期
      v_end_date := to_date(add_months(p_acct_date, -3) );
    ELSIF p_date_type = 'LMD' THEN
      --上月同期日期
      v_end_date := to_date(add_months(p_acct_date, -1) );
    END IF;
    RETURN v_end_date;
  END;
/ 

create or replace PROCEDURE omi.pro_mid_ln_prdt_base_info_actual(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：贷款产品信息中间表(实际还款)数据加载中间过程
执行周期：月/季/年
功能描述：加载贷款产品信息中间表(实际还款)数据
编写人： dingkai
编写日期：2019-9-18
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    v_delete_sql STRING;
    v_sql STRING;
    first_day_month STRING;
    first_day_quart STRING;
    first_day_hyear STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
BEGIN
--0   初始化
	str_procname :="omi.pro_mid_ln_prdt_base_info_actual";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     --'MB'为月初日期,
     first_day_month := to_char(omi.fun_get_date(i_etl_date, 'MB'), 'yyyyMMdd');
     --'QB'为季初日期,
     first_day_quart := to_char(omi.fun_get_date(i_etl_date, 'QB'), 'yyyyMMdd');
     --'HYB'为半年年初
     first_day_hyear := to_char(omi.fun_get_date(i_etl_date, 'HYB'), 'yyyyMMdd');
     --1.0 读取配置表信息
     --2.0 创建贷款产品信息中间表
     --3.0 清空表里面的数据
     v_delete_sql := "DELETE FROM omi.mid_ln_prdt_base_info_actual";
     EXECUTE IMMEDIATE (v_delete_sql);
     --3.0 加载数据
     v_sql := "INSERT INTO TABLE omi.mid_ln_prdt_base_info_actual
              select
                  ROW_NUMBER()
                   OVER
                        ( PARTITION BY t1.due_no
                          ORDER BY
                               t2.repay_date) AS pr_no
                   , t1.due_no
                   , t3.prdt_no
                   , t3.prdt_name
                   , t1.ln_rate
                   , t2.pcipal_amt
                   , t2.intst_amt
                   , t2.pun_intst_amt
                   , t2.comp_intst_amt
                   , t1.due_amt
                   , t1.beg_date
                   , t1.end_date
                   , t1.pay_state
                   , t2.repay_date
                   , DATEDIFF(STR_TO_DATE(t1.end_date)
                               , STR_TO_DATE(t1.beg_date)) as dif_days
                   , t4.prjt_name
              FROM
                  omi.ext_ln_due_mst t1 --贷款借据主表全量表
                  INNER JOIN omi.EXT_LN_REPAY_LIST t2 --贷款还款明细表
                  ON t1.due_no = t2.due_no
                  INNER JOIN omi.ext_prdt_base_info t3 --产品基本信息全量表
                  ON t1.prdt_no = t3.prdt_no
                  INNER JOIN omi.orc_pro_relation t4 --项目产品关系表
                  ON t3.prdt_name = t4.prdt_name
              WHERE
                   t2.repay_date BETWEEN '"|| first_day_month ||"' AND '"|| v_etl_date ||"'";
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_mid_ln_prdt_base_info_actual {etl_date}:'"|| v_etl_date ||"') is ok");
END;
/

create or replace PROCEDURE omi.pro_mid_ln_prdt_base_info_plan(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：贷款产品信息中间表(计划还款)数据加载中间过程
执行周期：月/季/年
功能描述：加载贷款产品信息中间表(计划还款)数据
编写人： dingkai
编写日期：2019-9-18
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    v_delete_sql STRING;
    v_sql STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
BEGIN
--0   初始化
	str_procname :="omi.pro_mid_ln_prdt_base_info_plan";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     --1.0 读取配置表信息
     --2.0 创建贷款产品信息中间表
     --3.0 清空表里面的数据
     v_delete_sql := "DELETE FROM omi.mid_ln_prdt_base_info_plan";
     EXECUTE IMMEDIATE (v_delete_sql);
     --3.0 加载数据
     v_sql := "INSERT INTO TABLE omi.mid_ln_prdt_base_info_plan
select
    ROW_NUMBER()
     OVER
          ( PARTITION BY t1.due_no
            ORDER BY
                 t2.curr_cnt) AS pr_no
     , t1.due_no
     , t3.prdt_no
     , t3.prdt_name
     , t1.ln_rate
     , t2.setl_int
     , t2.setl_od_int
     , t2.setl_com_int
     , t1.due_amt
     , t1.beg_date
     , t1.end_date
     , t1.pay_state
     , t2.last_trade_date
     , DATEDIFF(STR_TO_DATE(t1.end_date), STR_TO_DATE(t1.beg_date)) as dif_days
     , t4.prjt_name
FROM
    omi.ext_ln_due_mst t1 --贷款借据主表全量表
    , omi.EXT_LN_PAY_PLAN t2 --贷款还款计划全量表
    , omi.ext_prdt_base_info t3 --产品基本信息全量表
    , omi.orc_pro_relation t4 --项目产品关系表
WHERE
     t1.prdt_no = t3.prdt_no
     AND t1.due_no = t2.due_no
     AND t3.prdt_name = t4.prdt_name
     AND t1.beg_date IS NOT NULL
     AND t1.end_date IS NOT NULL";
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_mid_ln_prdt_base_info_plan {etl_date}:'"|| v_etl_date ||"') is ok");
END;
/

create or replace PROCEDURE omi.pro_mid_mb_acct_balance(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：理财魔方日存款余额中间表数据加载中间过程
执行周期：日
功能描述：理财魔方日存款余额中间表数据加工
编写人： dingkai
编写日期：2019-9-18
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    v_sql STRING;
    beg_qua STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
BEGIN
--0   初始化
	str_procname :="omi.pro_mid_mb_acct_balance";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     beg_qua := CONCAT(YEAR(STR_TO_DATE(v_etl_date))
                       , 'Q'
                       , QUARTER(v_etl_date, 'yyyyMMdd'));
                       --1.0 读取配置表信息
                       --2.0 加载数据更新或者插入
     v_sql := "MERGE INTO omi.mid_mb_acct_balance d USING(
              SELECT
              "
              ||v_etl_date || " AS DATA_DATE
              ,'" ||beg_qua || "' AS beg_qua
              ,abs(sum(t2.TOTAL_AMOUNT_PREV)) AS TOTAL_AMOUNT_PREV
              from omi.ext_mb_acct t1, omi.ext_mb_acct_balance t2
              WHERE
              t1.prod_type ='11040' and t1.INTERNAL_KEY=t2.INTERNAL_KEY and t2.AMT_TYPE= 'BAL'
              )s
              ON (s.DATA_DATE=d.data_date)
              WHEN MATCHED THEN UPDATE SET beg_qua=s.beg_qua,total_amount_prev=s.TOTAL_AMOUNT_PREV
              WHERE d.data_date=s.DATA_DATE
              WHEN NOT MATCHED THEN INSERT (data_date,beg_qua,total_amount_prev)
              VALUES(s.DATA_DATE,s.beg_qua,s.TOTAL_AMOUNT_PREV)";
              --put_line(v_sql);
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_mid_mb_acct_balance {etl_date}:'"|| v_etl_date ||"') is ok");
END;
/

create or replace PROCEDURE omi.pro_orc_tpay_main(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：代收付_主表全量表数据加载中间过程
执行周期：日
功能描述：加载代收付_主表全量表数据
编写人： dingkai
编写日期：2019-9-18
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    v_delete_data STRING;
    v_sql STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
BEGIN
--0   初始化
	str_procname :="omi.pro_orc_tpay_main";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     --1.0 读取配置表信息
     --2.0 创建贷款产品信息中间表
     --3.0 清空表里面的数据
     v_delete_data := "DELETE FROM omi.orc_tpay_main WHERE etl_date='"|| v_etl_date ||"'";
     EXECUTE IMMEDIATE (v_delete_data);
     --3.0 加载数据
     v_sql := "MERGE INTO omi.orc_tpay_main d
    USING (SELECT
                req_date
                 , req_syscode
                 , req_channel
                 , tx_date
                 , tx_time
                 , snd_channel
                 , br_no
                 , teller
                 , mchnt_cd
                 , bustype
                 , global_traceno
                 , tx_stat
                 , tx_th_trace
                 , sw_no
                 , tx_type
                 , product_type
                 , prot_no
                 , prot_chnl
                 , tx_object
                 , payer_bankcode
                 , payer_bankname
                 , payer_type
                 , payer_no
                 , payer_nm
                 , payee_bankcode
                 , payee_bankname
                 , payee_type
                 , payee_no
                 , payee_nm
                 , currency
                 , tx_amt
                 , tx_charge
                 , id_type
                 , id_no
                 , prov_code
                 , city_code
                 , phone_no
                 , buscode
                 , host_date
                 , host_trace
                 , host_retcode
                 , host_retmsg
                 , check_stat
                 , check_ident
                 , resp_traceno
                 , resp_date
                 , resp_time
                 , usage
                 , resp_charge
                 , remark
                 , syscode
                 , check_th_sts
                 , check_ht_sts
                 , check_date
                 , clear_date
                 , clear_sts
                 , clear_type
            FROM
                omi.ext_tpay_main) s
    ON(s.global_traceno = d.global_traceno)
WHEN MATCHED THEN
    UPDATE SET req_date = s.req_date
                , req_syscode = s.req_syscode
                , req_channel = s.req_channel
                , tx_date = s.tx_date
                , tx_time = s.tx_time
                , snd_channel = s.snd_channel
                , br_no = s.br_no
                , teller = s.teller
                , mchnt_cd = s.mchnt_cd
                , bustype = s.bustype
                , tx_stat = s.tx_stat
                , tx_th_trace = s.tx_th_trace
                , sw_no = s.sw_no
                , tx_type = s.tx_type
                , product_type = s.product_type
                , prot_no = s.prot_no
                , prot_chnl = s.prot_chnl
                , tx_object = s.tx_object
                , payer_bankcode = s.payer_bankcode
                , payer_bankname = s.payer_bankname
                , payer_type = s.payer_type
                , payer_no = s.payer_no
                , payer_nm = s.payer_nm
                , payee_bankcode = s.payee_bankcode
                , payee_bankname = s.payee_bankname
                , payee_type = s.payee_type
                , payee_no = s.payee_no
                , payee_nm = s.payee_nm
                , currency = s.currency
                , tx_amt = s.tx_amt
                , tx_charge = s.tx_charge
                , id_type = s.id_type
                , id_no = s.id_no
                , prov_code = s.prov_code
                , city_code = s.city_code
                , phone_no = s.phone_no
                , buscode = s.buscode
                , host_date = s.host_date
                , host_trace = s.host_trace
                , host_retcode = s.host_retcode
                , host_retmsg = s.host_retmsg
                , check_stat = s.check_stat
                , check_ident = s.check_ident
                , resp_traceno = s.resp_traceno
                , resp_date = s.resp_date
                , resp_time = s.resp_time
                , usage = s.usage
                , resp_charge = s.resp_charge
                , remark = s.remark
                , syscode = s.syscode
                , check_th_sts = s.check_th_sts
                , check_ht_sts = s.check_ht_sts
                , check_date = s.check_date
                , clear_date = s.clear_date
                , clear_sts = s.clear_sts
                , clear_type = s.clear_type
                , etl_date = '"|| v_etl_date ||"'
    WHERE
          d.global_traceno = s.global_traceno
WHEN NOT MATCHED THEN
    INSERT(req_date
                , req_syscode
                , req_channel
                , tx_date
                , tx_time
                , snd_channel
                , br_no
                , teller
                , mchnt_cd
                , bustype
                , global_traceno
                , tx_stat
                , tx_th_trace
                , sw_no
                , tx_type
                , product_type
                , prot_no
                , prot_chnl
                , tx_object
                , payer_bankcode
                , payer_bankname
                , payer_type
                , payer_no
                , payer_nm
                , payee_bankcode
                , payee_bankname
                , payee_type
                , payee_no
                , payee_nm
                , currency
                , tx_amt
                , tx_charge
                , id_type
                , id_no
                , prov_code
                , city_code
                , phone_no
                , buscode
                , host_date
                , host_trace
                , host_retcode
                , host_retmsg
                , check_stat
                , check_ident
                , resp_traceno
                , resp_date
                , resp_time
                , usage
                , resp_charge
                , remark
                , syscode
                , check_th_sts
                , check_ht_sts
                , check_date
                , clear_date
                , clear_sts
                , clear_type
                , etl_date)
     VALUES(s.req_date
            , s.req_syscode
            , s.req_channel
            , s.tx_date
            , s.tx_time
            , s.snd_channel
            , s.br_no
            , s.teller
            , s.mchnt_cd
            , s.bustype
            , s.global_traceno
            , s.tx_stat
            , s.tx_th_trace
            , s.sw_no
            , s.tx_type
            , s.product_type
            , s.prot_no
            , s.prot_chnl
            , s.tx_object
            , s.payer_bankcode
            , s.payer_bankname
            , s.payer_type
            , s.payer_no
            , s.payer_nm
            , s.payee_bankcode
            , s.payee_bankname
            , s.payee_type
            , s.payee_no
            , s.payee_nm
            , s.currency
            , s.tx_amt
            , s.tx_charge
            , s.id_type
            , s.id_no
            , s.prov_code
            , s.city_code
            , s.phone_no
            , s.buscode
            , s.host_date
            , s.host_trace
            , s.host_retcode
            , s.host_retmsg
            , s.check_stat
            , s.check_ident
            , s.resp_traceno
            , s.resp_date
            , s.resp_time
            , s.usage
            , s.resp_charge
            , s.remark
            , s.syscode
            , s.check_th_sts
            , s.check_ht_sts
            , s.check_date
            , s.clear_date
            , s.clear_sts
            , s.clear_type
            , '"|| v_etl_date ||"')";
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_orc_tpay_main {etl_date}:'"|| v_etl_date ||"') is ok");
END;
/

create or replace PROCEDURE omi.pro_rpt_partner_fee_hyear(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：半年结合作方服务费统计表数据加载中间过程
执行周期：年度
功能描述：半年结合作方服务费统计表
编写人： dingkai
编写日期：2019-9-19
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    v_sql STRING;
    COUNT_YEAR STRING;
    v_delete_data STRING;
    first_day_hyear STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
BEGIN
--0   初始化
	str_procname :="omi.pro_rpt_partner_fee_hyear";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     COUNT_YEAR := CASE WHEN MONTH(STR_TO_DATE(i_etl_date)) <= 6 THEN
                        CONCAT(YEAR(STR_TO_DATE(i_etl_date)), 'Y1')
                        WHEN MONTH(STR_TO_DATE(i_etl_date)) > 6 THEN
                        CONCAT(YEAR(STR_TO_DATE(i_etl_date)), 'Y2')
                   ELSE
                       NULL
                   END;
                   --1.0 读取配置表信息
                   --2.0 删除数据
     first_day_hyear := to_char(omi.fun_get_date(i_etl_date, 'HYB'), 'yyyyMMdd');
     v_delete_data := "DELETE FROM omi.rpt_partner_fee_hyear WHERE count_year='"|| COUNT_YEAR ||"'";
     EXECUTE IMMEDIATE (v_delete_data);
     --3.0 加载数据
     v_sql := "with t1 as
                  (select
                      count(1) AS ct
                  from
                      omi.orc_tpay_main
                  where
                       syscode = 'TCIB'
                       AND tx_type = '10'
                       AND tx_object = '00'
                       AND tx_stat = 'D013'
                       AND tx_date BETWEEN '"|| first_day_hyear ||"' AND '"|| v_etl_date ||"'), --个人客户代付
              t2 as
                  (select
                      count(1) AS ct
                  from
                      omi.orc_tpay_main
                  where
                       syscode = 'TCIB'
                       AND tx_stat = 'D005'
                       AND tx_date BETWEEN '"|| first_day_hyear ||"' AND '"|| v_etl_date ||"'), --个人客户代扣
              t3 as
                  (select
                      count(1) AS ct
                  from
                      omi.orc_tpay_main
                  where
                       syscode = 'TCIB'
                       AND tx_type = '10'
                       AND tx_object = '01'
                       AND tx_stat = 'D013'
                       AND tx_amt <= 50000
                       AND tx_date BETWEEN '"|| first_day_hyear ||"' AND '"|| v_etl_date ||"'), --单位客户代付（<=5W）
              t4 as
                  (select
                      count(1) AS ct
                  from
                      omi.orc_tpay_main
                  where
                       syscode = 'TCIB'
                       AND tx_type = '10'
                       AND tx_object = '01'
                       AND tx_stat = 'D013'
                       AND tx_amt > 50000
                       AND tx_date BETWEEN '"|| first_day_hyear ||"' AND '"|| v_etl_date ||"'), --单位客户代付（>5W）
              t5 as
                  (select
                      count(1) AS ct
                  from
                      omi.ext_tpay_protocol
                  where
                       prot_route = 'D3031300'
                       AND sign_date BETWEEN '"|| first_day_hyear ||"' AND '"|| v_etl_date ||"') --四要素鉴权
                       INSERT INTO omi.RPT_PARTNER_FEE_HYEAR
              select
                  ROW_NUMBER()
                   OVER
                        ( PARTITION BY UPDT_DATE
                          ORDER BY
                               prjt_name) AS RPT_NO
                   , PRJT_NAME
                   , SERVICE_FEE
                   , DUE_AMT
                   , AVDY_AMOUNT
                   , UNIT_AMT
                   , AU_TRC
                   , '"|| COUNT_YEAR ||"' AS COUNT_YEAR
                   , UPDT_DATE
                   , NULL as REMARK
              FROM
                  (SELECT
                       '银银平台项目' AS PRJT_NAME
                        , ((t1.ct + t3.ct) * 0.5 + t2.ct * 2 + t4.ct * 5.5
                            + t5.ct) as SERVICE_FEE
                        , t1.ct as DUE_AMT
                        , t2.ct as AVDY_AMOUNT
                        , (t3.ct + t4.ct) as UNIT_AMT
                        , t5.ct as AU_TRC
                        , '"|| v_etl_date ||"' as UPDT_DATE
                   from
                       t1
                       , t2
                       , t3
                       , t4
                       , t5
                   WHERE
                        1 = 1)";
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_rpt_partner_fee_hyear {COUNT_YEAR}:'"|| COUNT_YEAR ||"')  is ok");
END;
/

create or replace PROCEDURE omi.pro_rpt_partner_fee_month(i_etl_date IN STRING) IS
    /**************************************************************************************************
     过程中文名：月结合作方服务费统计表数据加载中间过程
     执行周期：月度
     功能描述：月结合作方服务费统计表
     编写人： dingkai
     编写日期：2019-9-19
    **************************************************************************************************/
    --过程变量
    v_etl_date STRING;
    v_sql STRING;
    beg_month STRING;
    v_delete_data STRING;
    first_day_month STRING;
    --first_day_quart STRING;
    --first_day_hyear STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
  BEGIN
    --0   初始化
	str_procname :="omi.pro_rpt_partner_fee_month";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
    --0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
    v_etl_date        := i_etl_date;    
    beg_month         :=SUBSTRING(i_etl_date,0,6);
     --'MB'为月初日期,
	first_day_month	:= to_char(omi.fun_get_date(i_etl_date,'MB'),'yyyyMMdd');
	--'QB'为季初日期,
	--first_day_quart := to_char(omi.fun_get_date(i_etl_date,'QB'),'yyyyMMdd');
	--'HYB'为半年年初
	--first_day_hyear := to_char(omi.fun_get_date(i_etl_date,'HYB'),'yyyyMMdd');
    --1.0 读取配置表信息
    --2.0 删除数据
    v_delete_data :="DELETE FROM omi.rpt_partner_fee_month WHERE count_mon='"||beg_month ||"'";
	EXECUTE IMMEDIATE (v_delete_data);
    --3.0 加载数据
     v_sql := "INSERT INTO omi.rpt_partner_fee_month
	SELECT
    ROW_NUMBER()
     OVER
          ( PARTITION BY UPDT_DATE
            ORDER BY
                 prjt_name) AS RPT_NO
     , prjt_name
     , SERVICE_FEE
     , SETL_INT
     , '"||beg_month||"' AS COUNT_MON
     , UPDT_DATE
     , NULL AS REMARK
FROM
    (WITH t1 AS
         (SELECT
             sum(due_amt) AS due_amt
         FROM
             omi.mid_ln_prdt_base_info_plan
         WHERE
              prjt_name = '沣邦项目'
              AND pr_no = '1'
              AND beg_date BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"') --当月放款量
     SELECT
         a.prjt_name
          , sum(a.service_fee) AS service_fee
          , sum(a.setl_int) AS setl_int
          , '"||v_etl_date||"' AS updt_date
     FROM
         (SELECT
              prjt_name
               , s.intst_amt AS setl_int
               , CASE WHEN t1.due_amt <= 200000000 THEN
                       (((s.ln_rate / 100) - 0.067) * s.intst_amt
                        / (s.ln_rate / 100)
                        + 0.375 * (s.pun_intst_amt + s.comp_intst_amt))
                       WHEN t1.due_amt > 200000000 THEN
                       (((s.ln_rate / 100) - 0.083) * s.intst_amt
                        / (s.ln_rate / 100)
                        + 0.375 * (s.pun_intst_amt + s.comp_intst_amt))
                  ELSE
                      NULL
                  END AS SERVICE_FEE
          FROM
              omi.mid_ln_prdt_base_info_actual s
              , t1
          WHERE
               prjt_name = '沣邦项目'
               AND repay_date BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"') a
     GROUP BY
          a.prjt_name
      union ALL
      --力合普惠项目 OK
      SELECT
          b.prjt_name
           , sum(b.service_fee) AS service_fee
           , sum(b.setl_int) AS setl_int
           , '"||v_etl_date||"' AS updt_date
      FROM
          (SELECT
               prjt_name
                , CASE WHEN prdt_name = '友金普惠（二期）' THEN
                        ((ln_rate / 100) - 0.088) / (ln_rate / 100)
                        * intst_amt
                        WHEN prdt_name = '友金普惠贷款产品' THEN
                        (ln_rate - 0.088) / ln_rate
                        * intst_amt
                   ELSE
                       NULL
                   END AS SERVICE_FEE
                , intst_amt AS setl_int
           FROM
               omi.mid_ln_prdt_base_info_actual
           WHERE
                prjt_name = '力合普惠项目'
                AND repay_date BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"') b
      GROUP BY
           b.prjt_name
      union ALL
      --优卡分期项目 OK
      SELECT
          c.prjt_name
           , sum(c.SERVICE_FEE) AS service_fee
           , sum(c.setl_int) AS setl_int
           , '"||v_etl_date||"' AS updt_date
      FROM
          (SELECT
               prjt_name
               -- , (intst_amt + pun_intst_amt + comp_intst_amt)
                , intst_amt / (ln_rate / 100)
                   * ((ln_rate / 100) - 0.12) AS service_fee
                , intst_amt AS setl_int
           FROM
               omi.mid_ln_prdt_base_info_actual
           WHERE
                prjt_name = '优卡分期项目'
                AND repay_date BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"') c
      GROUP BY
           c.prjt_name)";
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_rpt_partner_fee_month {beg_month}:'"||beg_month ||"')  is ok");
  END;
/

  create or replace PROCEDURE omi.pro_rpt_partner_fee_quart(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：季结合作方服务费统计表数据加载中间过程
执行周期：季度
功能描述：季结合作方服务费统计表
编写人： dingkai
编写日期：2019-9-19
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    beg_qua STRING;
    v_sql STRING;
    v_delete_data STRING;
    first_day_quart STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
BEGIN
	--0   初始化
	str_procname :="omi.pro_rpt_partner_fee_quart";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
	--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     beg_qua := CONCAT(YEAR(STR_TO_DATE(i_etl_date))
                       , 'Q'
                       , QUARTER(i_etl_date, 'yyyyMMdd'));
                       --1.0 读取配置表信息
                       --2.0 删除数据
     first_day_quart := to_char(omi.fun_get_date(i_etl_date,'QB'),'yyyyMMdd');
     v_delete_data := "DELETE FROM omi.rpt_partner_fee_quart WHERE count_qua='"|| beg_qua ||"'";
     EXECUTE IMMEDIATE (v_delete_data);
     --3.0 加载数据
     v_sql := "WITH t1 AS
    (SELECT
        '理财魔方项目' AS PRJT_NAME
         , sum(total_amount_prev) / 365 AS AVDY_AMOUNT
    FROM
        omi.mid_mb_acct_balance
    WHERE
         data_date BETWEEN '"|| first_day_quart ||"' AND '"|| v_etl_date ||"') --理财魔方
INSERT INTO omi.rpt_partner_fee_quart
SELECT
    ROW_NUMBER()
     OVER
          ( PARTITION BY UPDT_DATE
            ORDER BY
                 prjt_name) AS RPT_NO
     , prjt_name
     , SERVICE_FEE
     , DUE_AMT
     , AVDY_AMOUNT
     , '"|| beg_qua ||"' AS COUNT_QUA
     , UPDT_DATE
     , NULL AS REMARK
from
    (SELECT
         '理财魔方项目' AS prjt_name
          , CASE WHEN t1.AVDY_AMOUNT <= 100000000 THEN
                  t1.AVDY_AMOUNT * 0.001
                  WHEN t1.AVDY_AMOUNT > 100000000
                        AND t1.AVDY_AMOUNT <= 200000000 THEN
                  t1.AVDY_AMOUNT * 0.015
                  WHEN t1.AVDY_AMOUNT > 200000000
                        AND t1.AVDY_AMOUNT <= 300000000 THEN
                  t1.AVDY_AMOUNT * 0.02
                  WHEN t1.AVDY_AMOUNT > 300000000
                        AND t1.AVDY_AMOUNT <= 500000000 THEN
                  t1.AVDY_AMOUNT * 0.025
                  WHEN t1.AVDY_AMOUNT > 500000000 THEN
                  t1.AVDY_AMOUNT * 0.03
             ELSE
                 NULL
             END AS SERVICE_FEE
          , NULL AS DUE_AMT
          , t1.AVDY_AMOUNT AS AVDY_AMOUNT
          , '"|| v_etl_date ||"' AS UPDT_DATE
     FROM
         t1
      )";
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_rpt_partner_fee_quart {beg_qua}:'"|| beg_qua ||"')  is ok");
END;
/

DELETE FROM omi.orc_pro_relation;  
INSERT INTO omi.orc_pro_relation(prdt_name,prjt_name) VALUES
('沣邦汽车电子存款户','沣邦项目')
,('沣邦汽车（消费）','沣邦项目')
,('沣邦汽车(二期)电子存款户','沣邦项目')
,('沣邦汽车（经营）','沣邦项目')
,('金融魔方','理财魔方项目')
,('友金普惠（二期）','力合普惠项目')
,('友金普惠贷款产品','力合普惠项目')
,('友金普惠电子存款户','力合普惠项目')
,('优卡分期电子存款户','优卡分期项目')
,('优卡分期','优卡分期项目')
,('兴业代收付','银银平台项目')
,('蛋壳公寓贷款产品','神州融项目');

