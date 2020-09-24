!set plsqlUseSlash true

create or replace PROCEDURE omi.pro_mid_ln_prdt_base_info_actual(i_etl_date IN string) IS
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
     v_day STRING;
BEGIN
	--0   初始化
	str_procname :="omi.pro_mid_ln_prdt_base_info_actual";
    str_msg :="开始执行";
    put_line(""||str_msg||":"||str_procname||"");
	--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     omi.pro_orc_sys_log(1,str_procname,v_etl_date,str_msg);
     v_day   :=SUBSTRING(v_etl_date,7,8);
     --put_line(v_day)
     IF v_day = '01' THEN
     	--'LMB'为上月初日期,
		first_day_month	:= to_char(omi.fun_get_date(v_etl_date,'LMB'),'yyyyMMdd');
	 ELSE
     --'MB'为月初日期,
     	first_day_month := to_char(omi.fun_get_date(v_etl_date, 'MB'), 'yyyyMMdd');
     END IF;
     --put_line(first_day_month)
     --'QB'为季初日期,
     first_day_quart := to_char(omi.fun_get_date(v_etl_date, 'QB'), 'yyyyMMdd');
     --'HYB'为半年年初
     first_day_hyear := to_char(omi.fun_get_date(v_etl_date, 'HYB'), 'yyyyMMdd');
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
     str_msg :="成功执行";
     put_line(""||str_msg||":"||str_procname||"");
     omi.pro_orc_sys_log(1,str_procname,v_etl_date,str_msg);
     put_line("贷款产品信息中间表(实际还款)数据加载中间过程({pro_name}:pro_mid_ln_prdt_base_info_actual {etl_date}:'"|| v_etl_date ||"') is ok");
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
     first_day_month STRING;
     first_day_quart STRING;
     first_day_hyear STRING;
	 str_procname STRING;
	 str_etl_date STRING;
	 str_msg STRING;
	 v_day STRING;
BEGIN
--0   初始化
    str_procname := "omi.pro_mid_ln_prdt_base_info_plan";
     str_msg := "开始执行";
     put_line(""||str_msg||":"||str_procname||"");
     --0.1  环境变量初始化
     set_env('transaction.type', 'inceptor');
     --0.2  变量赋值
     v_etl_date := i_etl_date;
     omi.pro_orc_sys_log(1,str_procname,v_etl_date,str_msg);
     v_day   :=SUBSTRING(v_etl_date,7,8);
     --put_line(v_day)
     IF v_day = '01' THEN
     	--'LMB'为上月初日期,
		first_day_month	:= to_char(omi.fun_get_date(v_etl_date,'LMB'),'yyyyMMdd');
	 ELSE
     --'MB'为月初日期,
     	first_day_month := to_char(omi.fun_get_date(v_etl_date, 'MB'), 'yyyyMMdd');
     END IF;
     --put_line(first_day_month)
     --'QB'为季初日期,
     first_day_quart := to_char(omi.fun_get_date(i_etl_date, 'QB'), 'yyyyMMdd');
     --'HYB'为半年年初
     first_day_hyear := to_char(omi.fun_get_date(i_etl_date, 'HYB'), 'yyyyMMdd');
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
                                 , DATEDIFF(STR_TO_DATE(t1.end_date)
                                             , STR_TO_DATE(t1.beg_date)) as dif_days
                                 , t4.prjt_name
                            FROM
                                omi.ext_ln_due_mst t1 --贷款借据主表全量表
                                INNER JOIN omi.EXT_LN_PAY_PLAN t2 --贷款还款计划全量表
                                ON t1.due_no = t2.due_no
                                INNER JOIN omi.ext_prdt_base_info t3 --产品基本信息全量表
                                ON t1.prdt_no = t3.prdt_no
                                INNER JOIN omi.orc_pro_relation t4 --项目产品关系表
                                ON t3.prdt_name = t4.prdt_name
                            WHERE
                                 t1.beg_date BETWEEN '"
              || first_day_month || "' AND '" || v_etl_date || "'";
     EXECUTE IMMEDIATE (v_sql);
     str_msg := "成功执行";
     put_line(""||str_msg||":"||str_procname||"");
     omi.pro_orc_sys_log(1, str_procname, i_etl_date, str_msg);
     put_line("贷款产品信息中间表(计划还款)数据加载中间过程({pro_name}:pro_mid_ln_prdt_base_info_plan {etl_date}:"
              || v_etl_date || ") is ok");
END;
/

create or replace PROCEDURE omi.pro_mid_mb_acct_balance(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：上日存款余额中间表数据加载中间过程
执行周期：日
功能描述：上日存款余额中间表数据加工
编写人： dingkai
编写日期：2019-11-1
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    v_sql STRING;
    v_delete_sql STRING;
    str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
BEGIN
--0   初始化
	str_procname :="omi.pro_mid_mb_acct_balance";
	str_msg :="开始执行";
	put_line(""||str_msg||":"||str_procname||"");
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     --1.0 读取配置表信息
     --2.0 加载数据更新或者插入
     --3.0 清除当天数据如果存在
     v_delete_sql := "DELETE FROM omi.mid_mb_acct_balance where etl_date='"||v_etl_date||"'";
     EXECUTE IMMEDIATE (v_delete_sql);
     v_sql := "INSERT INTO omi.mid_mb_acct_balance
SELECT
    *
FROM
    (SELECT
         '"||v_etl_date||"' AS etl_date
          , '理财魔方项目' AS prjt_name
          , abs(sum(t2.total_amount_prev)) AS total_amount_prev
          , null as bal
     FROM
         omi.ext_mb_acct t1
         LEFT JOIN omi.ext_mb_acct_balance t2
         ON t1.internal_key = t2.internal_key
     WHERE
          t1.prod_type = '11040' AND t2.amt_type = 'BAL')
 UNION ALL
 (SELECT
      '"||v_etl_date||"' AS etl_date
       , '新证华益项目' AS prjt_name
       , abs(sum(t2.total_amount_prev)) AS total_amount_prev
       , null as bal
  FROM
      omi.ext_mb_acct t1
      LEFT JOIN omi.ext_mb_acct_balance t2
      ON t1.internal_key = t2.internal_key
  WHERE
       t1.prod_type = '11046'
       AND t2.amt_type = 'BAL'
       AND abs(t2.total_amount_prev) >= 1000)
 UNION ALL
 SELECT
     '"||v_etl_date||"' AS etl_date
      , '360项目' AS prjt_name
      , sum(total_amount_prev) AS total_amount_prev
      , null as bal
 FROM
     (SELECT
          client_no
           , abs(sum(total_amount_prev)) AS total_amount_prev
      FROM
          (SELECT
               t1.client_no
                , t2.total_amount_prev
           FROM
               omi.ext_mb_acct t1
               LEFT JOIN omi.ext_mb_acct_balance t2
               ON t1.internal_key = t2.internal_key
           WHERE
                t1.SOURCE_TYPE = 'HCNCF'
                AND t2.amt_type = 'BAL'
                AND prod_type IN ('10050', '10070', '11048'))
      GROUP BY
           client_no
      HAVING
          total_amount_prev > 100)
 UNION ALL
 SELECT
     '"||v_etl_date||"' AS etl_date
      , '度小满项目' AS prjt_name
      , sum(total_amount_prev) AS total_amount_prev
      , null as bal
 FROM
     (SELECT
          client_no
           , abs(sum(total_amount_prev)) AS total_amount_prev
      FROM
          (SELECT
               t1.client_no
                , t2.total_amount_prev
           FROM
               omi.ext_mb_acct t1
               LEFT JOIN omi.ext_mb_acct_balance t2
               ON t1.internal_key = t2.internal_key
           WHERE
                t1.SOURCE_TYPE = 'HCDXM'
                AND t2.amt_type = 'BAL'
                AND prod_type IN ('10050', '10070', '11047'))
      GROUP BY
           client_no
      HAVING
          total_amount_prev >= 100)
 UNION ALL
 SELECT
     '"||v_etl_date||"' AS etl_date
      , '网金项目' AS prjt_name
      , sum(total_amount_prev) AS total_amount_prev
      , null as bal
 FROM
     (SELECT
          client_no
           , abs(sum(total_amount_prev)) AS total_amount_prev
      FROM
          (SELECT
               t1.client_no
                , t2.total_amount_prev
           FROM
               omi.ext_mb_acct t1
               LEFT JOIN omi.ext_mb_acct_balance t2
               ON t1.internal_key = t2.internal_key
           WHERE
                t1.SOURCE_TYPE = 'HCWJZX'
                AND t2.amt_type = 'BAL'
                AND prod_type IN ('10050', '10070', '11045'))
      GROUP BY
           client_no
      HAVING
          total_amount_prev >= 5000)
 UNION ALL
 SELECT
     '"||v_etl_date||"' AS etl_date
      , ti3.prjt_name as prjt_name
      , null AS total_amount_prev
      , sum(ti1.bal) AS bal --贷款余额
 FROM
     omi.ext_ln_due_mst ti1
     INNER JOIN omi.ext_prdt_base_info ti2 --产品基本信息全量表
     ON ti1.prdt_no = ti2.prdt_no
     INNER JOIN omi.orc_pro_relation ti3 --项目产品关系表
     ON ti2.prdt_name = ti3.prdt_name
 where
      ti3.prjt_name in ('沣邦汽车（消费）','沣邦汽车（经营）','沣邦汽车（经营）', '力合普惠项目', '优卡分期项目')
 group by
      prjt_name";
              --put_line(v_sql);
     EXECUTE IMMEDIATE (v_sql);
     str_msg :="成功执行";
     put_line(""||str_msg||":"||str_procname||"");
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_mid_mb_acct_balance {etl_date}:'"||v_etl_date||"') is ok");
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
	put_line(""||str_msg||":"||str_procname||"");
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
     --'HYB'为半年年初
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
     str_msg :="成功执行";
     put_line(""||str_msg||":"||str_procname||"");
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("半年结合作方服务费统计表数据加载中间过程({pro_name}:pro_rpt_partner_fee_hyear {COUNT_YEAR}:"
              || COUNT_YEAR || ")  is ok");
END;
/

create or replace PROCEDURE omi.pro_rpt_partner_fee_month(i_etl_date IN STRING) IS
    /**************************************************************************************************
     过程中文名：月结合作方服务费统计表数据加载中间过程
     执行周期：月度(会存)
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
	put_line(""||str_msg||":"||str_procname||"");
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
    v_delete_data :="DELETE FROM omi.rpt_partner_fee_month WHERE updt_date='"||v_etl_date||"'";
	EXECUTE IMMEDIATE (v_delete_data);
    --3.0 加载数据
     v_sql := "INSERT INTO omi.rpt_partner_fee_month
	SELECT
       rpt_no
     , prjt_name --项目名称
     , service_fee --收费金额
     , bal --贷款余额
     , av_bal --日均贷款余额
     , total_amount_prev --有效存款余额
     , av_amount_prev --日均有效存款余额
     , rate --费率
     , av_ln_rate --利率平均值
     , '"||beg_month||"' as count_mon
     , updt_date --报表日期
     , remark --备注
FROM
    (WITH t1 AS
    (SELECT
        sum(total_amount_prev * 0.2 / 100 / 360) AS service_fee
         , sum(total_amount_prev) / count(total_amount_prev) AS av_amount_prev
    FROM
        omi.mid_mb_acct_balance
    WHERE
         prjt_name = '度小满项目'
         AND etl_date
              BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"')
t2 AS
    (SELECT
        prjt_name
         , total_amount_prev
    FROM
        omi.mid_mb_acct_balance
    WHERE
         prjt_name = '度小满项目'
         AND etl_date = '"||v_etl_date||"')
SELECT
    '会存1' as rpt_no
     , t2.prjt_name AS prjt_name
     , t1.service_fee AS service_fee
     , NULL AS bal
     , NULL AS av_bal
     , t2.total_amount_prev AS total_amount_prev
     , t1.av_amount_prev AS av_amount_prev
     , 0.2 AS rate
     , NULL AS av_ln_rate
     , '"||v_etl_date||"' AS updt_date
     , '日服务费=有效存款余额*0.2%/360 月服务费=当月日服务费累计 有效存款余额=度小满引流会存客户名下存款当日余额(含活期和会存)>=100元合计' AS remark
FROM
    t1
    , t2
 UNION
 WITH t3 AS
     (SELECT
         sum(total_amount_prev * 0.2 / 100 / 360) AS service_fee
          , sum(total_amount_prev) / count(total_amount_prev) AS av_amount_prev
     FROM
         omi.mid_mb_acct_balance
     WHERE
          prjt_name = '360项目'
          AND etl_date
               BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"')
 t4 AS
     (SELECT
         prjt_name
          , total_amount_prev
     FROM
         omi.mid_mb_acct_balance
     WHERE
          prjt_name = '360项目'
          AND etl_date = '"||v_etl_date||"')
 SELECT
     '会存2' as rpt_no
      , t4.prjt_name AS prjt_name
      , t3.service_fee AS service_fee
      , NULL AS bal
      , NULL AS av_bal
      , t4.total_amount_prev AS total_amount_prev
      , t3.av_amount_prev AS av_amount_prev
      , 0.2 AS rate
      , NULL AS av_ln_rate
      , '"||v_etl_date||"' AS updt_date
      , '日服务费=有效存款余额*0.2%/360 月服务费=当月日服务费累计 有效存款余额=360引流会存客户名下存款当日余额(含活期和会存)>100元的客户资金合计' AS remark
 FROM
     t3
     , t4
  UNION
  WITH t5 AS
      (SELECT
          sum(total_amount_prev) / count(total_amount_prev) AS av_amount_prev
      FROM
          omi.mid_mb_acct_balance
      WHERE
           prjt_name = '新证华益项目'
           AND etl_date
                BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"')
  t6 AS
      (SELECT
          total_amount_prev
      FROM
          omi.mid_mb_acct_balance
      WHERE
           prjt_name = '新证华益项目'
           AND etl_date = '"||v_etl_date||"')
  t7 AS
      (SELECT
          prjt_name
           , total_amount_prev
           , CASE WHEN total_amount_prev < 100000000 THEN
                   0.1
                   WHEN total_amount_prev >= 100000000
                         AND total_amount_prev < 200000000 THEN
                   0.15
                   WHEN total_amount_prev >= 200000000 THEN
                   0.02
              ELSE
                  0
              END AS rate
      FROM
          omi.mid_mb_acct_balance
      WHERE
           prjt_name = '新证华益项目'
           AND etl_date
                BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"')
  SELECT
      '会存3' as rpt_no
       , t7.prjt_name AS prjt_name
       , sum(t7.total_amount_prev * t7.rate / 100 / 360) AS service_fee
       , NULL AS bal
       , NULL AS av_bal
       , t6.total_amount_prev AS total_amount_prev
       , t5.av_amount_prev AS av_amount_prev
       , CASE WHEN t6.total_amount_prev < 100000000 THEN
               0.1
               WHEN t6.total_amount_prev >= 100000000
                     AND t6.total_amount_prev < 200000000 THEN
               0.15
               WHEN t6.total_amount_prev >= 200000000 THEN
               0.02
          ELSE
              0
          END AS rate
       , NULL AS av_ln_rate
       , '"||v_etl_date||"' AS updt_date
       , '日服务费=会存每日余额*服务费率/360 月服务费=当月日服务费累计 只统计每日名下会存存款合计>=1000元以上的客户 沉淀资金=会存每日余额 沉淀资金<1亿,服务费率=0.1%;1亿<=沉淀资金<2亿,服务费率=0.15%;沉淀资金>=2亿,服务费率=0.02%' AS remark
  FROM
      t5
      , t6
      , t7
  GROUP BY
       t7.prjt_name
       , t6.total_amount_prev
       , t5.av_amount_prev
   UNION
   WITH t8 AS
       (SELECT
           sum(total_amount_prev) / count(total_amount_prev) AS av_amount_prev
       FROM
           omi.mid_mb_acct_balance
       WHERE
            prjt_name = '网金项目'
            AND etl_date
                 BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"')
   t9 AS
       (SELECT
           total_amount_prev
       FROM
           omi.mid_mb_acct_balance
       WHERE
            prjt_name = '网金项目'
            AND etl_date = '"||v_etl_date||"')
   t10 AS
       (SELECT
           prjt_name
            , total_amount_prev AS total_amount_prev
            , CASE WHEN total_amount_prev < 10000000 THEN
                    0.1
                    WHEN total_amount_prev >= 10000000 THEN
                    0.3
               ELSE
                   0
               END AS rate
       FROM
           omi.mid_mb_acct_balance
       WHERE
            prjt_name = '网金项目'
            AND etl_date
                 BETWEEN '"||first_day_month||"' AND '"||v_etl_date||"')
   SELECT
       '会存4' as rpt_no
        , t10.prjt_name AS prjt_name
        , sum(t10.total_amount_prev * t10.rate / 100 / 360) AS service_fee
        , NULL AS bal
        , NULL AS av_bal
        , t9.total_amount_prev AS total_amount_prev
        , t8.av_amount_prev AS av_amount_prev
        , CASE WHEN t9.total_amount_prev < 10000000 THEN
                0.1
                WHEN t9.total_amount_prev >= 10000000 THEN
                0.3
           ELSE
               0
           END AS rate
        , NULL AS av_ln_rate
        , '"||v_etl_date||"' AS updt_date
        , '日服务费=有效存款余额*费率/360 月服务费=当月日服务费累计 有效存款余额=网金会存客户名下存款当日余额(含活期和会存)>=5000元 费率:有效存款余额<1000万元,费率=0.1%;有效存款余额>=1000万元,费率=0.3%' AS remark
   FROM
       t8
       , t9
       , t10
   GROUP BY
        t10.prjt_name
        , t9.total_amount_prev
        , t8.av_amount_prev)";
     EXECUTE IMMEDIATE (v_sql);
     str_msg :="成功执行";
     put_line(""||str_msg||":"||str_procname||"");
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("月结合作方服务费统计表数据加载中间过程({pro_name}:pro_rpt_partner_fee_month {beg_month}:"||beg_month ||")  is ok");
  END;
/

create or replace PROCEDURE omi.pro_rpt_partner_fee_month2(i_etl_date IN STRING) IS
    /**************************************************************************************************
     过程中文名：月结合作方服务费统计表数据加载中间过程
     执行周期：月度2号(贷款)
     功能描述：月结合作方服务费统计表
     编写人： dingkai
     编写日期：2019-12-6
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
    str_procname :="omi.pro_rpt_partner_fee_month2";
	str_msg :="开始执行";
	put_line(""||str_msg||":"||str_procname||"");
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
    --0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
    v_etl_date        := i_etl_date;    
     --'LMB'为上月初日期,
	first_day_month	:= to_char(omi.fun_get_date(i_etl_date,'LMB'),'yyyyMMdd');
	beg_month         :=SUBSTRING(first_day_month,0,6);
	--'QB'为季初日期,
	--first_day_quart := to_char(omi.fun_get_date(i_etl_date,'QB'),'yyyyMMdd');
	--'HYB'为半年年初
	--first_day_hyear := to_char(omi.fun_get_date(i_etl_date,'HYB'),'yyyyMMdd');
    --1.0 读取配置表信息
    --2.0 删除数据
    v_delete_data :="DELETE FROM omi.rpt_partner_fee_month WHERE updt_date='"||v_etl_date||"'";
	EXECUTE IMMEDIATE (v_delete_data);
    --3.0 加载数据
     v_sql := "INSERT INTO omi.rpt_partner_fee_month
SELECT
    rpt_no
     , prjt_name --项目名称
     , service_fee --收费金额
     , bal --贷款余额
     , av_bal --日均贷款余额
     , total_amount_prev --有效存款余额
     , av_amount_prev --日均有效存款余额
     , rate --费率
     , av_ln_rate --利率平均值
     , '"||beg_month||"' AS count_mon
     , '"||v_etl_date||"' AS updt_date --报表日期
     , remark --备注, 
from
    (WITH t1 AS
         (SELECT
             sum(due_amt) AS due_amt --总放款额
         FROM
             omi.mid_ln_prdt_base_info_plan
         WHERE
              prjt_name = '沣邦汽车（消费）'
              AND pr_no = '1'
              AND beg_date >= '"||first_day_month||"'
              AND beg_date < '"||v_etl_date||"') --当月放款量
     t2 AS
         (SELECT
             bal
         FROM
             omi.mid_mb_acct_balance
         WHERE
              prjt_name = '沣邦汽车（消费）'
              AND etl_date = '"||v_etl_date||"')
     t3 AS
         (SELECT
             sum(bal) / 365 AS av_bal
         FROM
             omi.mid_mb_acct_balance
         WHERE
              prjt_name = '沣邦汽车（消费）'
              AND etl_date > '"||first_day_month||"'
              AND etl_date <= '"||v_etl_date||"')
     SELECT
         '贷款1' as rpt_no
          , a.prjt_name AS prjt_name
          , sum(a.service_fee) AS service_fee
          , t2.bal AS bal --贷款余额
          , t3.av_bal AS av_bal
          , NULL AS total_amount_prev
          , NULL AS av_amount_prev
          , NULL AS rate
          , sum(a.ln_rate) / count(a.ln_rate) AS av_ln_rate
          , '月服务费=本月服务费+本月附加服务费 总放款量<=2亿元：本月服务费=(借款利率-6.7%)*当月偿还的正常利息/借款利率 本月附加服务费=0.375*当月偿还的罚息(含复利)/借款利率 总放款量>2亿元： 本月服务费=(借款利率-8.3%)*当月偿还的正常利息/借款利率 本月附加服务费=0.375*当月偿还的罚息(含复利)/借款利率' AS remark
     FROM
         (SELECT
              prjt_name
               , s.ln_rate / 100 AS ln_rate
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
                  END AS service_fee
          FROM
              omi.mid_ln_prdt_base_info_actual s
              , t1
          WHERE
               prjt_name = '沣邦汽车（消费）'
               AND repay_date >= '"||first_day_month||"'
               AND repay_date < '"||v_etl_date||"') a
         , t2
         , t3
     GROUP BY
          prjt_name
          ,bal
          ,av_bal
      union
      --力合普惠项目 OK
      WITH t4 AS
          (SELECT
              bal
          FROM
              omi.mid_mb_acct_balance
          WHERE
               prjt_name = '力合普惠项目'
               AND etl_date = '"||v_etl_date||"')
      t5 AS
          (SELECT
              sum(bal) / 365 AS av_bal
          FROM
              omi.mid_mb_acct_balance
          WHERE
               prjt_name = '力合普惠项目'
               AND etl_date > '"||first_day_month||"'
               AND etl_date <= '"||v_etl_date||"')
      SELECT
          '贷款2' as rpt_no
           , b.prjt_name AS prjt_name
           , sum(b.service_fee) AS service_fee
           , t4.bal AS bal
           , t5.av_bal AS av_bal
           , NULL AS total_amount_prev
           , NULL AS av_amount_prev
           , NULL AS rate
           , sum(b.ln_rate) / count(b.ln_rate) AS av_ln_rate
           , '月服务费=当月偿还的正常利息/借款利率*(借款利率-8.8%)' AS remark
      FROM
          (SELECT
               prjt_name
                , CASE WHEN prdt_name = '友金普惠（二期）' THEN
                        ln_rate / 100
                        WHEN prdt_name = '友金普惠贷款产品' THEN
                        ln_rate
                   ELSE
                       NULL
                   END AS ln_rate
                , CASE WHEN prdt_name = '友金普惠（二期）' THEN
                        ((ln_rate / 100) - 0.088) / (ln_rate / 100)
                        * intst_amt
                        WHEN prdt_name = '友金普惠贷款产品' THEN
                        (ln_rate - 0.088) / ln_rate * intst_amt
                   ELSE
                       NULL
                   END AS service_fee
           FROM
               omi.mid_ln_prdt_base_info_actual
           WHERE
                prjt_name = '力合普惠项目'
                AND repay_date >= '"||first_day_month||"'
                AND repay_date < '"||v_etl_date||"') b
          , t4
          , t5
      GROUP BY
           prjt_name
           , bal
           , av_bal
       union
       --优卡分期项目 OK
       WITH t6 AS
           (SELECT
               bal
           FROM
               omi.mid_mb_acct_balance
           WHERE
                prjt_name = '优卡分期项目'
                AND etl_date = '"||v_etl_date||"')
       t7 AS
           (SELECT
               sum(bal) / 365 AS av_bal
           FROM
               omi.mid_mb_acct_balance
           WHERE
                prjt_name = '优卡分期项目'
                AND etl_date > '"||first_day_month||"' AND etl_date <= '"||v_etl_date||"')
       SELECT
           '贷款3' as rpt_no
            , c.prjt_name AS prjt_name
            , sum(c.service_fee) AS service_fee
            , t6.bal AS bal
            , t7.av_bal AS av_bal
            , NULL AS total_amount_prev
            , NULL AS av_amount_prev
            , NULL AS rate
            , sum(c.ln_rate) / count(c.ln_rate) AS av_ln_rate
            , '月服务费=当月偿还利息总额/借款利率*(借款利率-12%)' AS remark
       FROM
           (SELECT
                prjt_name
                 , ln_rate / 100 AS ln_rate
                 , intst_amt / (ln_rate / 100)
                    * ((ln_rate / 100) - 0.12) AS service_fee
            FROM
                omi.mid_ln_prdt_base_info_actual
            WHERE
                 prjt_name = '优卡分期项目'
                 AND repay_date >= '"||first_day_month||"'
                 AND repay_date < '"||v_etl_date||"') c
           , t6
           , t7
       GROUP BY
            prjt_name
            , bal
            , av_bal
        UNION
        SELECT
            '贷款4' as rpt_no
             , prjt_name
             , sum((real_rate / 100 - 0.08) * total_amount_prev / 360
                    * (1 - 0.06)) AS SERVICE_FEE
             , sum(total_amount_prev) AS bal
             , sum(total_amount_prev) / 365 AS av_bal
             , NULL AS total_amount_prev
             , NULL AS av_amount_prev
             , NULL AS rate
             , sum(real_rate / 100) / count(real_rate) AS av_ln_rate
             , '众邦当期应收服务费=（实际对客利率-合作方资金定价）*合作方在该笔贷款的承贷金额*承贷金额实际占用天数/360*(1-6%)。  对客利率=贷款实际利率 X=8 甲方承贷金额=贷款余额 实际占用天数=1.当月发放当月未结清的贷款：发放日至月末最后一日 2.当月发放当月结清的贷款：结清日-发放日 3.以往月发放当月未结清的贷款：当月实际天数 4.以往月发放当月结清的贷款：结清日-月初第一日' AS remark
        FROM
            omi.orc_mb_acc_int_det_spl
        WHERE
             etl_date > '"||first_day_month||"'
             AND etl_date <= '"||v_etl_date||"'
        GROUP BY
             prjt_name
        UNION
        WITH t8 AS
            (SELECT
                bal
            FROM
                omi.mid_mb_acct_balance
            WHERE
                 prjt_name = '沣邦汽车（经营）'
                 AND etl_date = '"||v_etl_date||"')
        t9 AS
            (SELECT
                sum(bal) / 365 AS av_bal
            FROM
                omi.mid_mb_acct_balance
            WHERE
                 prjt_name = '沣邦汽车（经营）'
                 AND etl_date = '"||first_day_month||"'
                 AND etl_date <= '"||v_etl_date||"')
        SELECT
            '贷款5' as rpt_no
             , a.prjt_name AS prjt_name
             , sum(a.service_fee) AS service_fee
             , t8.bal AS bal --贷款余额
             , t9.av_bal AS av_bal
             , NULL AS total_amount_prev
             , NULL AS av_amount_prev
             , NULL AS rate
             , sum(a.ln_rate) / count(a.ln_rate) AS av_ln_rate
             , '按月付费，每月付费金额=本月服务费+本月附加服务费 服务费=(借款利率-8%)X当月偿还的正常利息/借款利率 附加服务费=0.375X当月偿还的罚息（含复利）' AS remark
        FROM
            (SELECT
                 prjt_name
                  , ln_rate / 100 AS ln_rate
                  , (((ln_rate / 100) - 0.08) * intst_amt
                      / (ln_rate / 100)
                      + 0.375 * (pun_intst_amt + comp_intst_amt)) AS service_fee
             FROM
                 omi.mid_ln_prdt_base_info_actual
             WHERE
                  prjt_name = '沣邦汽车（经营）'
                  AND repay_date >= '"||first_day_month||"'
                  AND repay_date < '"||v_etl_date||"') a
            , t8
            , t9
        GROUP BY
             prjt_name
             , bal
             , av_bal)
	";
     EXECUTE IMMEDIATE (v_sql);
     str_msg :="成功执行";
     put_line(""||str_msg||":"||str_procname||"");
	 omi.pro_orc_sys_log(1,str_procname,v_etl_date,str_msg);
     put_line("月结合作方服务费统计表数据加载中间过程({pro_name}:pro_rpt_partner_fee_month2 {beg_month}:"||beg_month ||")  is ok");
  END;
/

create or replace PROCEDURE omi.pro_rpt_partner_fee_quart(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：季结合作方服务费统计表数据加载中间过程
执行周期：每个季度初2号执行
功能描述：季结合作方服务费统计表
编写人： dingkai
编写日期：2019-9-19
修改：2019-10-17 理财魔方逻辑修改
**************************************************************************************************/
--过程变量
    v_etl_date STRING;
    beg_qua STRING;
    dif_days INT;
    --second_day_quart STRING;
    v_sql STRING;
    v_delete_data STRING;
    first_day_quart STRING;
	str_procname STRING;
	str_etl_date STRING;
	str_msg STRING;
	lc_beg_qua STRING;
BEGIN
	--0   初始化
	str_procname :="omi.pro_rpt_partner_fee_quart";
	str_msg :="开始执行";
	put_line(""||str_msg||":"||str_procname||"");
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
	--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     --理财魔方参数处理
     --上个季度
     --lc_beg_qua := CONCAT(YEAR(STR_TO_DATE(to_char(omi.fun_get_date(i_etl_date,'LQB'),'yyyyMMdd'))),'Q',QUARTER(to_char(omi.fun_get_date(i_etl_date,'LQB'),'yyyyMMdd'), 'yyyyMMdd'));
     --本季度
     beg_qua :=CONCAT(YEAR(STR_TO_DATE(i_etl_date)),'Q',QUARTER(i_etl_date,'yyyyMMdd'));
     --上季天数
	 dif_days :=datediff(omi.fun_get_date(i_etl_date,'QB'),omi.fun_get_date(i_etl_date,'LQB'))+1;
     --上季第二天
	 --second_day_quart := to_char((omi.fun_get_date(to_char(omi.fun_get_date(i_etl_date,'LQB'),'yyyyMMdd'),'QB')+1),'yyyyMMdd');
     --本季初
     first_day_quart := to_char(omi.fun_get_date(i_etl_date,'QB'),'yyyyMMdd');
     
     --put_line("上个季度:'"||lc_beg_qua||"'");
     put_line("本季度初:'"||first_day_quart||"'");
     --put_line("上个季度第二天:'"||second_day_quart||"'");
     put_line("本季度天数:'"||dif_days||"'");
	 --1.0 读取配置表信息
     --2.0 删除数据
     
     v_delete_data := "DELETE FROM omi.rpt_partner_fee_quart WHERE count_qua='"|| beg_qua ||"'";
     EXECUTE IMMEDIATE (v_delete_data);
     --3.0 加载数据
     v_sql := "WITH t1 AS
    (SELECT
        prjt_name
         , sum(total_amount_prev) / 365 AS AVQY_AMOUNT
         , sum(total_amount_prev) / "|| dif_days ||" AS AVDY_AMOUNT
    FROM
        omi.mid_mb_acct_balance
    WHERE
         etl_date BETWEEN '"|| first_day_quart ||"' AND '"|| v_etl_date ||"' AND prjt_name='理财魔方项目' GROUP BY prjt_name) --理财魔方
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
                  t1.AVQY_AMOUNT * 0.001
                  WHEN t1.AVDY_AMOUNT > 100000000
                        AND t1.AVDY_AMOUNT <= 200000000 THEN
                  t1.AVQY_AMOUNT * 0.0015
                  WHEN t1.AVDY_AMOUNT > 200000000
                        AND t1.AVDY_AMOUNT <= 300000000 THEN
                  t1.AVQY_AMOUNT * 0.002
                  WHEN t1.AVDY_AMOUNT > 300000000
                        AND t1.AVDY_AMOUNT <= 500000000 THEN
                  t1.AVQY_AMOUNT * 0.0025
                  WHEN t1.AVDY_AMOUNT > 500000000 THEN
                  t1.AVQY_AMOUNT * 0.003
             ELSE
                 NULL
             END AS SERVICE_FEE
          , NULL AS DUE_AMT
          , t1.AVDY_AMOUNT AS AVDY_AMOUNT
          , '"|| v_etl_date ||"' AS UPDT_DATE
     FROM
         t1
      )";
     --put_line("SQL:'"||v_sql||"'");
     EXECUTE IMMEDIATE (v_sql);
	 str_msg :="成功执行";
	 put_line(""||str_msg||":"||str_procname||"");
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("({pro_name}:pro_rpt_partner_fee_quart {beg_qua}:'"|| beg_qua ||"')  is ok");
END;
/

create or replace PROCEDURE omi.pro_orc_mb_acc_int_det_spl(i_etl_date IN STRING) IS
/**************************************************************************************************
过程中文名：众邦联合贷款中间表
执行周期：日
功能描述：加载众邦联合贷款中间表数据
编写人： dingkai
编写日期：2019-12-05
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
	str_procname :="omi.pro_orc_mb_acc_int_det_spl";
	str_msg :="开始执行";
	omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
--0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    --0.2  变量赋值
     v_etl_date := i_etl_date;
     --1.0 读取配置表信息
     --2.0 
     --3.0 清空表里面的数据
     v_delete_data := "DELETE FROM omi.orc_mb_acc_int_det_spl WHERE etl_date='"|| v_etl_date ||"'";
     EXECUTE IMMEDIATE (v_delete_data);
     --3.0 加载数据
     v_sql := "INSERT INTO omi.orc_mb_acc_int_det_spl
SELECT
    '众邦项目' AS prjt_name
    , a.internal_key
     , b.total_amount_prev
     , c.real_rate
     , '"|| v_etl_date ||"' AS etl_date
FROM
    omi.ext_mb_acct a
    INNER JOIN omi.ext_mb_acct_balance b
    ON a.internal_key = b.internal_key
    --left JOIN omi.export_table c
    left JOIN omi.ext_mb_acc_int_det_spl c
    ON a.internal_key = c.internal_key
WHERE
     a.prod_type IN ('10048', '10049')
     AND a.acct_seq_no = 1
     AND b.amt_type = 'BAL'
     AND c.int_class = 'INT'
     AND b.total_amount_prev != 0";
     EXECUTE IMMEDIATE (v_sql);
     str_msg :="成功";
	 omi.pro_orc_sys_log(1,str_procname,i_etl_date,str_msg);
     put_line("众邦联合贷款中间表({pro_name}:pro_orc_mb_acc_int_det_spl {etl_date}:"|| v_etl_date ||") is ok");
END;
/
