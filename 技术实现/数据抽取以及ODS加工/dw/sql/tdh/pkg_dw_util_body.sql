!set plsqlUseSlash true
CREATE OR REPLACE PACKAGE BODY cc.pkg_dw_util IS 
  --函数
  FUNCTION fun_get_date(p_acct_date IN DATE DEFAULT SYSDATE,
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

    --编写人：   guxn
    --编写日期： 2015-08-07 
    --修改记录： by guxn 2016-04-30 增加 LYT，LQT，LMT三种类型
    --          by guxn 2016-12-02 增加 LYD，LQD，LMD三种类型  
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

  FUNCTION fun_get_days(p_acct_date DATE DEFAULT SYSDATE, p_type STRING)
    RETURN INT IS
    /******************************************************************************
    函数中文名：特定天数获取函数
    功能描述：获取传入日期距离年初，季初，月初的天数
    说明： Y 表示 距离年初的天数
           Q 表示 距离季初的天数
           M 表示 距离月初的天数
          LY 表示 距离去年今日的天数
          LQ 表示 距离上季度今日的天数
          LM 标识 距离上月今日的天数
    编写人：guxn    
    编写日期：2016-01-15
    修改记录： by guxn 2016-04-30 增加 LY LQ LM三种类型
    ******************************************************************************/
    v_etl_date DATE;
    v_days      INT;
  BEGIN
    v_etl_date := p_acct_date;
  
    IF p_type = 'Y' THEN
      v_days := datediff(v_etl_date,
                         cc.pkg_dw_util.fun_get_date(v_etl_date, 'YB')) + 1;
    ELSIF p_type = 'Q' THEN
      v_days := datediff(v_etl_date,
                         cc.pkg_dw_util.fun_get_date(v_etl_date, 'QB')) + 1;
    ELSIF p_type = 'M' THEN
      v_days := datediff(v_etl_date,
                         cc.pkg_dw_util.fun_get_date(v_etl_date, 'MB')) + 1;
    ELSIF p_type = 'LY' THEN
      v_days := datediff(v_etl_date,
                         cc.pkg_dw_util.fun_get_date(v_etl_date, 'LYT')) + 1;
    ELSIF p_type = 'LQ' THEN
      v_days := datediff(v_etl_date,
                         cc.pkg_dw_util.fun_get_date(v_etl_date, 'LQT')) + 1;
    ELSIF p_type = 'LM' THEN
      v_days := datediff(v_etl_date,
                         cc.pkg_dw_util.fun_get_date(v_etl_date, 'LMT')) + 1;
    END IF;
    RETURN v_days;
  END;

  FUNCTION fun_get_micro_time(p_time TIMESTAMP DEFAULT systimestamp)
    RETURN STRING IS
    /******************************************************************************
    函数中文名：特定格式的毫秒时间获取函数
    功能描述：获取传入时间毫秒级别的时间
    编写人：guxn
    编写日期：2015-08-28
    ******************************************************************************/
    v_time        TIMESTAMP;
    v_string_time STRING;
  BEGIN
    v_time        := p_time;
    v_string_time := date_format(v_time, 'yyyyMMddHHmmss') ||
                     substr(STRING(v_time), 21, 3);
    RETURN v_string_time;
  END;

  FUNCTION fun_if_valid_date(p_acct_date IN STRING) RETURN INT IS
    /******************************************************************************
      函数中文名：判断日期是否有效的函数
      功能描述：判断传入的日期是否有效
      编写人：guxn
      编写日期：2016-04-27
    ******************************************************************************/
    v_etl_date STRING;
    v_tran_date STRING;
  BEGIN
    IF length(p_acct_date) = 10 THEN
      v_etl_date := regexp_replace(p_acct_date, '-', '');
    ELSIF length(p_acct_date) = 8 THEN
      v_etl_date := p_acct_date;
    ELSE
      RETURN 0;
    END IF;
  
    v_tran_date := to_char(tdh_todate(v_etl_date), 'yyyyMMdd');
  
    IF v_tran_date - v_etl_date = 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
    RETURN 1;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END;
  
   FUNCTION fun_des_acct(v_no STRING, v_type string)
RETURN string IS
/************************************************************
      函数中文名：账号、客户号类脱敏函数
      功能描述：
          a01：保留前四位,其他位置使用*做替换。
          a02：对数字做映射替换，1-8,2-9,3-4,4-7,5-1,6-3,7-2,8-5,9-x,0-6。
          a03：对数字做映射替换，1-q,2-w,3-a,4-s,5-z,6-t,7-e,8-d,9-c,0-x；
                                                 对字母做映射替换，l-1,k-2,j-3,h-4,g-5,f-6,d-7,s-8,b-9,m-0。
      编写人：guxn
      编写日期：2019-05-20
*************************************************************/
DECLARE
    out_no string;
     v_len INT;
     i INT;
     v_mid STRING;
BEGIN
    v_len := length(trim(v_no));
     IF v_len = 0 THEN
         out_no := v_no;
      ELSEIF v_type = 'a01' THEN
          out_no := substr(v_no, 1, 4);
          i := 5;
          WHILE i <= v_len LOOP
               out_no := out_no || '*';
               i := i + 1;
          END LOOP;
      ELSEIF v_type = 'a02' THEN
          i := 1;
          out_no := '';
          WHILE i <= v_len LOOP
               v_mid := (CASE WHEN
                             substr(trim(v_no), i, 1) = '0'
                         THEN
                             '6'
                         WHEN
                             substr(trim(v_no), i, 1) = '1'
                         THEN
                             '8'
                         WHEN
                             substr(trim(v_no), i, 1) = '2'
                         THEN
                             '9'
                         WHEN
                             substr(trim(v_no), i, 1) = '3'
                         THEN
                             '4'
                         WHEN
                             substr(trim(v_no), i, 1) = '4'
                         THEN
                             '7'
                         WHEN
                             substr(trim(v_no), i, 1) = '5'
                         THEN
                             '1'
                         WHEN
                             substr(trim(v_no), i, 1) = '6'
                         THEN
                             '3'
                         WHEN
                             substr(trim(v_no), i, 1) = '7'
                         THEN
                             '2'
                         WHEN
                             substr(trim(v_no), i, 1) = '8'
                         THEN
                             '5'
                         WHEN
                             substr(trim(v_no), i, 1) = '9'
                         THEN
                             'x'
                         ELSE
                             substr(trim(v_no), i, 1)
                         end);
               out_no := out_no || v_mid;
               i := i + 1;
          END LOOP;
      ELSEIF v_type = 'a03' THEN
          i := 1;
          out_no := '';
          WHILE i <= v_len LOOP
               v_mid := (CASE WHEN
                             substr(trim(v_no), i, 1) = '0'
                         THEN
                             'x'
                         WHEN
                             substr(trim(v_no), i, 1) = '1'
                         THEN
                             'q'
                         WHEN
                             substr(trim(v_no), i, 1) = '2'
                         THEN
                             'w'
                         WHEN
                             substr(trim(v_no), i, 1) = '3'
                         THEN
                             'a'
                         WHEN
                             substr(trim(v_no), i, 1) = '4'
                         THEN
                             's'
                         WHEN
                             substr(trim(v_no), i, 1) = '5'
                         THEN
                             'z'
                         WHEN
                             substr(trim(v_no), i, 1) = '6'
                         THEN
                             't'
                         WHEN
                             substr(trim(v_no), i, 1) = '7'
                         THEN
                             'e'
                         WHEN
                             substr(trim(v_no), i, 1) = '8'
                         THEN
                             'd'
                         WHEN
                             substr(trim(v_no), i, 1) = '9'
                         THEN
                             'c'
                         WHEN
                             substr(trim(v_no), i, 1) = 'l'
                         THEN
                             '1'
                         WHEN
                             substr(trim(v_no), i, 1) = 'k'
                         THEN
                             '2'
                         WHEN
                             substr(trim(v_no), i, 1) = 'j'
                         THEN
                             '3'
                         WHEN
                             substr(trim(v_no), i, 1) = 'h'
                         THEN
                             '4'
                         WHEN
                             substr(trim(v_no), i, 1) = 'g'
                         THEN
                             '5'
                         WHEN
                             substr(trim(v_no), i, 1) = 'f'
                         THEN
                             '6'
                         WHEN
                             substr(trim(v_no), i, 1) = 'd'
                         THEN
                             '7'
                         WHEN
                             substr(trim(v_no), i, 1) = 's'
                         THEN
                             '8'
                         WHEN
                             substr(trim(v_no), i, 1) = 'b'
                         THEN
                             '9'
                         WHEN
                             substr(trim(v_no), i, 1) = 'm'
                         THEN
                             '0'
                         ELSE
                             substr(trim(v_no), i, 1)
                         end);
               out_no := out_no || v_mid;
               i := i + 1;
          END LOOP;
     END if
     return out_no;
end;

 FUNCTION fun_des_cert(v_cert STRING, v_type string)
RETURN string IS
/************************************************************
      函数中文名：证件号类脱敏函数
      功能描述：
          c01:前4位不变，后面全部用*做替换。
          c02:对数字做映射替换：1-8,2-9,3-4,4-7,5-1,6-3,7-2,8-5,9-x,0-6。
          c03:对数字做映射替换：1-q,2-w,3-a,4-s,5-z,6-t,7-e,8-d,9-c,0-x；
              对字母做映射替换：l-1,k-2,j-3,h-4,g-5,f-6,d-7,s-8,b-9,m-0。
      编写人：guxn
      编写日期：2019-05-20
*************************************************************/
DECLARE
    i INT;
     out_cert string;
     v_len INT;
     v_mid STRING;
BEGIN
    v_len := length(trim(v_cert));
     IF v_len = 0 THEN
         out_cert := v_cert
      ELSEIF v_type = 'c01' THEN
          i := 4;
          out_cert := substr(trim(v_cert), 1, 4);
          WHILE i < v_len LOOP
               out_cert := out_cert || '*';
               i := i + 1;
          END LOOP;
      ELSEIF v_type = 'c02' THEN
          i := 1;
          out_cert := '';
          WHILE i <= v_len LOOP
               v_mid := (CASE WHEN
                             substr(trim(v_cert), i, 1) = '0'
                         THEN
                             '6'
                         WHEN
                             substr(trim(v_cert), i, 1) = '1'
                         THEN
                             '8'
                         WHEN
                             substr(trim(v_cert), i, 1) = '2'
                         THEN
                             '9'
                         WHEN
                             substr(trim(v_cert), i, 1) = '3'
                         THEN
                             '4'
                         WHEN
                             substr(trim(v_cert), i, 1) = '4'
                         THEN
                             '7'
                         WHEN
                             substr(trim(v_cert), i, 1) = '5'
                         THEN
                             '1'
                         WHEN
                             substr(trim(v_cert), i, 1) = '6'
                         THEN
                             '3'
                         WHEN
                             substr(trim(v_cert), i, 1) = '7'
                         THEN
                             '2'
                         WHEN
                             substr(trim(v_cert), i, 1) = '8'
                         THEN
                             '5'
                         WHEN
                             substr(trim(v_cert), i, 1) = '9'
                         THEN
                             'x'
                         ELSE
                             substr(trim(v_cert), i, 1)
                         end);
               out_cert := out_cert || v_mid;
               i := i + 1;
          END LOOP;
      ELSEIF v_type = 'c03' THEN
          i := 1;
          out_cert := '';
          WHILE i <= v_len LOOP
               v_mid := (CASE WHEN
                             substr(trim(v_cert), i, 1) = '0'
                         THEN
                             'x'
                         WHEN
                             substr(trim(v_cert), i, 1) = '1'
                         THEN
                             'q'
                         WHEN
                             substr(trim(v_cert), i, 1) = '2'
                         THEN
                             'w'
                         WHEN
                             substr(trim(v_cert), i, 1) = '3'
                         THEN
                             'a'
                         WHEN
                             substr(trim(v_cert), i, 1) = '4'
                         THEN
                             's'
                         WHEN
                             substr(trim(v_cert), i, 1) = '5'
                         THEN
                             'z'
                         WHEN
                             substr(trim(v_cert), i, 1) = '6'
                         THEN
                             't'
                         WHEN
                             substr(trim(v_cert), i, 1) = '7'
                         THEN
                             'e'
                         WHEN
                             substr(trim(v_cert), i, 1) = '8'
                         THEN
                             'd'
                         WHEN
                             substr(trim(v_cert), i, 1) = '9'
                         THEN
                             'c'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'l'
                         THEN
                             '1'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'k'
                         THEN
                             '2'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'j'
                         THEN
                             '3'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'h'
                         THEN
                             '4'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'g'
                         THEN
                             '5'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'f'
                         THEN
                             '6'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'd'
                         THEN
                             '7'
                         WHEN
                             substr(trim(v_cert), i, 1) = 's'
                         THEN
                             '8'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'b'
                         THEN
                             '9'
                         WHEN
                             substr(trim(v_cert), i, 1) = 'm'
                         THEN
                             '0'
                         ELSE
                             substr(trim(v_cert), i, 1)
                         end);
               out_cert := out_cert || v_mid;
               i := i + 1;
          END LOOP;
     END if;
     return out_cert;
end;

 FUNCTION fun_des_fund(v_fund DECIMAL , v_type string)
RETURN decimal IS
/************************************************************
      函数中文名：金额类脱敏函数
      功能描述：
          f01:统一替换为0.01。
          f02:字段值*0.75+0.33。
      编写人：guxn
      编写日期：2019-05-20
*************************************************************/
DECLARE
    out_fund decimal ;
BEGIN
    IF v_fund IS NULL THEN
         out_fund := v_fund;
      ELSEIF v_type = 'f01' then
          out_fund := 0.01;
      ELSEIF v_type = 'f02' THEN
          out_fund := v_fund * 0.75 + 0.33;
     END IF;
     RETURN out_fund;
end;

 FUNCTION fun_des_local(v_addr STRING, v_type string)
RETURN string IS
/************************************************************
      函数中文名：地址类脱敏函数
      功能描述：
          l01:保留前三位和后三位，中间用*做替换。
      编写人：guxn
      编写日期：2019-05-20
*************************************************************/
DECLARE
    out_addr string;
BEGIN
    IF length(trim(v_addr)) = 0 THEN
         out_addr := v_addr
     ELSE
          out_addr := substr(v_addr, 1, 3) || '*****'
                      || substr(v_addr, -3);
     END;
     RETURN out_addr;
end;

 FUNCTION fun_des_name(v_name STRING, v_type string)
RETURN string IS
/************************************************************
      函数中文名：名称类脱敏函数
      功能描述：
          n01:两个字的名字在名字中间增加一个“京”字；三个字的名字将中间的字替换为“京”字。
          n02:使用字段值的前三位拼接5个“京”字，再与最后四位拼接。
          n03:统一脱敏为 Beijing Private Name。
          n04:统一脱敏为 Beijing Company Name。
      编写人：guxn
      编写日期：2019-05-20
*************************************************************/
DECLARE
    out_name string;
BEGIN
    IF length(trim(v_name)) = 0 THEN
         out_name := v_name
      ELSEIF v_type = 'n01' then
          out_name := substr(trim(v_name), 1, 1) || '京'
                      || substr(trim(v_name), -1)
      ELSEIF v_type = 'n02' then
          out_name := substr(trim(v_name), 1, 3) || '京京京京京'
                      || substr(trim(v_name), -4)
      ELSEIF v_type = 'n03' then
          out_name := 'Beijing Private Name'
      ELSEIF v_type = 'n04' then
          out_name := 'Beijing Company Name'
     END IF
     RETURN out_name;
end;

 FUNCTION fun_des_rela(v_rela STRING, v_type string)
RETURN string IS
/************************************************************
      函数中文名：联系方式类脱敏函数
      功能描述：
          r01:保留前三位和后4位，中间用‘*’替换。
          r02:保留前三位和保留@后面的内容，中间的用*替换。
          r03:去掉括号，保留区号，真实电话用*替换。
          r04:保留前两位，后面用*做替换（五个*）。
          r05:全部用*屏蔽。
      编写人：guxn
      编写日期：2019-05-20
*************************************************************/
DECLARE
    out_rela string;
     v_len INT;
BEGIN
    v_len := length(trim(v_rela));
     IF v_len = 0 THEN
         out_rela := v_rela;
      ELSEIF v_type = 'r01' THEN
          out_rela := substr(trim(v_rela), 1, 3) || '****'
                      || substr(trim(v_rela), -4);
      ELSEIF v_type = 'r02' THEN
          out_rela := substr(trim(v_rela), 1, 3) || '****'
                      || substr(trim(v_rela)
                             , instr(v_rela, '@') - 1 - v_len);
      ELSEIF v_type = 'r03' THEN
          out_rela := replace(v_rela, '(', '');
          out_rela := replace(out_rela, ')', '');
          out_rela := substr(out_rela, 1, instr(out_rela, '-'))
                      || '********';
      ELSEIF v_type = 'r04' THEN
          out_rela := substr(trim(v_rela), 1, 2) || '*****';
      ELSEIF v_type = 'r05' THEN
          out_rela := '********';
     END if
     return out_rela;
end;

  PROCEDURE pro_comm_trlg(t_trlg IN cc.pkg_dw_util.r_trlg) IS
    /**************************************************************************************************
     过程中文名：通用操作-写入操作日志记录
     功能描述：数据处理类存储过程每一段sql处理完成后调用，实现登记sql的执行日志
     编写人： guxn
     编写日期：2015-07-02

     修改记录：1. 2019-02-15 by guxn 修改描述及层级
    **************************************************************************************************/
  
    v_times bigint;
    l_trlg  cc.pkg_dw_util.r_trlg;
  BEGIN
    l_trlg           := t_trlg;
    v_times          := unix_timestamp(l_trlg.begin_time);
    l_trlg.end_time  := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    INSERT INTO cc.dw_sm_trlg
      (key, system_flag, begin_time, end_time, time_cost, pro_name,
       log_object, log_action, row_count, log_code, log_desc, etl_date,
       status)
    VALUES
      (named_struct('unix_time', v_times, 'log_object', l_trlg.log_object,'log_seq', l_trlg.log_seq), 
      l_trlg.system_flag,l_trlg.begin_time, l_trlg.end_time, l_trlg.time_cost, l_trlg.pro_name,
      l_trlg.log_object, lower(l_trlg.log_action), nvl(l_trlg.row_count, 0),
      nvl(l_trlg.log_code, 0), l_trlg.log_desc, l_trlg.etl_date,l_trlg.status);
  END;
   
  PROCEDURE pro_data_his_main(i_etl_date IN DATE, i_table_name IN STRING) IS
    /*******************************************************************
     过程中文名：数据处理-按历史存储策略分类处理
     功能描述：通过判断处理策略类型分类调用处理程序
     编写人： guxn
     编写日期：2015-06-05

     修改记录： 1.2019-03-07 by guxn  修改配置表结构及对应读取规则，调用程序方式
    ********************************************************************/
  
    l_hist r_hist; --声明配置表变量组
    l_trlg r_trlg; --声明日志表变量组

    v_begin_time TIMESTAMP; --程序开始时间
    v_etl_date   DATE; --操作日期的变量
    v_table_name  STRING; --操作的表的名称   

    v_log_code   STRING; --获取返回日志代码
    v_log_desc   STRING; --获取返回日志信息

    warn_exception EXCEPTION; --警告的异常
    error_exception EXCEPTION; --错误的异常
  BEGIN
    --设置环境
    set_env('transaction.type', 'inceptor');
    --初始化参数
    v_begin_time := systimestamp;
    v_etl_date  := i_etl_date;  
    v_table_name := i_table_name; 

    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.log_object  := v_table_name||"-历史存储处理";
    l_trlg.system_flag := "HIST";
    l_trlg.pro_name    := cc.pkg_dw_util.g_pkg_name||'pro_data_his_main';
    l_trlg.log_action  := 'Begin';
    l_trlg.row_count   := 0;
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';  
 
    --读取配置表
    SELECT table_name,system_flag,table_hs_name, fields, keys, region_type,trans_type,
           hist_field, sync_type
      INTO l_hist
      FROM cc.dw_sm_hist
     WHERE table_name = v_table_name; 
 
    --读取成功，修改日志参数
    l_trlg.log_object  :=l_hist.table_hs_name;
    l_trlg.system_flag := l_hist.system_flag;
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
   
    --根据存储策略调用不同处理 
    l_trlg.begin_time := systimestamp;     
    l_trlg.log_action  := 'Proc';
    l_trlg.log_seq     := l_trlg.log_seq + 1;

    --拉链处理-全量
    IF l_hist.trans_type = 'chain_all' THEN

      l_trlg.log_desc    := l_trlg.log_object || '-拉链处理-全量';
      cc.pkg_dw_util.pro_data_chain(i_etl_date => v_etl_date, t_hist => l_hist, o_log_code => v_log_code, o_log_desc => v_log_desc); 
    --拉链处理-增量
    ELSIF l_hist.trans_type = 'chain_add' THEN 
      l_trlg.log_desc    := l_trlg.log_object || '-拉链处理-增量';
      cc.pkg_dw_util.pro_data_chain_add(i_etl_date => v_etl_date,i_table_name => v_table_name, o_log_code => v_log_code, o_log_desc => v_log_desc); 
  --增量数据插入
    ELSIF l_hist.trans_type = 'add' THEN 
      l_trlg.log_desc    := l_trlg.log_object || '-增量数据插入';
      cc.pkg_dw_util.pro_data_add(i_etl_date => v_etl_date, t_hist => l_hist, o_log_code => v_log_code, o_log_desc => v_log_desc);
  
    --无匹配的报配置错误
    ELSE 
      l_trlg.log_code := cc.pkg_dw_util.log_error_configwrong_code;
      l_trlg.log_desc := cc.pkg_dw_util.log_error_configwrong_desc;
      RAISE  error_exception;
    END IF;
  
    --判断程序执行结果 

    IF v_log_code = 0 THEN
      l_trlg.status := '1'; 
    ELSE
      l_trlg.status := '-1'; 
      l_trlg.log_code := v_log_code;
      l_trlg.log_desc := l_trlg.log_object || '历史存储处理失败，请检查错误！';
      RAISE error_exception;
    END IF;
      
      --操作日志
      l_trlg.end_time   := systimestamp;
      l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);

    --结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.begin_time := v_begin_time;
    l_trlg.log_seq    := l_trlg.log_seq + 1;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg); 

  EXCEPTION
    WHEN warn_exception THEN
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg); 
      put_line(l_trlg.log_code);
    WHEN error_exception THEN
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg); 
      put_line(l_trlg.log_code);
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      put_line(l_trlg.log_code);
  END; 
  
  PROCEDURE pro_data_chain(i_etl_date IN DATE,t_hist IN cc.pkg_dw_util.r_hist,
                           o_log_code OUT STRING, o_log_desc OUT STRING) IS
    /***************************************************************************************************
      过程中文名：数据处理-历史拉链处理
      功能描述： 
              1 历史拉链表的拉链起始日期固定名称默认为begndt，拉链终止日期为overdt；
              2.进行日期判定时为左闭右开区间，对应代码为begndt <= i_acctdt and overdt > i_acctdt；
              2 历史拉链表默认为分区表，默认增加partid字段作为分区字段，并参与拉链计算、主要用于自动全表封链和开链。
              3 除begndt,overdt外，历史表的字段为全量表的字段子集 
      说明：
      i_etl_date  传入做历史拉链的日期
      i_table_name 传入做历史拉链的表名
      编写人：    guxn
      编写日期：  2015-07-02

      修改记录：1. 2019-03-07 by guxn  支持除月分区之外其他如 不分区（自定义分区）、日分区、年分区等等分区策略
                                      输入参数由表名直接改为上层程序的拉链变量配置组
    ***************************************************************************************************/
  
    l_hist r_hist; --声明配置表变量组
    l_trlg r_trlg; --声明日志表变量组
  
    v_etl_date   DATE; --操作日期

    v_over_date   DATE; --封链日期

    v_row_count   INT; --记录数
    v_sql         STRING; --动态sql语句
    v_terms1      STRING; --where条件字符串1
    v_terms2      STRING; --where条件字符串2
    v_fields_key  STRING; --解决a,b两表共有字段
    v_place       INT; --判断多主键还是单主键
  
    v_begin_time  TIMESTAMP; --程序开始时间
    v_yester_date DATE;  --昨日日期
  
    v_yester_part STRING; --昨天日期的分区
    v_today_part  STRING; --今天日期的分区
    --多主键下分开各个主键字段
    v_str_tab cc.pkg_dw_util.str_tab;  --字符串组
  
    error_exception EXCEPTION; --声明错误的异常变量
  
  BEGIN
    --设置环境
    set_env('transaction.type', 'inceptor');
    --初始化变量 
    l_hist       := t_hist;
    v_etl_date   := i_etl_date;
    v_yester_date:= v_etl_date -1;
    v_over_date  := to_date('2099-12-31'); 
    v_begin_time :=systimestamp;
  
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.log_object  := l_hist.table_hs_name;
    l_trlg.system_flag := l_hist.system_flag;
    l_trlg.pro_name    := cc.pkg_dw_util.g_pkg_name||'.pro_data_chain';
    l_trlg.log_action  := 'Begin';
    l_trlg.row_count   := 0;
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    

    --处理主键字段
    v_place := instr(l_hist.keys, ',');
  
    IF v_place <> 0 THEN
      --如果存在多主键
      v_str_tab := split(l_hist.keys, ',');
      FOR idx IN 1 .. v_str_tab.COUNT() LOOP
        v_terms1     := v_terms1 || 'b.' || v_str_tab(idx) || '=' || 'a.' ||
                        v_str_tab(idx) || ' and ';
        v_terms2     := v_terms2 || 'm.' || v_str_tab(idx) || '=' || 'c.' ||
                        v_str_tab(idx) || ' and ';
        v_fields_key := v_fields_key || 'a.' || v_str_tab(idx) || ',';
      END LOOP;
      v_terms1     := substr(v_terms1, 1,
                             oracle_instr(v_terms1, 'and', -1, 1) - 1);
      v_terms2     := substr(v_terms2, 1,
                             oracle_instr(v_terms2, 'and', -1, 1) - 1);
      v_fields_key := substr(v_fields_key, 1,
                             oracle_instr(v_fields_key, ',', -1, 1) - 1);
    ELSE
      v_terms1     := 'b.' || l_hist.keys || ' = a.' ||
                      l_hist.keys;
      v_terms2     := 'm.' || l_hist.keys || ' = c.' ||
                      l_hist.keys;
      v_fields_key := 'a.' || l_hist.keys;
    END IF;
     

    --判断分区类型 

    IF l_hist.region_type == 'none'  THEN
        v_today_part  := 'none';
        v_yester_part := 'none'; 
    ELSE
        v_today_part  := to_char(v_etl_date, l_hist.region_type);
        v_yester_part := to_char(v_etl_date - 1, l_hist.region_type); 
    END IF;
    
    --put_line('v_terms1：'||v_terms1);
    --put_line('v_terms2：'||v_terms2);
    --put_line('v_fields_key：'||v_fields_key);
    --put_line('v_today_part：'||v_today_part);
    --put_line('v_yester_part：'||v_yester_part);
    --put_line('v_yester_date'||v_yester_date);

    --1   转换
    --1.1 转换前复原当天拉链（当天已进行过拉链处理）
    --1.1.1 判断是否已存在当天拉链数据
    v_sql := 'begin select count(*) into :row_count from :table_hs_name where begndt = :acct_date and partid = :today_part end';
    v_sql := regexp_replace(v_sql, ':table_hs_name',
                            l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':today_part',
                            "'" || v_today_part || "'");
    BEGIN
      EXECUTE IMMEDIATE (v_sql)
        USING OUT v_row_count, IN v_etl_date;
    EXCEPTION
      WHEN OTHERS THEN
        v_row_count := 0;
    END;

    IF v_row_count <> 0 THEN
      --存在当天拉链数据
      v_sql := "delete from :table_hs_name partition(partid = :today_part) where begndt = :acct_date";
    
      v_sql := regexp_replace(v_sql, ':today_part', "'" || v_today_part || "'");
      v_sql := regexp_replace(v_sql, ':table_hs_name', l_hist.table_hs_name);

      --put_line(v_sql);

      BEGIN
        l_trlg.log_desc   := '转换前删除当天拉链数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql)
          USING IN v_etl_date;
        l_trlg.row_count  := SQL%ROWCOUNT;
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
    END IF;
  
    --判断是否已存在当天封链的数据
    v_sql := 'begin select count(*) into :row_count from :table_hs_name where partid = :yester_part and overdt = :acct_date end';
    v_sql := regexp_replace(v_sql, ':table_hs_name',
                            l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':yester_part',
                            "'" || v_yester_part || "'");
    --put_line(v_sql);
    BEGIN
      EXECUTE IMMEDIATE (v_sql)
        USING OUT v_row_count, IN v_etl_date;
    EXCEPTION
      WHEN OTHERS THEN
        v_row_count := 0;
    END;
    IF v_row_count <> 0 THEN
      --存在当天封链的数据，将封链数据还原
      v_sql := "update :table_hs_name partition(partid=:yester_part) set overdt= :over_date where overdt = :acct_date";
      v_sql := regexp_replace(v_sql, ':yester_part',
                              "'" || v_yester_part || "'");
      v_sql := regexp_replace(v_sql, ':table_hs_name',
                              l_hist.table_hs_name);
      BEGIN
        l_trlg.log_desc   :=  '转换前将当天封链数据还原';
        l_trlg.log_action := 'UPDATE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.log_object := l_hist.table_hs_name;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql)
          USING v_over_date, v_etl_date;
        l_trlg.row_count := SQL%ROWCOUNT;
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
    END IF;
  
    --1.2 插入增量数据
    v_sql := "insert into table :table_hs_name partition(partid=:today_part) (begndt,overdt,:fields) select begndt,overdt,:fields from (select :acct_date as begndt,:over_date as overdt,:today_part,:fields from :table_name except select :acct_date as begndt,:over_date as overdt,:yester_part,:fields from :table_hs_name where partid = :yester_part and begndt <= :acct_yesdate and overdt > :acct_yesdate)";
    v_sql := regexp_replace(v_sql, ':yester_part',
                            "'" || v_yester_part || "'");
    v_sql := regexp_replace(v_sql, ':today_part',
                            "'" || v_today_part || "'");
    v_sql := regexp_replace(v_sql, ':table_hs_name',
                            l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':fields', l_hist.fields);
    v_sql := regexp_replace(v_sql, ':table_name', l_hist.table_name);
    --put_line(v_sql);
    BEGIN
      l_trlg.log_desc   :=  '插入增量数据';
      l_trlg.log_action := 'INSERT';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.log_object := l_hist.table_hs_name;
      l_trlg.begin_time := systimestamp;
      --put_line(v_sql)
      EXECUTE IMMEDIATE (v_sql)
        USING IN v_etl_date, v_over_date, v_etl_date, v_over_date, v_yester_date,v_yester_date;
      l_trlg.row_count := SQL%ROWCOUNT;
      l_trlg.end_time  := systimestamp;
      l_trlg.time_cost := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
  
    --1.3 变动数据的封链
    v_sql := "merge into :table_hs_name partition(partid=:yester_part) m using ( select a.begndt as begndt, :fields_key from :table_hs_name a inner join :table_hs_name b on b.partid = :today_part and b.begndt = :acct_date and :terms1 where a.begndt <= :acct_yesdate and a.overdt > :acct_yesdate and a.partid = :yester_part) c on(m.begndt = c.begndt and :terms2) when matched then update set overdt = :acct_date";
    v_sql := regexp_replace(v_sql, ':yester_part',
                            "'" || v_yester_part || "'");
    v_sql := regexp_replace(v_sql, ':today_part',
                            "'" || v_today_part || "'");
    v_sql := regexp_replace(v_sql, ':table_hs_name',
                            l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':fields_key', v_fields_key);
    v_sql := regexp_replace(v_sql, ':terms1', v_terms1);
    v_sql := regexp_replace(v_sql, ':terms2', v_terms2);
    --put_line(v_sql);
    BEGIN
      l_trlg.log_desc   :=  '变动数据的封链';
      l_trlg.log_action := 'UPDATE';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.log_object := l_hist.table_hs_name;
      l_trlg.begin_time := systimestamp;
      EXECUTE IMMEDIATE v_sql
        USING v_etl_date,v_yester_date,v_yester_date, v_etl_date;
      l_trlg.row_count := SQL%ROWCOUNT;
      l_trlg.end_time   := systimestamp;
      l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
  
    --1.4 追加数据封链（当天处理的是追加数据）
    --1.4.1 判断是否是追加数据
  
    v_sql := 'begin select count(1) into :row_count from :table_hs_name where begndt > :acct_date and partid >= :today_part end';
    v_sql := regexp_replace(v_sql, ':table_hs_name',
                            l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':today_part',
                            "'" || v_today_part || "'");
    ---put_line(v_sql);
    BEGIN
      EXECUTE IMMEDIATE (v_sql)
        USING OUT v_row_count, IN v_etl_date;
    EXCEPTION
      WHEN OTHERS THEN
        v_row_count := 0;
    END;
    IF v_row_count <> 0 THEN
      --是追加数据
      --1.4.2 对本次追加数据进行封链
    
      v_sql := 'merge into :table_hs_name partition(partid=:today_part) m using( select a.begndt as c_begndt,b.begndt as begndt, :fields_key from (select overdt, :keys, begndt from :table_hs_name where begndt <= :acct_date and overdt = :over_date and partid = :today_part) a, (select min(begndt) as begndt, :keys from :table_hs_name where begndt > :acct_date group by :keys ) b where :terms1 ) c on (:terms2 and m.begndt=c.c_begndt) when matched then update  set  overdt= c.begndt';
      v_sql := regexp_replace(v_sql, ':today_part',
                              "'" || v_today_part || "'");
      v_sql := regexp_replace(v_sql, ':table_hs_name',
                              l_hist.table_hs_name);
      v_sql := regexp_replace(v_sql, ':fields_key', v_fields_key);
      v_sql := regexp_replace(v_sql, ':keys', l_hist.keys);
      v_sql := regexp_replace(v_sql, ':terms1', v_terms1);
      v_sql := regexp_replace(v_sql, ':terms2', v_terms2);
      BEGIN
        l_trlg.log_desc   := '对追加数据的封链';
        l_trlg.log_action := 'UPDATE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.log_object := l_hist.table_hs_name;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql)
          USING v_etl_date, v_over_date, v_etl_date;
        l_trlg.row_count := SQL%ROWCOUNT;
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
    END IF;
  
    --1.5对删除数据的封链
    v_sql := 'update :table_hs_name partition(partid=:yester_part) a set overdt = :acct_date where not exists (select 1 from :table_name b  where :terms1 ) and a.begndt <= :acct_date and a.overdt > :acct_date';
    v_sql := regexp_replace(v_sql, ':yester_part',
                            "'" || v_yester_part || "'");
    v_sql := regexp_replace(v_sql, ':table_hs_name',
                            l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':table_name', l_hist.table_name);
    v_sql := regexp_replace(v_sql, ':terms1', v_terms1);
    
    --put_line(v_sql);
    BEGIN
      l_trlg.log_desc   := '对删除数据的封链';
      l_trlg.log_action := 'UPDATE';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.log_object := l_hist.table_hs_name;
      l_trlg.begin_time := systimestamp;
      EXECUTE IMMEDIATE (v_sql)
        USING v_etl_date, v_etl_date, v_etl_date;
      l_trlg.row_count := SQL%ROWCOUNT;
      l_trlg.end_time   := systimestamp;
      l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
    
    --2.1  写处理结束的日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    l_trlg.end_time   := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
  
    --所有代码执行成功，为返回值赋值
    o_log_code := cc.pkg_dw_util.log_code_ok;
    o_log_desc := cc.pkg_dw_util.log_desc_ok;
  
  EXCEPTION
    WHEN error_exception THEN
      o_log_code := l_trlg.log_code;
      o_log_desc := l_trlg.log_desc;
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      put_line(l_trlg.log_code);
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
      o_log_code      := l_trlg.log_code;
      o_log_desc      := l_trlg.log_desc;
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      put_line(l_trlg.log_code);
  END;

  PROCEDURE pro_data_add(i_etl_date IN DATE, t_hist IN cc.pkg_dw_util.r_hist,
                          o_log_code OUT STRING, o_log_desc OUT STRING) IS
    /********************************************************************************************
     过程中文名：数据处理-增量数据插入处理
     功能描述：针对流水类数据的历史存储处理，增量插入
     编写人： guxn
     编写日期：2015-07-22

     修改记录： 2019-02-28 by guxn  产品化改造  支持多种类分区策略
    ********************************************************************************************/
    l_hist  cc.pkg_dw_util.r_hist; --声明配置表变量组
    l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
  
    v_sql         STRING;
    v_etl_date   DATE;
    v_today_part STRING; 
    v_row_count  INT;
    v_begin_time TIMESTAMP; 
    
    error_exception EXCEPTION; --声明错误的异常变量

  BEGIN
    --设置环境
    set_env('transaction.type', 'inceptor');
    l_hist       := t_hist;
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;

    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.log_object  := l_hist.table_hs_name;
    l_trlg.system_flag := l_hist.system_flag;
    l_trlg.pro_name    := cc.pkg_dw_util.g_pkg_name||'.pro_data_add';
    l_trlg.log_action  := 'Begin';
    l_trlg.row_count   := 0;
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
       
    --判断分区类型 

    IF l_hist.region_type == 'none'  THEN
        v_today_part  := 'none';
    ELSE
        v_today_part  := to_char(v_etl_date, l_hist.region_type);
    END IF;
    
    --put_line('v_today_part:'||v_today_part);
    --判断是否存在当天的数据
    v_sql := 'begin select count(*) into :row_count from :table_hs_name where begndt = :acct_date and partid = :today_part end';
    v_sql := regexp_replace(v_sql, ':table_hs_name',l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':today_part', "'" || v_today_part || "'");

    BEGIN
      EXECUTE IMMEDIATE (v_sql)
        USING OUT v_row_count, IN v_etl_date;
    EXCEPTION
      WHEN OTHERS THEN
        v_row_count := 0;
    END;

    --存在当天数据时，对数据进行删除操作
    l_trlg.log_desc   :='转换前删除当天拉链数据';
    l_trlg.log_action := 'Delete';
    l_trlg.log_seq    := 1;
    l_trlg.begin_time := systimestamp;

    IF v_row_count <> 0 THEN 
      v_sql := "delete from :table_hs_name partition(partid = :today_part) where begndt = :acct_date";
      v_sql := regexp_replace(v_sql, ':today_part',"'" || v_today_part || "'");
      v_sql := regexp_replace(v_sql, ':table_hs_name',l_hist.table_hs_name);
      
      BEGIN
        EXECUTE IMMEDIATE (v_sql)  USING IN v_etl_date;
        l_trlg.row_count := SQL%ROWCOUNT;
        l_trlg.end_time    := systimestamp;
        l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg); 
      END;

    END IF;
  
    -- 插入当天的数据
    l_trlg.log_action := 'Insert';
    l_trlg.begin_time := systimestamp;

    --sql拼接
    IF l_hist.hist_field = 'none' THEN
      v_sql := 'insert into :table_hs_name partition(partid=:today_part) (begndt,overdt,:fields) select :acct_date as begndt,:next_date as overdt, :fields from :table_name';
    ELSE
      v_sql := 'insert into :table_hs_name partition(partid=:today_part) (begndt,overdt,:fields) select :acct_date as begndt,:next_date as overdt, :fields from :table_name where tdh_todate(cast(:hist_field as string)) = :acct_date';
      v_sql := regexp_replace(v_sql, ':hist_field', l_hist.hist_field);
    END IF;
    v_sql := regexp_replace(v_sql, ':today_part',"'" || v_today_part || "'");
    v_sql := regexp_replace(v_sql, ':fields', l_hist.fields);
    v_sql := regexp_replace(v_sql, ':table_hs_name',l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':table_name', l_hist.table_name);

    BEGIN
      IF l_hist.hist_field = 'none' THEN
        EXECUTE IMMEDIATE (v_sql) USING IN v_etl_date, IN v_etl_date + 1;
      ELSE
        EXECUTE IMMEDIATE (v_sql) USING IN v_etl_date, IN v_etl_date + 1, IN v_etl_date;
      END IF;
      --put_line(v_sql)
      l_trlg.end_time  := systimestamp;
      l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);      
      l_trlg.row_count := SQL%ROWCOUNT;
      l_trlg.log_desc  := l_trlg.log_object || "数据插入" || l_trlg.row_count || "条";
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);  
    END;
  
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    l_trlg.row_count  := 0;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    --执行成功返回成功代码
    o_log_code := cc.pkg_dw_util.log_code_ok;
    o_log_desc := cc.pkg_dw_util.log_desc_ok;
  
  EXCEPTION
    WHEN error_exception THEN 
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      put_line(l_trlg.log_code);
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
      o_log_code      := l_trlg.log_code;
      o_log_desc      := l_trlg.log_desc;
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      put_line(l_trlg.log_code);
  END; 

 
 PROCEDURE cc.pro_data_chain_add(i_etl_date IN DATE,i_table_name IN STRING,o_log_code OUT STRING, o_log_desc OUT STRING) IS
 /********************************************************************************************
     过程中文名：数据处理-全量数据写入
     功能描述：将历史拉链表和外表的数据合并
              1.OMI下创建orc事物表为当前表
              2.写入拉链表overdt=date'2099-12-31'的数据到新建的当前表
              3.外表left join关联历史拉链表写入当前新增的数据到当前表
     编写人： guxn
     编写日期：2019-06-17
     修改记录： by guxn @2019-06-17 新建
    ********************************************************************************************/ 
	l_hist cc.pkg_dw_util.r_hist; --声明配置表变量组
	l_trlg cc.pkg_dw_util.r_trlg; --日志变量组
	v_table_name  STRING; --源表名
	v_object_name STRING;
	v_system_flag STRING;
	v_begin_time TIMESTAMP;
    v_etl_date   DATE; --操作日期
    v_over_date   DATE; --封链日期
    
    v_sql         STRING; --动态sql语句
    v_terms1      STRING; --where条件字符串1
    v_terms2      STRING; --where条件字符串2
    v_fields_key  STRING; --解决a,b两表共有字段
    v_place       INT; --判断多主键还是单主键
    v_log_code   STRING; --获取返回日志代码
    v_log_desc   STRING; --获取返回日志信息
    --多主键下分开各个主键字段
    v_str_tab cc.pkg_dw_util.str_tab;  --字符串组
    error_exception EXCEPTION; --声明错误的异常变量
 BEGIN
	 --设置环境
	 set_env('transaction.type', 'inceptor');
	--初始化变量
    v_object_name := 'pro_data_chain_add';
    v_etl_date   := i_etl_date;
    v_table_name :=i_table_name;
    v_over_date  := to_date('2099-12-31'); 
    v_system_flag:= 'omi';
    v_begin_time := systimestamp;
    --日志变量组的初始化
   l_trlg.pro_name    := 'pro_data_chain_add';
   l_trlg.log_object  := v_table_name;
   l_trlg.system_flag := v_system_flag;
   l_trlg.log_desc   := v_object_name || '-处理开始';
   l_trlg.log_action := 'Begin';
   l_trlg.log_seq    := 0;
   l_trlg.begin_time := systimestamp;
   cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    
   --读取配置表
    SELECT 'omi.'||substr(table_name,5) table_name,system_flag,table_hs_name, fields, keys, region_type,trans_type,
           hist_field, sync_type
      INTO l_hist
      FROM cc.dw_sm_hist
     WHERE table_name = v_table_name;
     
	--处理主键字段
    v_place := instr(l_hist.keys, ',');
  
    IF v_place <> 0 THEN
      --如果存在多主键
      v_str_tab := split(l_hist.keys, ',');
      FOR idx IN 1 .. v_str_tab.COUNT() LOOP
        v_terms1     := v_terms1 || 'b.' || v_str_tab(idx) || '=' || 'a.' ||v_str_tab(idx) || ' and ';
        v_terms2     := v_terms2 || 'a.' || v_str_tab(idx) ||' is null or ';
        v_fields_key := v_fields_key || 'a.' || v_str_tab(idx) || ',';
      END LOOP;
      v_terms1     := substr(v_terms1, 1,oracle_instr(v_terms1, 'and', -1, 1) - 1);
      v_terms2     := substr(v_terms2, 1,oracle_instr(v_terms2, 'or', -1, 1) - 1);
      v_fields_key := substr(v_fields_key, 1,oracle_instr(v_fields_key, ',', -1, 1) - 1);
    ELSE
      v_terms1     := 'b.' || l_hist.keys || ' = a.' ||l_hist.keys;
      v_terms2     := 'a.' || l_hist.keys || ' is null ';
      v_fields_key := 'a.' || l_hist.keys;
    END IF;
    
    --put_line('v_terms1：'||v_terms1);
    --put_line('v_terms2：'||v_terms2);
    --put_line('v_fields_key：'||v_fields_key);
    
    --1.清空orc当前表
      v_sql := "truncate table :table_name";
      v_sql := regexp_replace(v_sql, ':table_name', l_hist.table_name);
      --put_line(v_sql);
      BEGIN
        l_trlg.log_desc   := '清空orc当前表';
        l_trlg.log_action := 'TRUNCATE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.row_count  := SQL%ROWCOUNT;
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
    
    --2插入外表表数据
    v_sql := "insert into table :table_name (:fields) select :fields from :ext_table_name";
    v_sql := regexp_replace(v_sql, ':fields', l_hist.fields);
    v_sql := regexp_replace(v_sql, ':table_name', l_hist.table_name);
    v_sql := regexp_replace(v_sql, ':ext_table_name', v_table_name);
    --put_line(v_sql);
    BEGIN
      l_trlg.log_desc   :=  '插入外表表数据';
      l_trlg.log_action := 'INSERT';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.log_object := l_hist.table_name;
      l_trlg.begin_time := systimestamp;
      EXECUTE IMMEDIATE (v_sql);
      l_trlg.row_count := SQL%ROWCOUNT;
      l_trlg.end_time  := systimestamp;
      l_trlg.time_cost := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
	
    --3外表left join关联历史拉链表写入当前新增的数据到当前表
    v_sql := "insert into table :table_name (:fields) select :fields from (select b.* from :table_hs_name b left join :ext_table_name a on :terms1 where :terms2 and b.overdt = :over_date )";
    v_sql := regexp_replace(v_sql, ':table_hs_name',l_hist.table_hs_name);
    v_sql := regexp_replace(v_sql, ':fields', l_hist.fields);
    v_sql := regexp_replace(v_sql, ':table_name', l_hist.table_name);
    v_sql := regexp_replace(v_sql, ':ext_table_name', v_table_name);
    v_sql := regexp_replace(v_sql, ':terms1', v_terms1);
    v_sql := regexp_replace(v_sql, ':terms2', v_terms2);
    --put_line(v_sql);
    BEGIN
      l_trlg.log_desc   :=  '插入增量数据';
      l_trlg.log_action := 'INSERT';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.log_object := l_hist.table_name;
      l_trlg.begin_time := systimestamp;
      EXECUTE IMMEDIATE (v_sql) USING v_over_date;
      l_trlg.row_count := SQL%ROWCOUNT;
      l_trlg.end_time  := systimestamp;
      l_trlg.time_cost := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
    
    --4调用拉链存储过程
    BEGIN
      l_trlg.log_desc   :=  '调用拉链存储过程';
      l_trlg.log_action := 'call pro_data_chain';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.log_object := l_hist.table_hs_name;
      l_trlg.begin_time := systimestamp;
      cc.pkg_dw_util.pro_data_chain(i_etl_date => v_etl_date, t_hist => l_hist, o_log_code => v_log_code, o_log_desc => v_log_desc);
      l_trlg.row_count := SQL%ROWCOUNT;
      l_trlg.end_time  := systimestamp;
      l_trlg.time_cost := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
    
    --所有代码执行成功，为返回值赋值
    o_log_code := cc.pkg_dw_util.log_code_ok;
    o_log_desc := cc.pkg_dw_util.log_desc_ok;
    
    EXCEPTION
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
      o_log_code      := l_trlg.log_code;
      o_log_desc      := l_trlg.log_desc;
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      put_line(l_trlg.log_code);
  END;
    
  PROCEDURE pro_check_unload_num(i_file_seq IN INT, i_table_name IN STRING,
                         i_etl_date IN STRING) IS
    /**************************************************************************************************
     过程中文名：供数文件记录数和数据库检查
     功能描述：供数文件记录数和数据库检查
     编写人： guxn
     编写日期：2018-05-25
    **************************************************************************************************/
    --通用变量
    l_trlg        cc.pkg_dw_util.r_trlg; --日志变量组
    v_object_name STRING; --操作对象
    error_exception EXCEPTION; --错误异常
  
    --过程变量
    v_count STRING;
    v_table_name  STRING;
    v_etl_date    STRING;
    v_sql      STRING;
    v_file_seq INT;
    v_partid   STRING;
  
    --对外配置表变量组
    l_dsa_conf r_unload;
  BEGIN
    --0   初始化
    --0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
  
    --0.2  变量赋值
    v_table_name      := i_table_name;
    v_object_name     := '对外供数检查表数据量';
    v_etl_date        := i_etl_date;
    v_file_seq        := i_file_seq;
    v_partid          := to_char(v_etl_date, 'yyyyMM')
                        
    --0.3   日志变量赋值
    l_trlg.pro_name := 'cc.pkg_dw_util.pro_dsa_text_count';
    l_trlg.log_object := 'dsa.' || v_table_name;
    l_trlg.etl_date   := systimestamp;
  
    --0.4   写日志
    l_trlg.log_desc   := v_object_name || ' 处理开始';
    l_trlg.log_action := 'Begin';
    l_trlg.log_seq    := 0;
    l_trlg.begin_time := systimestamp;
    --cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
  
    --1.0 读取配置表信息
    SELECT table_name,file_seq,separator,fields, condition, status
      INTO l_dsa_conf
      FROM cc.dw_sm_unload
     WHERE file_seq = v_file_seq
       AND table_name = v_table_name;
  
    --2.0 将数据导出到本地下面来
    BEGIN
      l_trlg.log_desc   := v_object_name || '将数据导出到HDFS下面来';
      l_trlg.log_action := 'Create';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.begin_time := systimestamp;
    
      v_sql := "begin select count(1) into :count from " ||  v_table_name ||" "|| l_dsa_conf.condition ||
               " end";
      
      --put_line(v_sql);
      EXECUTE IMMEDIATE (v_sql) using out v_count;
      put_line(v_count);
      --cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
  EXCEPTION
    WHEN error_exception THEN
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := SQLERRM();
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
  END;
  
PROCEDURE pro_check_cols_quality(i_acct_date IN DATE  , i_table_name in string) IS
/************************************************************************
  过程中文名：数据质量检查
  功能描述：  数据质量检查
  编写人：    guxn
  编写日期：  2019-05-21
  修改记录：
  *************************************************************************/
----------------------------------通用变量
    l_trlg cc.pkg_dw_util.r_trlg;
     --日志变量组
     v_object_name STRING;
     --操作对象
     error_exception EXCEPTION;
     --错误异常
---------------------------------程序变量
     v_acct_date DATE;
     v_sql string;
     v_table_name string;
     v_col_name string;
     v_rule_no string;
     v_rule_expr string;
     v_rule_desc string;
     v_row_count int;
     v_std_result string;
     v_table_name_a string;
     v_col_name_a string;
     v_partid string;
BEGIN
     --0   初始化
     --0.1  环境变量初始化
     set_env('transaction.type', 'inceptor');
     --0.2  变量赋值
     v_object_name := '数据检核';
     --0.2  初始化变量
     v_acct_date := i_acct_date;
     v_table_name := upper(i_table_name);
     v_col_name := "";
     v_table_name_a := "";
     v_col_name_a := "";
     v_partid := to_char(i_acct_date, 'yyyyMM');
     --0.3   日志变量赋值
     l_trlg.pro_name := 'cc.pkg_dw_util.pro_check_cols_quality';
     l_trlg.log_object := v_table_name;
     l_trlg.etl_date := systimestamp;
     --0.4   写日志
     l_trlg.log_desc := v_object_name || ' 处理开始';
     l_trlg.log_action := 'Begin';
     l_trlg.log_seq := 0;
     l_trlg.begin_time := systimestamp;
     cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);

     --v_sql赋值
     v_sql := "begin select count(1) into :row_count from "
              || v_table_name
              --|| "_hs where begndt<=:acct_date and overdt>:acct_date and partid=:partid end";
              || " where begndt<=:acct_date and overdt>:acct_date end";
     EXECUTE IMMEDIATE (v_sql)
      --using out v_row_count, v_acct_date, v_partid;
      using out v_row_count, v_acct_date;
     v_sql := "insert into cc.dw_sm_check_result(key,check_date,table_name,col_name,rule_no,rule_desc,std_result,table_row_count,check_incf_row_count) select named_struct('check_date',check_date,'table_name',table_name,'col_name',col_name,'rule_no',rule_no),check_date,table_name,col_name,rule_no,rule_desc,std_result,table_row_count,nvl(check_incf_row_count,0) from (";
     for idx in (select
                     a.*
                      , b.rule_expr
                      , b.rule_desc
                 from
                     cc.dw_sm_check_cols_rule a
                     inner join cc.dw_sm_check_quality_rules b
                     on a.rule_no = b.rule_no
                 where
                      a.table_name = v_table_name) loop
          v_col_name := idx.col_name;
          v_rule_no := idx.rule_no;
          v_rule_expr := idx.rule_expr;
          v_rule_desc := idx.rule_desc;
          v_std_result := idx.std_result;
          v_sql := v_sql || "select date '" || v_acct_date
                   || "' as check_date,cast('" || v_table_name
                   || "' AS STRING) as table_name,cast('"
                   || v_col_name || "' AS STRING) as col_name,cast('"
                   || v_rule_no || "' AS STRING) as rule_no,cast('"
                   || v_rule_desc || "' AS STRING) as rule_desc,cast("
                   || v_std_result || " as float) as std_result,"
                   || v_row_count || " as table_row_count,";
          if v_rule_no = 'N0001' then
              v_sql := v_sql || " sum(case when " || v_col_name
                        || " is null then 1 else 0 end) as check_incf_row_count from "
                        || v_table_name
                        || " where begndt<=:acct_date and overdt>:acct_date and partid=:partid union ";
           elsif v_rule_no = 'N0002' then
               v_sql := v_sql || " sum(case when " || v_col_name
                        || " is not null then 1 else 0 end) as check_incf_row_count from "
                        || v_table_name
                        || " where begndt<=:acct_date and overdt>:acct_date and partid=:partid union ";
           elsif v_rule_no like 'L%' then
               v_sql := v_sql || "(" || v_row_count
                        || " - sum(case when length(" || v_col_name || ") "
                        || substr(v_rule_expr, 7)
                        || " then 1 else 0 end)) as check_incf_row_count from "
                        || v_table_name
                        || " where begndt<=:acct_date and overdt>:acct_date and partid=:partid union ";
           elsif v_rule_no like 'R%' then
               v_sql := v_sql || "(" || v_row_count || " - sum(case when "
                        || v_col_name || " " || v_rule_expr
                        || " then 1 else 0 end)) as check_incf_row_count from "
                        || v_table_name
                        || " where begndt<=:acct_date and overdt>:acct_date and partid=:partid union ";
           elsif v_rule_no like 'V%' then
               v_sql := v_sql || "(" || v_row_count || " - sum(case when "
                        || v_rule_expr
                        || " then 1 else 0 end)) as check_incf_row_count from "
                        || v_table_name
                        || " where begndt<=:acct_date and overdt>:acct_date and partid=:partid union ";
               v_sql := regexp_replace(v_sql, ':col1', v_col_name);
           elsif v_rule_no like 'D%' then
               v_sql := v_sql || " count(1) as check_incf_row_count from "
                        || v_table_name
                        || " a where begndt<=:acct_date and overdt>:acct_date and partid=:partid 
                           and not exists (select std_dict_value from fmi.f_cm_df_std_dict b where a."
                        || v_col_name || "=b.std_dict_type) union ";
           elsif v_rule_no like 'A%' then
               v_table_name_a := substr(v_rule_expr,oracle_instr(v_rule_expr, '.', 1) - 1);
               v_col_name_a := substr(v_rule_expr,oracle_instr(v_rule_expr, '.', 1) + 1);
               v_sql := v_sql
                             || " count(1) as check_incf_row_count from "
                             || v_table_name
                             || " a where begndt<=:acct_date and overdt>:acct_date and partid=:partid and not exists(select 1 from "
                             || v_table_name_a
                             || " b where begndt<=:acct_date and overdt>:acct_date and partid=:partid and  a."
                             || v_col_name || " = b." || v_col_name_a
                             || ") union ";
          END IF;
     end loop;
     
     v_sql:=regexp_replace(v_sql,':acct_date',"'"||v_acct_date||"'");
     v_sql:=regexp_replace(v_sql,':partid',"'"||v_partid||"'");
     v_sql:=substr(v_sql,1,length(v_sql)-7)||")";
     
     --put_line(v_sql);

     -- 删除当日处理过数据
     delete from cc.dw_sm_check_result where table_name=v_table_name and check_date=v_acct_date;
  
     --执行插入
     EXECUTE IMMEDIATE (v_sql) ;
  
     --更新校验结果和差值校验
     update cc.dw_sm_check_result set check_result=check_incf_row_count/table_row_count,check_diff=round((check_incf_row_count/table_row_count)*100 - std_result*100,2) where table_name=v_table_name and check_date=v_acct_date ;
  
     --更新错误级别
     update cc.dw_sm_check_result set error_level=(case when check_incf_row_count =0 then '正常' when check_diff<=0 then '正常' when check_diff>0 and check_diff<=20 then '低' when check_diff>20 and check_diff<=50 then '中' when check_diff>50 and check_diff<=95 then '高' when check_diff>95 and check_diff<=100 then '失效' end) where table_name=v_table_name and check_date=v_acct_date ;
  EXCEPTION
     WHEN OTHERS THEN
         l_trlg.log_code := SQLCODE();
         l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
         RAISE error_exception;
 END;

  PROCEDURE pro_serv_dsa_unload(i_file_seq IN INT,i_table_name IN STRING,
                         i_etl_date IN STRING) IS
    /**************************************************************************************************
     过程中文名：对外供数将文本导出到本地存储过程
     功能描述：将select语句的结果通过insert overwrite的方式导出到HDFS目录下
     编写人： guxn
     编写日期：2016-11-13
    **************************************************************************************************/
    --通用变量
    l_trlg        cc.pkg_dw_util.r_trlg; --日志变量组
    v_object_name STRING; --操作对象
    error_exception EXCEPTION; --错误异常
  
    --过程变量
    v_system_flag STRING;
    v_table_name  STRING;
    v_etl_date    STRING;
  
    v_sql      STRING;
    v_file_seq INT;
    v_partid   STRING;
  
    --对外配置表变量组
    l_dsa_conf r_unload;
  BEGIN
    --0   初始化
    --0.1  环境变量初始化
    set_env('transaction.type', 'inceptor');
    set_env('mapred.reduce.tasks', '5');
  
    --0.2  变量赋值
    v_table_name      := i_table_name;
    v_object_name     := '对外供数';
    v_etl_date        := i_etl_date;
    v_file_seq        := i_file_seq;
    v_partid          := to_char(v_etl_date, 'yyyyMM');
                        
    --0.3   日志变量赋值
    l_trlg.pro_name := 'cc.pkg_dw_util.pro_serv_dsa_unload';
    l_trlg.log_object := 'dsa.' || v_table_name;
    l_trlg.etl_date   := systimestamp;
  
    --0.4   写日志
    l_trlg.log_desc   := v_object_name || ' 处理开始';
    l_trlg.log_action := 'Begin';
    l_trlg.log_seq    := 0;
    l_trlg.begin_time := systimestamp;
    --cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
  
    --1.0 读取配置表信息
    SELECT table_name,file_seq,separator,fields, condition, status
      INTO l_dsa_conf
      FROM cc.dw_sm_unload
     WHERE file_seq = v_file_seq
       AND table_name = v_table_name;
  
    --2.0 将数据导出到本地下面来
    BEGIN
      l_trlg.log_desc   := v_object_name || '将数据导出到HDFS下面来';
      l_trlg.log_action := 'Create';
      l_trlg.log_seq    := l_trlg.log_seq + 1;
      l_trlg.begin_time := systimestamp;

      v_sql := "insert overwrite directory '/dw/dsa/" ||
               v_table_name || '_' || v_file_seq ||
               "' row format delimited fields terminated by '"|| l_dsa_conf.separator ||"'  select " ||
               l_dsa_conf.fields || " from " || v_table_name ||" "||  l_dsa_conf.condition ||
               " distribute by rand()";
                   
      --put_line(v_sql);
      EXECUTE IMMEDIATE (v_sql);
      --cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    EXCEPTION
      WHEN OTHERS THEN
        l_trlg.log_code := SQLCODE();
        l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
        RAISE error_exception;
    END;
  EXCEPTION
    WHEN error_exception THEN
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := SQLERRM();
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
  END;

PROCEDURE pro_serv_view(i_table_name STRING, i_view_name string) IS
/*************************************************************************
  程序名称: pro_serv_view
  程序中文名: 创建视图
  传入参数: i_table_name-表名
          i_view_name-视图名
  编写人: guxn
  编写日期: 2019-05-15
*************************************************************************/
----------------------------------通用变量
    l_trlg cc.pkg_dw_util.r_trlg;
     --日志变量组
     v_object_name STRING;
     --操作对象
     error_exception EXCEPTION;
     --错误异常
     ---------------------------------程序变量
     v_sql STRING;
     v_map_type string;
     v_table_name string;
     v_view_name string;
     v_view_chn string;
     v_condition string;
     v_col_name string;
     v_col_chn string;
     v_map_col string;
     v_fun_meth string;
     v_des_type string;
     v_view_db_name STRING;
     v_view_tab_name STRING;
BEGIN
    v_table_name := i_table_name;
     v_view_name := i_view_name;
     --0   初始化
     --0.1  环境变量初始化
     set_env('transaction.type', 'inceptor');
     --0.2  变量赋值
     v_object_name := '创建视图';
     --0.3   日志变量赋值
     l_trlg.pro_name := 'cc.pkg_dw_util.pro_serv_view';
     l_trlg.log_object := 'v_table_name';
     l_trlg.etl_date := systimestamp;
     --0.4   写日志
     l_trlg.log_desc := v_object_name || ' 处理开始';
     l_trlg.log_action := 'Begin';
     l_trlg.log_seq := 0;
     l_trlg.begin_time := systimestamp;
     cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
     --创建视图
     v_sql := "create or replace view " || v_view_name
              || " as SELECT ";
     SELECT
         a.map_type
          , a.view_schema
          , a.view_name
     INTO
         v_map_type
         , v_view_db_name
         , v_view_tab_name
     FROM
         cc.dw_sm_serve_view a
     WHERE
          a.table_name = v_table_name
		  and a.view_schema || '.' || a.view_name = v_view_name;
     IF v_map_type = 'all' THEN
         v_sql := v_sql || '* from ' || v_table_name;
      ELSEIF v_map_type IN ('des', 'part') then
          FOR idx IN (SELECT
                          a.map_type
                           , a.table_name
                           , a.view_schema view_db_name
                           , a.view_name view_tab_name
                           , a.view_schema || '.' || a.view_name view_name
                           , a.view_chn
                           , a.condition
                           , b.col_name
                           , b.col_chn
                           , b.map_col
                           , b.fun_meth
                           , b.des_type
                      FROM
                          cc.dw_sm_serve_view a
                          JOIN cc.dw_sm_serve_view_mapping b
                          ON a.view_name = b.view_name
                      WHERE
                           a.view_schema || '.' || a.view_name = v_view_name order by b.col_no asc) LOOP
               v_map_type := idx.map_type;
               v_view_chn := idx.view_chn;
               v_condition := idx.condition;
               v_col_name := idx.col_name;
               v_col_chn := idx.col_chn;
               v_map_col := idx.map_col;
               v_fun_meth := idx.fun_meth;
               v_des_type := idx.des_type;
               v_view_db_name := idx.view_db_name;
               v_view_tab_name := idx.view_tab_name;
               IF v_fun_meth IS NULL THEN
                   v_sql := v_sql || v_col_name || ',';
               else
                    v_sql := v_sql || v_fun_meth || "(" || v_col_name || ",'"
                             || v_des_type || "') as " || v_map_col || ',';
               END IF;
          END LOOP;
          --put_line(v_sql);
          v_sql := substr(v_sql, 1, length(v_sql) - 1);
          v_sql := v_sql || ' from ' || v_table_name;
          IF v_map_type = 'part' THEN
              v_sql := v_sql || ' where ' || v_condition;
          END IF;
     END IF;
     --put_line(v_sql);
     EXECUTE IMMEDIATE (v_sql);
     --添加注释
     cc.pkg_dw_util.pro_serv_view_desc(v_view_db_name, v_view_tab_name);
EXCEPTION
     WHEN OTHERS THEN
         l_trlg.log_code := SQLCODE();
         l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
         RAISE error_exception;
END;

PROCEDURE pro_serv_view_desc(i_db_name IN STRING, i_view_name IN STRING) IS
/***************************************************************
  程序名称: pro_serv_view_desc
  程序中文名: 获取创建视图的注释,只适合单表创建的视图
  传入参数: i_db_name 库名，i_view_name 视图名
  编写人: guxn
  编写日期: 2019-01-15
  修改人：guxn
  修改时间：2019-05-21
  *************************************************************/
    v_db_name STRING;
     v_view_name STRING;
     v_count INT;
     v_sql STRING;
     v_db_tab_name STRING;
     --源表的名字
     v_place STRING;
     v_src_db_name STRING;
     -- 源表的库名
     v_src_tab_name STRING;
     --源表的表名
     v_log_code STRING;
     v_log_desc STRING;
     error_exception EXCEPTION;
BEGIN
--变量初始化
    v_db_name := i_db_name;
     v_view_name := i_view_name;
     --判断是否存在该视图
     SELECT
         COUNT(1)
     INTO
         v_count
     FROM
         system.views_v
     WHERE
          database_name = v_db_name
          AND view_name = v_view_name;
     --如果视图未找到   
     IF v_count = 0 THEN
         v_log_code := '1';
          v_log_desc := '该视图未找到';
          RAISE error_exception;
     END IF;
     --判断是否为单表
     SELECT
         COUNT(1)
     INTO
         v_count
     FROM
         system.views_v
     WHERE
          database_name = v_db_name
          AND view_name = v_view_name
          AND (lower(origin_text) LIKE '%join%'
                OR lower(origin_text) LIKE '%union%'
                OR lower(expanded_text) LIKE '%subquery%');
     IF v_count <> 0 THEN
         v_log_code := '2';
          v_log_desc := '该视图存在多表关联或子查询,需手工添加';
          RAISE error_exception;
     END IF;
     --获取视图的表名
     SELECT
         TRIM(substr(lower(origin_text)
                      , oracle_instr(lower(origin_text), 'from') + 5
                      , length(origin_text)))
     INTO
         v_db_tab_name
     FROM
         system.views_v
     WHERE
          database_name = v_db_name
          AND view_name = v_view_name;
     --判断表名中是否带上库名
     v_place := instr(v_db_tab_name, '.');
     IF v_place <> 0 THEN
         v_src_db_name := substr(v_db_tab_name
                                  , 1
                                  , oracle_instr(v_db_tab_name, '.') - 1);
          v_src_tab_name := substr(v_db_tab_name
                                   , oracle_instr(v_db_tab_name, '.') + 1
                                   , length(v_db_tab_name));
     ELSE
          v_src_db_name := v_db_name;
          v_src_tab_name := v_db_tab_name;
     END IF;
     FOR idx IN (SELECT
                     col1.column_name
                      , col1.column_type
                      , col2.commentstring
                 FROM
                     system.columns_v col1
                     JOIN system.columns_v col2
                     ON col1.column_name = col2.column_name
                         AND col1.database_name = v_db_name
                         AND col1.table_name = v_view_name
                         AND col2.database_name = v_src_db_name
                         AND col2.table_name = v_src_tab_name) LOOP
          v_sql := "alter view " || v_db_name || "." || v_view_name
                   || " change column " || idx.column_name || ' '
                   || idx.column_name || ' ' || idx.column_type
                   || " comment '" || idx.commentstring || "'";
          EXECUTE IMMEDIATE (v_sql);
     END LOOP;
EXCEPTION
     WHEN error_exception THEN
         put_line('错误代码:' || v_log_code);
         put_line('错误信息:' || v_log_desc);
     when others then
         put_line(sqlcode());
         put_line(sqlerrm());
  END;
  
  PROCEDURE pro_sync_cc_data(i_src_table IN STRING,i_tar_table IN STRING) IS
/********************************************************************************************
     过程中文名：数据同步-同步cc库数据
     功能描述：把外表数据写入hbase表
              1.获取目标表是否存在key值
              2.对有key值的表获取并拼接strct字符串
              3.获取目标表的列
              4.生成insert into语句
     编写人： guxn
     编写日期：2019-06-11
     修改记录： by guxn @2019-06-11 新建
    ********************************************************************************************/ 
	v_key_name STRING; --hbase表的key列名
	v_key_num STRING; --hbase表是否有key
	v_sql STRING; --动态sql
	v_column_name STRING; --列名
	v_src_table  STRING; --源表名
	v_tar_table  STRING; --目标表名
	v_struct STRING; --struct子句
	TYPE str_tab IS TABLE OF STRING; --String类型的嵌套表类型
	v_str_tab str_tab; --String类型的嵌套表类型
	l_trlg cc.pkg_dw_util.r_trlg; --日志变量组
	
	v_object_name STRING;
	v_system_flag STRING;
	v_begin_time TIMESTAMP;
 BEGIN
	--初始化变量
    v_object_name := 'pro_sync_cc_data';
    v_src_table:=i_src_table;
	v_tar_table:=i_tar_table;
    v_system_flag:= 'cc';
    v_begin_time := systimestamp;
    set_env('transaction.type', 'inceptor');
	
    --日志变量组的初始化
   l_trlg.pro_name    := 'pro_sync_cc_data';
   l_trlg.log_object  := v_tar_table;
   l_trlg.system_flag := v_system_flag;
   l_trlg.log_desc   := v_object_name || '-处理开始';
   l_trlg.log_action := 'Begin';
   l_trlg.log_seq    := 0;
   l_trlg.begin_time := systimestamp;
   cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);

	--获取hbase表是否有key字段
	v_sql:="BEGIN SELECT count(column_type) INTO :v_key_num FROM system.columns_v WHERE  database_name='cc' AND column_name='key' AND table_name=':v_tar_table' end";
	v_sql:=regexp_replace(v_sql,':v_tar_table',v_tar_table);
	EXECUTE IMMEDIATE (v_sql) USING OUT v_key_num;
	--put_line(v_sql);
	--如果有key获取并赋值给变量
	IF v_key_num <> 0 THEN 
	 v_sql:="BEGIN SELECT column_type INTO :v_key_name FROM system.columns_v WHERE  database_name='cc' AND column_name='key' AND table_name=':v_tar_table' end";
	 v_sql:=regexp_replace(v_sql,':v_tar_table',v_tar_table);
	 EXECUTE IMMEDIATE (v_sql) USING OUT v_key_name;
	END IF;
	--获取表的列
	v_sql:="BEGIN select column_name INTO :v_column_name from(select group_concat(cast(column_name as string),',') over(partition by table_name order by column_id) as column_name,row_number() over(partition by table_name order by column_id desc) as row_num from system.columns_v where database_name='cc' and table_name=':v_tar_table') where row_num=1 end";
	v_sql:=regexp_replace(v_sql,':v_tar_table',v_tar_table);
	EXECUTE IMMEDIATE (v_sql)USING OUT v_column_name;
	--PUT_LINE("a1-"||v_column_name);
	--PUT_LINE("a1-"||v_key_name);
	--生成insert语句
	IF length(v_key_name) <> 0 THEN
	     --替换变量中的值
		 v_key_name:=regexp_replace(v_key_name,':string','');
		 v_key_name:=regexp_replace(v_key_name,'struct<','');
		 v_key_name:=regexp_replace(v_key_name,'>','');
		 v_str_tab:=split(v_key_name,',');
		 --生成struct语句及插入语句
		FOR idx IN 1 .. v_str_tab.count() LOOP 
           v_struct := v_struct||"'"||v_str_tab(idx)||"',"||v_str_tab(idx)||',';
        END LOOP; 
        v_struct:=substr(v_struct, 1,oracle_instr(v_struct, ',', -1, 1) - 1);
        v_struct:="named_struct("||v_struct||")";
		--PUT_LINE("a2-"||v_struct);
		v_sql:="INSERT INTO "||v_tar_table||" ("||v_column_name||") SELECT "|| v_struct||" AS "||v_column_name||" FROM "||v_src_table ||";"
	ELSE
		v_sql:="INSERT INTO "||v_tar_table||" ("||v_column_name||") SELECT "||v_column_name||" FROM "||v_src_table ||";"
	END IF; 
	--PUT_LINE("a3-"||v_sql);
	EXECUTE IMMEDIATE (v_sql);
	 --日志变量组的初始化
   l_trlg.pro_name    := 'pro_sync_cc_data';
   l_trlg.log_object  := v_tar_table;
   l_trlg.system_flag := v_system_flag;
   l_trlg.log_desc   := v_object_name || '-处理结束';
   l_trlg.log_action := 'End';
   l_trlg.log_seq    := 1;
   l_trlg.end_time := systimestamp;
   cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
   
    EXCEPTION
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := SQLERRM();
      cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);  
  END;

END;
/
!set plsqlUseSlash false
