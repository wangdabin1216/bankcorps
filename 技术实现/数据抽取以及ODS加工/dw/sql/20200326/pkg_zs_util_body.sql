!set plsqlUseSlash true
CREATE OR REPLACE PACKAGE BODY omi.pkg_zs_util IS 
 PROCEDURE pro_data_zs_qykzlj_gd(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qykzlj_gd';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYKZLJ_GD WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYKZLJ_GD d
    USING (SELECT
                TARGET_QY
                 , GD
                 , CZBL_SFKG
                 , ZZKGGD
                 , SJJD
                 , JDLX
                 , TYSHXYDM_GRBSM
                 , ZCZB
                 , XYML
                 , CLRQ
            FROM
                TBO.ZS_QYKZLJ_GD) s
    ON(nvl(s.TARGET_QY,0) = nvl(d.TARGET_QY,0)
            AND nvl(s.GD,0) = nvl(d.GD,0)
            AND nvl(s.CZBL_SFKG,0) = nvl(d.CZBL_SFKG,0)
            AND nvl(s.ZZKGGD,0) = nvl(d.ZZKGGD,0)
            AND nvl(s.SJJD,0) = nvl(d.SJJD,0)
            AND nvl(s.JDLX,0) = nvl(d.JDLX,0)
            AND nvl(s.TYSHXYDM_GRBSM,0) = nvl(d.TYSHXYDM_GRBSM,0)
            AND nvl(s.ZCZB,0) = nvl(d.ZCZB,0)
            AND nvl(s.XYML,0) = nvl(d.XYML,0)
            AND nvl(s.CLRQ,0) = nvl(d.CLRQ,0))
WHEN NOT MATCHED THEN
    INSERT(TARGET_QY
                , GD
                , CZBL_SFKG
                , ZZKGGD
                , SJJD
                , JDLX
                , TYSHXYDM_GRBSM
                , ZCZB
                , XYML
                , CLRQ
                , ETL_DATE)
     VALUES(s.TARGET_QY
            , s.GD
            , s.CZBL_SFKG
            , s.ZZKGGD
            , s.SJJD
            , s.JDLX
            , s.TYSHXYDM_GRBSM
            , s.ZCZB
            , s.XYML
            , s.CLRQ
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;
 
  PROCEDURE pro_data_zs_qykzlj_dwtz(i_etl_date STRING)  IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qykzlj_dwtz';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYKZLJ_DWTZ WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYKZLJ_DWTZ d
    USING (SELECT
                TARGET_QY
                 , DWTZ
                 , CZBL_SFKG
                 , ZZKGGD
                 , SJJD
                 , JDLX
                 , TYSHXYDM_GRBSM
                 , ZCZB
                 , XYML
                 , CLRQ
            FROM
                TBO.ZS_QYKZLJ_DWTZ) s
    ON(nvl(s.TARGET_QY,0) = nvl(d.TARGET_QY,0)
            AND nvl(s.DWTZ,0) = nvl(d.DWTZ,0)
            AND nvl(s.CZBL_SFKG,0) = nvl(d.CZBL_SFKG,0)
            AND nvl(s.ZZKGGD,0) = nvl(d.ZZKGGD,0)
            AND nvl(s.SJJD,0) = nvl(d.SJJD,0)
            AND nvl(s.JDLX,0) = nvl(d.JDLX,0)
            AND nvl(s.TYSHXYDM_GRBSM,0) = nvl(d.TYSHXYDM_GRBSM,0)
            AND nvl(s.ZCZB,0) = nvl(d.ZCZB,0)
            AND nvl(s.XYML,0) = nvl(d.XYML,0)
            AND nvl(s.CLRQ,0) = nvl(d.CLRQ,0))
   WHEN NOT MATCHED THEN
    INSERT(TARGET_QY
                , DWTZ
                , CZBL_SFKG
                , ZZKGGD
                , SJJD
                , JDLX
                , TYSHXYDM_GRBSM
                , ZCZB
                , XYML
                , CLRQ
                , ETL_DATE)
     VALUES(s.TARGET_QY
            , s.DWTZ
            , s.CZBL_SFKG
            , s.ZZKGGD
            , s.SJJD
            , s.JDLX
            , s.TYSHXYDM_GRBSM
            , s.ZCZB
            , s.XYML
            , s.CLRQ
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qykzlj_sjkzr(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qykzlj_sjkzr';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYKZLJ_SJKZR WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYKZLJ_SJKZR d
    USING (SELECT
                TARGET_QY
                 , SJKZR
                 , CZBL_SFKG
                 , ZZKGGD
                 , SJJD
                 , JDLX
                 , TYSHXYDM_GRBSM
                 , ZCZB
                 , XYML
                 , CLRQ
            FROM
                TBO.ZS_QYKZLJ_SJKZR) s
    ON(nvl(s.TARGET_QY,0) = nvl(d.TARGET_QY,0)
            AND nvl(s.SJKZR,0) = nvl(d.SJKZR,0)
            AND nvl(s.CZBL_SFKG,0) = nvl(d.CZBL_SFKG,0)
            AND nvl(s.ZZKGGD,0) = nvl(d.ZZKGGD,0)
            AND nvl(s.SJJD,0) = nvl(d.SJJD,0)
            AND nvl(s.JDLX,0) = nvl(d.JDLX,0)
            AND nvl(s.TYSHXYDM_GRBSM,0) = nvl(d.TYSHXYDM_GRBSM,0)
            AND nvl(s.ZCZB,0) = nvl(d.ZCZB,0)
            AND nvl(s.XYML,0) = nvl(d.XYML,0)
            AND nvl(s.CLRQ,0) = nvl(d.CLRQ,0))
    WHEN NOT MATCHED THEN
    INSERT(TARGET_QY
                , SJKZR
                , CZBL_SFKG
                , ZZKGGD
                , SJJD
                , JDLX
                , TYSHXYDM_GRBSM
                , ZCZB
                , XYML
                , CLRQ
                , ETL_DATE)
     VALUES(s.TARGET_QY
            , s.SJKZR
            , s.CZBL_SFKG
            , s.ZZKGGD
            , s.SJJD
            , s.JDLX
            , s.TYSHXYDM_GRBSM
            , s.ZCZB
            , s.XYML
            , s.CLRQ
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qyzpzwxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qyzpzwxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYZPZWXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYZPZWXX d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , QYMC2
                 , ZPLY
                 , ZPLYBM
                 , ZWMC
                 , GZJY
                 , XLYQ
                 , GLJY
                 , ZPBM
                 , XSRS
                 , ZPRS
                 , XZFW
                 , NLFW
                 , ZWLB
                 , GZDD
                 , ZWMS
                 , GZZT
                 , HBDX
                 , XBYQ
                 , YYYQ
                 , ZYYQ
                 , FLDY
                 , SBDZ
                 , ZWFBSJ
                 , ZWGXSJ
            FROM
                TBO.ZS_QYZPZWXX) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.QYMC2,0) = nvl(d.QYMC2,0)
            AND nvl(s.ZPLY,0) = nvl(d.ZPLY,0)
            AND nvl(s.ZPLYBM,0) = nvl(d.ZPLYBM,0)
            AND nvl(s.ZWMC,0) = nvl(d.ZWMC,0)
            AND nvl(s.GZJY,0) = nvl(d.GZJY,0)
            AND nvl(s.XLYQ,0) = nvl(d.XLYQ,0)
            AND nvl(s.GLJY,0) = nvl(d.GLJY,0)
            AND nvl(s.ZPBM,0) = nvl(d.ZPBM,0)
            AND nvl(s.XSRS,0) = nvl(d.XSRS,0)
            AND nvl(s.ZPRS,0) = nvl(d.ZPRS,0)
            AND nvl(s.XZFW,0) = nvl(d.XZFW,0)
            AND nvl(s.NLFW,0) = nvl(d.NLFW,0)
            AND nvl(s.ZWLB,0) = nvl(d.ZWLB,0)
            AND nvl(s.GZDD,0) = nvl(d.GZDD,0)
            AND nvl(s.ZWMS,0) = nvl(d.ZWMS,0)
            AND nvl(s.GZZT,0) = nvl(d.GZZT,0)
            AND nvl(s.HBDX,0) = nvl(d.HBDX,0)
            AND nvl(s.XBYQ,0) = nvl(d.XBYQ,0)
            AND nvl(s.YYYQ,0) = nvl(d.YYYQ,0)
            AND nvl(s.ZYYQ,0) = nvl(d.ZYYQ,0)
            AND nvl(s.FLDY,0) = nvl(d.FLDY,0)
            AND nvl(s.SBDZ,0) = nvl(d.SBDZ,0)
            AND nvl(s.ZWFBSJ,0) = nvl(d.ZWFBSJ,0)
            AND nvl(s.ZWGXSJ,0) = nvl(d.ZWGXSJ,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , QYMC2
                , ZPLY
                , ZPLYBM
                , ZWMC
                , GZJY
                , XLYQ
                , GLJY
                , ZPBM
                , XSRS
                , ZPRS
                , XZFW
                , NLFW
                , ZWLB
                , GZDD
                , ZWMS
                , GZZT
                , HBDX
                , XBYQ
                , YYYQ
                , ZYYQ
                , FLDY
                , SBDZ
                , ZWFBSJ
                , ZWGXSJ
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.QYMC2
            , s.ZPLY
            , s.ZPLYBM
            , s.ZWMC
            , s.GZJY
            , s.XLYQ
            , s.GLJY
            , s.ZPBM
            , s.XSRS
            , s.ZPRS
            , s.XZFW
            , s.NLFW
            , s.ZWLB
            , s.GZDD
            , s.ZWMS
            , s.GZZT
            , s.HBDX
            , s.XBYQ
            , s.YYYQ
            , s.ZYYQ
            , s.FLDY
            , s.SBDZ
            , s.ZWFBSJ
            , s.ZWGXSJ
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_cpws(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_cpws';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_CPWS WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_CPWS d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , BT
                 , AH
                 , DQ
                 , AJLX
                 , ZXFY
                 , SPRQ
                 , CXCPWSXQDIDH
                 , ZWNR
            FROM
                TBO.ZS_CPWS) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.BT,0) = nvl(d.BT,0)
            AND nvl(s.AH,0) = nvl(d.AH,0)
            AND nvl(s.DQ,0) = nvl(d.DQ,0)
            AND nvl(s.AJLX,0) = nvl(d.AJLX,0)
            AND nvl(s.ZXFY,0) = nvl(d.ZXFY,0)
            AND nvl(s.SPRQ,0) = nvl(d.SPRQ,0)
            AND nvl(s.CXCPWSXQDIDH,0) = nvl(d.CXCPWSXQDIDH,0)
            AND nvl(s.ZWNR,0) = nvl(d.ZWNR,0))
     WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , BT
                , AH
                , DQ
                , AJLX
                , ZXFY
                , SPRQ
                , CXCPWSXQDIDH
                , ZWNR
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.BT
            , s.AH
            , s.DQ
            , s.AJLX
            , s.ZXFY
            , s.SPRQ
            , s.CXCPWSXQDIDH
            , s.ZWNR
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_dcdy_bgxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_dcdy_bgxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_DCDY_BGXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_DCDY_BGXX d
    USING (SELECT
                QYENTID
                 , BGRQ
                 , BGNR
                 , DJBH
            FROM
                TBO.ZS_QYCXSJ_DCDY_BGXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.BGRQ,0) = nvl(d.BGRQ,0)
            AND nvl(s.BGNR,0) = nvl(d.BGNR,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, BGRQ, BGNR, DJBH, ETL_DATE)
     VALUES(s.QYENTID
            , s.BGRQ
            , s.BGNR
            , s.DJBH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_bgxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_bgxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_BGXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_BGXX d
    USING (SELECT
                QYENTID
                 , BGHNR
                 , BGQNR
                 , GQBGRQ
                 , BGSX
            FROM
                TBO.ZS_QYCXSJ_BGXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.BGHNR,0) = nvl(d.BGHNR,0)
            AND nvl(s.BGQNR,0) = nvl(d.BGQNR,0)
            AND nvl(s.GQBGRQ,0) = nvl(d.GQBGRQ,0)
            AND nvl(s.BGSX,0) = nvl(d.BGSX,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, BGHNR, BGQNR, GQBGRQ, BGSX, ETL_DATE)
     VALUES(s.QYENTID
            , s.BGHNR
            , s.BGQNR
            , s.GQBGRQ
            , s.BGSX
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_yzwf(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_yzwf';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_YZWF WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_YZWF d
    USING (SELECT
                QYENTID
                 , LRSJ
                 , LRYY
                 , LRZCJDJG
                 , LRZCJDWH
                 , YCSJ
                 , YCYY
                 , YCZCJDJG
                 , YCZCJDWH
            FROM
                TBO.ZS_QYCXSJ_YZWF) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.LRSJ,0) = nvl(d.LRSJ,0)
            AND nvl(s.LRYY,0) = nvl(d.LRYY,0)
            AND nvl(s.LRZCJDJG,0) = nvl(d.LRZCJDJG,0)
            AND nvl(s.LRZCJDWH,0) = nvl(d.LRZCJDWH,0)
            AND nvl(s.YCSJ,0) = nvl(d.YCSJ,0)
            AND nvl(s.YCYY,0) = nvl(d.YCYY,0)
            AND nvl(s.YCZCJDJG,0) = nvl(d.YCZCJDJG,0)
            AND nvl(s.YCZCJDWH,0) = nvl(d.YCZCJDWH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , LRSJ
                , LRYY
                , LRZCJDJG
                , LRZCJDWH
                , YCSJ
                , YCYY
                , YCZCJDJG
                , YCZCJDWH
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.LRSJ
            , s.LRYY
            , s.LRZCJDJG
            , s.LRZCJDWH
            , s.YCSJ
            , s.YCYY
            , s.YCZCJDJG
            , s.YCZCJDWH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_xzcfjbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_xzcfjbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_XZCFJBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_XZCFJBXX d
    USING (SELECT
                QYENTID
                 , ZYWFSS
                 , CFJG
                 , CFJGMC
                 , XZCFNR
                 , CFJDSQFRQ
                 , CFJDWS
                 , GSRQ
            FROM
                TBO.ZS_QYCXSJ_XZCFJBXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.ZYWFSS,0) = nvl(d.ZYWFSS,0)
            AND nvl(s.CFJG,0) = nvl(d.CFJG,0)
            AND nvl(s.CFJGMC,0) = nvl(d.CFJGMC,0)
            AND nvl(s.XZCFNR,0) = nvl(d.XZCFNR,0)
            AND nvl(s.CFJDSQFRQ,0) = nvl(d.CFJDSQFRQ,0)
            AND nvl(s.CFJDWS,0) = nvl(d.CFJDWS,0)
            AND nvl(s.GSRQ,0) = nvl(d.GSRQ,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , ZYWFSS
                , CFJG
                , CFJGMC
                , XZCFNR
                , CFJDSQFRQ
                , CFJDWS
                , GSRQ
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.ZYWFSS
            , s.CFJG
            , s.CFJGMC
            , s.XZCFNR
            , s.CFJDSQFRQ
            , s.CFJDWS
            , s.GSRQ
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_qydwtzxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_qydwtzxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_QYDWTZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_QYDWTZXX d
    USING (SELECT
                QYENTID
                 , NBID
                 , TYSHXYDM
                 , QYMC
                 , ZCH
            FROM
                TBO.ZS_QYCXSJ_NB_QYDWTZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0))
    WHEN NOT MATCHED THEN
    INSERT(QYENTID, NBID, TYSHXYDM, QYMC, ZCH, ETL_DATE)
     VALUES(s.QYENTID
            , s.NBID
            , s.TYSHXYDM
            , s.QYMC
            , s.ZCH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_qydwtzxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_qydwtzxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_QYDWTZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_QYDWTZXX d
    USING (SELECT
                QYENTID
                 , QYZSL
                 , ZXRQ
                 , CZFS
                 , RJCZBZ
                 , TYSHXYDM
                 , QYJGMC
                 , QYZT
                 , QYJGLX
                 , KYRQ
                 , CZBL
                 , DSRXM
                 , ZCZB
                 , ZCZBBZ
                 , ZCH
                 , DJJG
                 , ZCDZXZQBH
                 , DXRQ
                 , RJCZE
            FROM
                TBO.ZS_QYCXSJ_QYDWTZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.QYZSL,0) = nvl(d.QYZSL,0)
            AND nvl(s.ZXRQ,0) = nvl(d.ZXRQ,0)
            AND nvl(s.CZFS,0) = nvl(d.CZFS,0)
            AND nvl(s.RJCZBZ,0) = nvl(d.RJCZBZ,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.QYJGMC,0) = nvl(d.QYJGMC,0)
            AND nvl(s.QYZT,0) = nvl(d.QYZT,0)
            AND nvl(s.QYJGLX,0) = nvl(d.QYJGLX,0)
            AND nvl(s.KYRQ,0) = nvl(d.KYRQ,0)
            AND nvl(s.CZBL,0) = nvl(d.CZBL,0)
            AND nvl(s.DSRXM,0) = nvl(d.DSRXM,0)
            AND nvl(s.ZCZB,0) = nvl(d.ZCZB,0)
            AND nvl(s.ZCZBBZ,0) = nvl(d.ZCZBBZ,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0)
            AND nvl(s.DJJG,0) = nvl(d.DJJG,0)
            AND nvl(s.ZCDZXZQBH,0) = nvl(d.ZCDZXZQBH,0)
            AND nvl(s.DXRQ,0) = nvl(d.DXRQ,0)
            AND nvl(s.RJCZE,0) = nvl(d.RJCZE,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , QYZSL
                , ZXRQ
                , CZFS
                , RJCZBZ
                , TYSHXYDM
                , QYJGMC
                , QYZT
                , QYJGLX
                , KYRQ
                , CZBL
                , DSRXM
                , ZCZB
                , ZCZBBZ
                , ZCH
                , DJJG
                , ZCDZXZQBH
                , DXRQ
                , RJCZE
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.QYZSL
            , s.ZXRQ
            , s.CZFS
            , s.RJCZBZ
            , s.TYSHXYDM
            , s.QYJGMC
            , s.QYZT
            , s.QYJGLX
            , s.KYRQ
            , s.CZBL
            , s.DSRXM
            , s.ZCZB
            , s.ZCZBBZ
            , s.ZCH
            , s.DJJG
            , s.ZCDZXZQBH
            , s.DXRQ
            , s.RJCZE
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_qyycml(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_qyycml';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_QYYCML WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_QYYCML d
    USING (SELECT
                QYENTID
                 , QYMC
                 , QYJGLX
                 , LRSJ
                 , LRYY
                 , YCSJ
                 , YCYY
                 , ZCH
                 , TYSHXYDM
                 , YCJGMC
                 , LRJGMC
            FROM
                TBO.ZS_QYCXSJ_QYYCML) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.QYJGLX,0) = nvl(d.QYJGLX,0)
            AND nvl(s.LRSJ,0) = nvl(d.LRSJ,0)
            AND nvl(s.LRYY,0) = nvl(d.LRYY,0)
            AND nvl(s.YCSJ,0) = nvl(d.YCSJ,0)
            AND nvl(s.YCYY,0) = nvl(d.YCYY,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.YCJGMC,0) = nvl(d.YCJGMC,0)
            AND nvl(s.LRJGMC,0) = nvl(d.LRJGMC,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , QYMC
                , QYJGLX
                , LRSJ
                , LRYY
                , YCSJ
                , YCYY
                , ZCH
                , TYSHXYDM
                , YCJGMC
                , LRJGMC
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.QYMC
            , s.QYJGLX
            , s.LRSJ
            , s.LRYY
            , s.YCSJ
            , s.YCYY
            , s.ZCH
            , s.TYSHXYDM
            , s.YCJGMC
            , s.LRJGMC
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_fzjg(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_fzjg';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_FZJG WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_FZJG d
    USING (SELECT
                QYENTID
                 , FZJGMC
                 , FZJGTYSHXYDM
                 , FZJGDJJG
                 , FZJGQYZCH
            FROM
                TBO.ZS_QYCXSJ_FZJG) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.FZJGMC,0) = nvl(d.FZJGMC,0)
            AND nvl(s.FZJGTYSHXYDM,0) = nvl(d.FZJGTYSHXYDM,0)
            AND nvl(s.FZJGDJJG,0) = nvl(d.FZJGDJJG,0)
            AND nvl(s.FZJGQYZCH,0) = nvl(d.FZJGQYZCH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , FZJGMC
                , FZJGTYSHXYDM
                , FZJGDJJG
                , FZJGQYZCH
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.FZJGMC
            , s.FZJGTYSHXYDM
            , s.FZJGDJJG
            , s.FZJGQYZCH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_qyfddbrdwtzxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_qyfddbrdwtzxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_QYFDDBRDWTZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_QYFDDBRDWTZXX d
    USING (SELECT
                QYENTID
                 , ZXRQ
                 , CZFS
                 , TYSHXYDM
                 , RJCZBZ
                 , QYMC
                 , QYZT
                 , QYJGLX
                 , KYRQ
                 , CZBL
                 , DSRXM
                 , QYZSL
                 , ZCZB
                 , ZCZBBZ
                 , ZCH
                 , DJJG
                 , ZCDZXZQBH
                 , DXRQ
                 , RJCZE
            FROM
                TBO.ZS_QYCXSJ_QYFDDBRDWTZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.ZXRQ,0) = nvl(d.ZXRQ,0)
            AND nvl(s.CZFS,0) = nvl(d.CZFS,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.RJCZBZ,0) = nvl(d.RJCZBZ,0)
            AND nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.QYZT,0) = nvl(d.QYZT,0)
            AND nvl(s.QYJGLX,0) = nvl(d.QYJGLX,0)
            AND nvl(s.KYRQ,0) = nvl(d.KYRQ,0)
            AND nvl(s.CZBL,0) = nvl(d.CZBL,0)
            AND nvl(s.DSRXM,0) = nvl(d.DSRXM,0)
            AND nvl(s.QYZSL,0) = nvl(d.QYZSL,0)
            AND nvl(s.ZCZB,0) = nvl(d.ZCZB,0)
            AND nvl(s.ZCZBBZ,0) = nvl(d.ZCZBBZ,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0)
            AND nvl(s.DJJG,0) = nvl(d.DJJG,0)
            AND nvl(s.ZCDZXZQBH,0) = nvl(d.ZCDZXZQBH,0)
            AND nvl(s.DXRQ,0) = nvl(d.DXRQ,0)
            AND nvl(s.RJCZE,0) = nvl(d.RJCZE,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , ZXRQ
                , CZFS
                , TYSHXYDM
                , RJCZBZ
                , QYMC
                , QYZT
                , QYJGLX
                , KYRQ
                , CZBL
                , DSRXM
                , QYZSL
                , ZCZB
                , ZCZBBZ
                , ZCH
                , DJJG
                , ZCDZXZQBH
                , DXRQ
                , RJCZE
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.ZXRQ
            , s.CZFS
            , s.TYSHXYDM
            , s.RJCZBZ
            , s.QYMC
            , s.QYZT
            , s.QYJGLX
            , s.KYRQ
            , s.CZBL
            , s.DSRXM
            , s.QYZSL
            , s.ZCZB
            , s.ZCZBBZ
            , s.ZCH
            , s.DJJG
            , s.ZCDZXZQBH
            , s.DXRQ
            , s.RJCZE
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_fddbrqtgsrz(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_fddbrqtgsrz';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_FDDBRQTGSRZ WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_FDDBRQTGSRZ d
    USING (SELECT
                QYENTID
                 , ZXRQ
                 , TYSHXYDM
                 , QYMC
                 , QYZT
                 , QYJGLX
                 , KYRQ
                 , SFFDDBR
                 , DSRXM
                 , ZW
                 , QYZSL
                 , ZCZB
                 , ZCZBBZ
                 , ZCH
                 , DJJG
                 , ZCDZXZQBH
                 , DXRQ
            FROM
                TBO.ZS_QYCXSJ_FDDBRQTGSRZ) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.ZXRQ,0) = nvl(d.ZXRQ,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.QYZT,0) = nvl(d.QYZT,0)
            AND nvl(s.QYJGLX,0) = nvl(d.QYJGLX,0)
            AND nvl(s.KYRQ,0) = nvl(d.KYRQ,0)
            AND nvl(s.SFFDDBR,0) = nvl(d.SFFDDBR,0)
            AND nvl(s.DSRXM,0) = nvl(d.DSRXM,0)
            AND nvl(s.ZW,0) = nvl(d.ZW,0)
            AND nvl(s.QYZSL,0) = nvl(d.QYZSL,0)
            AND nvl(s.ZCZB,0) = nvl(d.ZCZB,0)
            AND nvl(s.ZCZBBZ,0) = nvl(d.ZCZBBZ,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0)
            AND nvl(s.DJJG,0) = nvl(d.DJJG,0)
            AND nvl(s.ZCDZXZQBH,0) = nvl(d.ZCDZXZQBH,0)
            AND nvl(s.DXRQ,0) = nvl(d.DXRQ,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , ZXRQ
                , TYSHXYDM
                , QYMC
                , QYZT
                , QYJGLX
                , KYRQ
                , SFFDDBR
                , DSRXM
                , ZW
                , QYZSL
                , ZCZB
                , ZCZBBZ
                , ZCH
                , DJJG
                , ZCDZXZQBH
                , DXRQ
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.ZXRQ
            , s.TYSHXYDM
            , s.QYMC
            , s.QYZT
            , s.QYJGLX
            , s.KYRQ
            , s.SFFDDBR
            , s.DSRXM
            , s.ZW
            , s.QYZSL
            , s.ZCZB
            , s.ZCZBBZ
            , s.ZCH
            , s.DJJG
            , s.ZCDZXZQBH
            , s.DXRQ
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_ccjc(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_ccjc';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_CCJC WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_CCJC d
    USING (SELECT
                QYENTID
                 , CJRQ
                 , GGMC
                 , GGRQ
                 , JCHSJGMC
                 , JCHSJGDM
                 , JCJG
                 , SJLX
            FROM
                TBO.ZS_QYCXSJ_CCJC) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.CJRQ,0) = nvl(d.CJRQ,0)
            AND nvl(s.GGMC,0) = nvl(d.GGMC,0)
            AND nvl(s.GGRQ,0) = nvl(d.GGRQ,0)
            AND nvl(s.JCHSJGMC,0) = nvl(d.JCHSJGMC,0)
            AND nvl(s.JCHSJGDM,0) = nvl(d.JCHSJGDM,0)
            AND nvl(s.JCJG,0) = nvl(d.JCJG,0)
            AND nvl(s.SJLX,0) = nvl(d.SJLX,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , CJRQ
                , GGMC
                , GGRQ
                , JCHSJGMC
                , JCHSJGDM
                , JCJG
                , SJLX
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.CJRQ
            , s.GGMC
            , s.GGRQ
            , s.JCHSJGMC
            , s.JCHSJGDM
            , s.JCJG
            , s.SJLX
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_sfxzjbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_sfxzjbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_SFXZJBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_SFXZJBXX d
    USING (SELECT
                QYENTID
                 , ZXFY
                 , XZGSTZSWH
                 , BZ
                 , BZXR
                 , GQBGID
                 , BZXRID
                 , GQSE
                 , ZT
            FROM
                TBO.ZS_QYCXSJ_SFXZJBXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.ZXFY,0) = nvl(d.ZXFY,0)
            AND nvl(s.XZGSTZSWH,0) = nvl(d.XZGSTZSWH,0)
            AND nvl(s.BZ,0) = nvl(d.BZ,0)
            AND nvl(s.BZXR,0) = nvl(d.BZXR,0)
            AND nvl(s.GQBGID,0) = nvl(d.GQBGID,0)
            AND nvl(s.BZXRID,0) = nvl(d.BZXRID,0)
            AND nvl(s.GQSE,0) = nvl(d.GQSE,0)
            AND nvl(s.ZT,0) = nvl(d.ZT,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , ZXFY
                , XZGSTZSWH
                , BZ
                , BZXR
                , GQBGID
                , BZXRID
                , GQSE
                , ZT
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.ZXFY
            , s.XZGSTZSWH
            , s.BZ
            , s.BZXR
            , s.GQBGID
            , s.BZXRID
            , s.GQSE
            , s.ZT
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_sfxzxq(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_sfxzxq';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_SFXZXQ WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_SFXZXQ d
    USING (SELECT
                QYENTID
                 , XXDJQX
                 , XXDJQX_START
                 , XXDJQX_END
                 , ZXFY
                 , BZ
                 , ZXSX
                 , SXRQ
                 , SXYY
                 , XZZXTZSWH
                 , DJQX
                 , JDRQ
                 , GQDJZT
                 , DJQSRQ
                 , DJJZRQ
                 , ZJLX
                 , BZXR
                 , BZXRLX
                 , ZXCDSWH
                 , ZZBH
                 , ZZLX
                 , BDJGQSZSCTYSHXYDM
                 , BDJGQSZSCZTMC
                 , BZXRID
                 , GSRQ
                 , BDJGQSZSCZTZCH
                 , GQSE
                 , GQSEDW
            FROM
                TBO.ZS_QYCXSJ_SFXZXQ) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.XXDJQX,0) = nvl(d.XXDJQX,0)
            AND nvl(s.XXDJQX_START,0) = nvl(d.XXDJQX_START,0)
            AND nvl(s.XXDJQX_END,0) = nvl(d.XXDJQX_END,0)
            AND nvl(s.ZXFY,0) = nvl(d.ZXFY,0)
            AND nvl(s.BZ,0) = nvl(d.BZ,0)
            AND nvl(s.ZXSX,0) = nvl(d.ZXSX,0)
            AND nvl(s.SXRQ,0) = nvl(d.SXRQ,0)
            AND nvl(s.SXYY,0) = nvl(d.SXYY,0)
            AND nvl(s.XZZXTZSWH,0) = nvl(d.XZZXTZSWH,0)
            AND nvl(s.DJQX,0) = nvl(d.DJQX,0)
            AND nvl(s.JDRQ,0) = nvl(d.JDRQ,0)
            AND nvl(s.GQDJZT,0) = nvl(d.GQDJZT,0)
            AND nvl(s.DJQSRQ,0) = nvl(d.DJQSRQ,0)
            AND nvl(s.DJJZRQ,0) = nvl(d.DJJZRQ,0)
            AND nvl(s.ZJLX,0) = nvl(d.ZJLX,0)
            AND nvl(s.BZXR,0) = nvl(d.BZXR,0)
            AND nvl(s.BZXRLX,0) = nvl(d.BZXRLX,0)
            AND nvl(s.ZXCDSWH,0) = nvl(d.ZXCDSWH,0)
            AND nvl(s.ZZBH,0) = nvl(d.ZZBH,0)
            AND nvl(s.ZZLX,0) = nvl(d.ZZLX,0)
            AND nvl(s.BDJGQSZSCTYSHXYDM,0) = nvl(d.BDJGQSZSCTYSHXYDM,0)
            AND nvl(s.BDJGQSZSCZTMC,0) = nvl(d.BDJGQSZSCZTMC,0)
            AND nvl(s.BZXRID,0) = nvl(d.BZXRID,0)
            AND nvl(s.GSRQ,0) = nvl(d.GSRQ,0)
            AND nvl(s.BDJGQSZSCZTZCH,0) = nvl(d.BDJGQSZSCZTZCH,0)
            AND nvl(s.GQSE,0) = nvl(d.GQSE,0)
            AND nvl(s.GQSEDW,0) = nvl(d.GQSEDW,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , XXDJQX
                , XXDJQX_START
                , XXDJQX_END
                , ZXFY
                , BZ
                , ZXSX
                , SXRQ
                , SXYY
                , XZZXTZSWH
                , DJQX
                , JDRQ
                , GQDJZT
                , DJQSRQ
                , DJJZRQ
                , ZJLX
                , BZXR
                , BZXRLX
                , ZXCDSWH
                , ZZBH
                , ZZLX
                , BDJGQSZSCTYSHXYDM
                , BDJGQSZSCZTMC
                , BZXRID
                , GSRQ
                , BDJGQSZSCZTZCH
                , GQSE
                , GQSEDW
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.XXDJQX
            , s.XXDJQX_START
            , s.XXDJQX_END
            , s.ZXFY
            , s.BZ
            , s.ZXSX
            , s.SXRQ
            , s.SXYY
            , s.XZZXTZSWH
            , s.DJQX
            , s.JDRQ
            , s.GQDJZT
            , s.DJQSRQ
            , s.DJJZRQ
            , s.ZJLX
            , s.BZXR
            , s.BZXRLX
            , s.ZXCDSWH
            , s.ZZBH
            , s.ZZLX
            , s.BDJGQSZSCTYSHXYDM
            , s.BDJGQSZSCZTMC
            , s.BZXRID
            , s.GSRQ
            , s.BDJGQSZSCZTZCH
            , s.GQSE
            , s.GQSEDW
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_qsxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_qsxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_QSXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_QSXX d
    USING (SELECT
                QYENTID
                 , QSFZR
                 , QSZCY
            FROM
                TBO.ZS_QYCXSJ_QSXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.QSFZR,0) = nvl(d.QSFZR,0)
            AND nvl(s.QSZCY,0) = nvl(d.QSZCY,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, QSFZR, QSZCY, ETL_DATE)
     VALUES(s.QYENTID
            , s.QSFZR
            , s.QSZCY
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_ssgpjbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_ssgpjbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_SSGPJBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_SSGPJBXX d
    USING (SELECT
                QYENTID
                 , SSGSQYJC
                 , SSCJYS
                 , SSCJYSBM
                 , SSZT
                 , ZQLBDM
                 , GPDM
            FROM
                TBO.ZS_QYCXSJ_SSGPJBXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.SSGSQYJC,0) = nvl(d.SSGSQYJC,0)
            AND nvl(s.SSCJYS,0) = nvl(d.SSCJYS,0)
            AND nvl(s.SSCJYSBM,0) = nvl(d.SSCJYSBM,0)
            AND nvl(s.SSZT,0) = nvl(d.SSZT,0)
            AND nvl(s.ZQLBDM,0) = nvl(d.ZQLBDM,0)
            AND nvl(s.GPDM,0) = nvl(d.GPDM,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , SSGSQYJC
                , SSCJYS
                , SSCJYSBM
                , SSZT
                , ZQLBDM
                , GPDM
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.SSGSQYJC
            , s.SSCJYS
            , s.SSCJYSBM
            , s.SSZT
            , s.ZQLBDM
            , s.GPDM
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_dcdy_jbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_dcdy_jbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_DCDY_JBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_DCDY_JBXX d
    USING (SELECT
                QYENTID
                 , GSRQ
                 , BDBZQSE
                 , DJBH
                 , DJRQ
                 , DJJG
                 , ZT
            FROM
                TBO.ZS_QYCXSJ_DCDY_JBXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.GSRQ,0) = nvl(d.GSRQ,0)
            AND nvl(s.BDBZQSE,0) = nvl(d.BDBZQSE,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0)
            AND nvl(s.DJRQ,0) = nvl(d.DJRQ,0)
            AND nvl(s.DJJG,0) = nvl(d.DJJG,0)
            AND nvl(s.ZT,0) = nvl(d.ZT,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , GSRQ
                , BDBZQSE
                , DJBH
                , DJRQ
                , DJJG
                , ZT
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.GSRQ
            , s.BDBZQSE
            , s.DJBH
            , s.DJRQ
            , s.DJJG
            , s.ZT
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_dcdy_zxxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_dcdy_zxxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_DCDY_ZXXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_DCDY_ZXXX d
    USING (SELECT
                QYENTID
                 , ZXRQ
                 , ZXYY
                 , DJBH
            FROM
                TBO.ZS_QYCXSJ_DCDY_ZXXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.ZXRQ,0) = nvl(d.ZXRQ,0)
            AND nvl(s.ZXYY,0) = nvl(d.ZXYY,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, ZXRQ, ZXYY, DJBH, ETL_DATE)
     VALUES(s.QYENTID
            , s.ZXRQ
            , s.ZXYY
            , s.DJBH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_dcdy_bdbzzqxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_dcdy_bdbzzqxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_DCDY_BDBZZQXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_DCDY_BDBZZQXX d
    USING (SELECT
                QYENTID
                 , LXZWJSRQ
                 , LXZWKSRQ
                 , SE
                 , DBFW
                 , BZ
                 , ZL
                 , DJBH
            FROM
                TBO.ZS_QYCXSJ_DCDY_BDBZZQXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.LXZWJSRQ,0) = nvl(d.LXZWJSRQ,0)
            AND nvl(s.LXZWKSRQ,0) = nvl(d.LXZWKSRQ,0)
            AND nvl(s.SE,0) = nvl(d.SE,0)
            AND nvl(s.DBFW,0) = nvl(d.DBFW,0)
            AND nvl(s.BZ,0) = nvl(d.BZ,0)
            AND nvl(s.ZL,0) = nvl(d.ZL,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , LXZWJSRQ
                , LXZWKSRQ
                , SE
                , DBFW
                , BZ
                , ZL
                , DJBH
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.LXZWJSRQ
            , s.LXZWKSRQ
            , s.SE
            , s.DBFW
            , s.BZ
            , s.ZL
            , s.DJBH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_dcdy_dywxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_dcdy_dywxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_DCDY_DYWXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_DCDY_DYWXX d
    USING (SELECT
                QYENTID
                 , QK
                 , MC
                 , SYQHSYQGS
                 , BZ
                 , DJBH
            FROM
                TBO.ZS_QYCXSJ_DCDY_DYWXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.QK,0) = nvl(d.QK,0)
            AND nvl(s.MC,0) = nvl(d.MC,0)
            AND nvl(s.SYQHSYQGS,0) = nvl(d.SYQHSYQGS,0)
            AND nvl(s.BZ,0) = nvl(d.BZ,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, QK, MC, SYQHSYQGS, BZ, DJBH, ETL_DATE)
     VALUES(s.QYENTID
            , s.QK
            , s.MC
            , s.SYQHSYQGS
            , s.BZ
            , s.DJBH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_dcdy_dyqrxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_dcdy_dyqrxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_DCDY_DYQRXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_DCDY_DYQRXX d
    USING (SELECT
                QYENTID
                 , DYQR_ZJHM
                 , DYQR_ZJLX
                 , SZD
                 , DYQRMC
                 , DJBH
            FROM
                TBO.ZS_QYCXSJ_DCDY_DYQRXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.DYQR_ZJHM,0) = nvl(d.DYQR_ZJHM,0)
            AND nvl(s.DYQR_ZJLX,0) = nvl(d.DYQR_ZJLX,0)
            AND nvl(s.SZD,0) = nvl(d.SZD,0)
            AND nvl(s.DYQRMC,0) = nvl(d.DYQRMC,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , DYQR_ZJHM
                , DYQR_ZJLX
                , SZD
                , DYQRMC
                , DJBH
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.DYQR_ZJHM
            , s.DYQR_ZJLX
            , s.SZD
            , s.DYQRMC
            , s.DJBH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_zyglry(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_zyglry';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_ZYGLRY WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_ZYGLRY d
    USING (SELECT
                QYENTID
                 , RYXM
                 , RYZSL
                 , ZW
                 , ZWDM
            FROM
                TBO.ZS_QYCXSJ_ZYGLRY) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.RYXM,0) = nvl(d.RYXM,0)
            AND nvl(s.RYZSL,0) = nvl(d.RYZSL,0)
            AND nvl(s.ZW,0) = nvl(d.ZW,0)
            AND nvl(s.ZWDM,0) = nvl(d.ZWDM,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, RYXM, RYZSL, ZW, ZWDM, ETL_DATE)
     VALUES(s.QYENTID
            , s.RYXM
            , s.RYZSL
            , s.ZW
            , s.ZWDM
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_sxbzxrxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_sxbzxrxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_SXBZXRXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_SXBZXRXX d
    USING (SELECT
                QYENTID
                 , NL
                 , SF
                 , FDDBR_FZRXM
                 , SFZHM_GSZCH
                 , AH
                 , AJZT
                 , ZXFY
                 , SXBZXRXWJTQX
                 , SXFLWSQDDYW
                 , TCRQ
                 , ZXYJWH
                 , ZCZXYJDW
                 , BZXRXM
                 , BZXRLXQK
                 , YLX
                 , FBSJ
                 , LASJ
                 , XB
                 , LX
                 , WLX
                 , SFZYSFZD
            FROM
                TBO.ZS_QYCXSJ_SXBZXRXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NL,0) = nvl(d.NL,0)
            AND nvl(s.SF,0) = nvl(d.SF,0)
            AND nvl(s.FDDBR_FZRXM,0) = nvl(d.FDDBR_FZRXM,0)
            AND nvl(s.SFZHM_GSZCH,0) = nvl(d.SFZHM_GSZCH,0)
            AND nvl(s.AH,0) = nvl(d.AH,0)
            AND nvl(s.AJZT,0) = nvl(d.AJZT,0)
            AND nvl(s.ZXFY,0) = nvl(d.ZXFY,0)
            AND nvl(s.SXBZXRXWJTQX,0) = nvl(d.SXBZXRXWJTQX,0)
            AND nvl(s.SXFLWSQDDYW,0) = nvl(d.SXFLWSQDDYW,0)
            AND nvl(s.TCRQ,0) = nvl(d.TCRQ,0)
            AND nvl(s.ZXYJWH,0) = nvl(d.ZXYJWH,0)
            AND nvl(s.ZCZXYJDW,0) = nvl(d.ZCZXYJDW,0)
            AND nvl(s.BZXRXM,0) = nvl(d.BZXRXM,0)
            AND nvl(s.BZXRLXQK,0) = nvl(d.BZXRLXQK,0)
            AND nvl(s.YLX,0) = nvl(d.YLX,0)
            AND nvl(s.FBSJ,0) = nvl(d.FBSJ,0)
            AND nvl(s.LASJ,0) = nvl(d.LASJ,0)
            AND nvl(s.XB,0) = nvl(d.XB,0)
            AND nvl(s.LX,0) = nvl(d.LX,0)
            AND nvl(s.WLX,0) = nvl(d.WLX,0)
            AND nvl(s.SFZYSFZD,0) = nvl(d.SFZYSFZD,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , NL
                , SF
                , FDDBR_FZRXM
                , SFZHM_GSZCH
                , AH
                , AJZT
                , ZXFY
                , SXBZXRXWJTQX
                , SXFLWSQDDYW
                , TCRQ
                , ZXYJWH
                , ZCZXYJDW
                , BZXRXM
                , BZXRLXQK
                , YLX
                , FBSJ
                , LASJ
                , XB
                , LX
                , WLX
                , SFZYSFZD
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.NL
            , s.SF
            , s.FDDBR_FZRXM
            , s.SFZHM_GSZCH
            , s.AH
            , s.AJZT
            , s.ZXFY
            , s.SXBZXRXWJTQX
            , s.SXFLWSQDDYW
            , s.TCRQ
            , s.ZXYJWH
            , s.ZCZXYJDW
            , s.BZXRXM
            , s.BZXRLXQK
            , s.YLX
            , s.FBSJ
            , s.LASJ
            , s.XB
            , s.LX
            , s.WLX
            , s.SFZYSFZD
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_bzxrxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_bzxrxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_BZXRXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_BZXRXX d
    USING (SELECT
                QYENTID
                 , NL
                 , SF
                 , SFZHM_QYZCH
                 , AH
                 , AJZT
                 , ZXFY
                 , ZXBD
                 , BZXRXM
                 , LASJ
                 , XB
                 , LX
                 , SFZYSFZD
            FROM
                TBO.ZS_QYCXSJ_BZXRXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NL,0) = nvl(d.NL,0)
            AND nvl(s.SF,0) = nvl(d.SF,0)
            AND nvl(s.SFZHM_QYZCH,0) = nvl(d.SFZHM_QYZCH,0)
            AND nvl(s.AH,0) = nvl(d.AH,0)
            AND nvl(s.AJZT,0) = nvl(d.AJZT,0)
            AND nvl(s.ZXFY,0) = nvl(d.ZXFY,0)
            AND nvl(s.ZXBD,0) = nvl(d.ZXBD,0)
            AND nvl(s.BZXRXM,0) = nvl(d.BZXRXM,0)
            AND nvl(s.LASJ,0) = nvl(d.LASJ,0)
            AND nvl(s.XB,0) = nvl(d.XB,0)
            AND nvl(s.LX,0) = nvl(d.LX,0)
            AND nvl(s.SFZYSFZD,0) = nvl(d.SFZYSFZD,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , NL
                , SF
                , SFZHM_QYZCH
                , AH
                , AJZT
                , ZXFY
                , ZXBD
                , BZXRXM
                , LASJ
                , XB
                , LX
                , SFZYSFZD
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.NL
            , s.SF
            , s.SFZHM_QYZCH
            , s.AH
            , s.AJZT
            , s.ZXFY
            , s.ZXBD
            , s.BZXRXM
            , s.LASJ
            , s.XB
            , s.LX
            , s.SFZYSFZD
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_jyzx_jbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_jyzx_jbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_JYZX_JBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_JYZX_JBXX d
    USING (SELECT
                QYENTID
                 , JYZXJG
                 , HZRQ
                 , TYSHXYDM
                 , QYMC
                 , GGKSRQ
                 , GGJZRQ
                 , ZCH
                 , DJJG
                 , ZCDZXZQBH
            FROM
                TBO.ZS_QYCXSJ_JYZX_JBXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.JYZXJG,0) = nvl(d.JYZXJG,0)
            AND nvl(s.HZRQ,0) = nvl(d.HZRQ,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.GGKSRQ,0) = nvl(d.GGKSRQ,0)
            AND nvl(s.GGJZRQ,0) = nvl(d.GGJZRQ,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0)
            AND nvl(s.DJJG,0) = nvl(d.DJJG,0)
            AND nvl(s.ZCDZXZQBH,0) = nvl(d.ZCDZXZQBH,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , JYZXJG
                , HZRQ
                , TYSHXYDM
                , QYMC
                , GGKSRQ
                , GGJZRQ
                , ZCH
                , DJJG
                , ZCDZXZQBH
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.JYZXJG
            , s.HZRQ
            , s.TYSHXYDM
            , s.QYMC
            , s.GGKSRQ
            , s.GGJZRQ
            , s.ZCH
            , s.DJJG
            , s.ZCDZXZQBH
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_glsxbzxrxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_glsxbzxrxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_GLSXBZXRXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_GLSXBZXRXX d
    USING (SELECT
                QYENTID
                 , NL
                 , SF
                 , FDDBR
                 , SFZHM
                 , AH
                 , AJZT
                 , ZXFY
                 , SXBZXRXWJTQX
                 , SXFLWSQDDYW
                 , TCRQ
                 , ZXYJWH
                 , ZCZXYJDW
                 , BZXRXM
                 , BZXRLXQK
                 , YLX
                 , FBSJ
                 , LASJ
                 , XB
                 , LX
                 , WLX
                 , SFZYSFZD
            FROM
                TBO.ZS_QYCXSJ_GLSXBZXRXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NL,0) = nvl(d.NL,0)
            AND nvl(s.SF,0) = nvl(d.SF,0)
            AND nvl(s.FDDBR,0) = nvl(d.FDDBR,0)
            AND nvl(s.SFZHM,0) = nvl(d.SFZHM,0)
            AND nvl(s.AH,0) = nvl(d.AH,0)
            AND nvl(s.AJZT,0) = nvl(d.AJZT,0)
            AND nvl(s.ZXFY,0) = nvl(d.ZXFY,0)
            AND nvl(s.SXBZXRXWJTQX,0) = nvl(d.SXBZXRXWJTQX,0)
            AND nvl(s.SXFLWSQDDYW,0) = nvl(d.SXFLWSQDDYW,0)
            AND nvl(s.TCRQ,0) = nvl(d.TCRQ,0)
            AND nvl(s.ZXYJWH,0) = nvl(d.ZXYJWH,0)
            AND nvl(s.ZCZXYJDW,0) = nvl(d.ZCZXYJDW,0)
            AND nvl(s.BZXRXM,0) = nvl(d.BZXRXM,0)
            AND nvl(s.BZXRLXQK,0) = nvl(d.BZXRLXQK,0)
            AND nvl(s.YLX,0) = nvl(d.YLX,0)
            AND nvl(s.FBSJ,0) = nvl(d.FBSJ,0)
            AND nvl(s.LASJ,0) = nvl(d.LASJ,0)
            AND nvl(s.XB,0) = nvl(d.XB,0)
            AND nvl(s.LX,0) = nvl(d.LX,0)
            AND nvl(s.WLX,0) = nvl(d.WLX,0)
            AND nvl(s.SFZYSFZD,0) = nvl(d.SFZYSFZD,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , NL
                , SF
                , FDDBR
                , SFZHM
                , AH
                , AJZT
                , ZXFY
                , SXBZXRXWJTQX
                , SXFLWSQDDYW
                , TCRQ
                , ZXYJWH
                , ZCZXYJDW
                , BZXRXM
                , BZXRLXQK
                , YLX
                , FBSJ
                , LASJ
                , XB
                , LX
                , WLX
                , SFZYSFZD
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.NL
            , s.SF
            , s.FDDBR
            , s.SFZHM
            , s.AH
            , s.AJZT
            , s.ZXFY
            , s.SXBZXRXWJTQX
            , s.SXFLWSQDDYW
            , s.TCRQ
            , s.ZXYJWH
            , s.ZCZXYJDW
            , s.BZXRXM
            , s.BZXRLXQK
            , s.YLX
            , s.FBSJ
            , s.LASJ
            , s.XB
            , s.LX
            , s.WLX
            , s.SFZYSFZD
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_glbzxrxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_glbzxrxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_GLBZXRXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_GLBZXRXX d
    USING (SELECT
                QYENTID
                 , NL
                 , SF
                 , SFZHM_QYZCH
                 , AH
                 , AJZT
                 , ZXFY
                 , ZXBD
                 , BZXRXM
                 , LASJ
                 , XB
                 , LX
                 , SFZYSFZD
            FROM
                TBO.ZS_QYCXSJ_GLBZXRXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NL,0) = nvl(d.NL,0)
            AND nvl(s.SF,0) = nvl(d.SF,0)
            AND nvl(s.SFZHM_QYZCH,0) = nvl(d.SFZHM_QYZCH,0)
            AND nvl(s.AH,0) = nvl(d.AH,0)
            AND nvl(s.AJZT,0) = nvl(d.AJZT,0)
            AND nvl(s.ZXFY,0) = nvl(d.ZXFY,0)
            AND nvl(s.ZXBD,0) = nvl(d.ZXBD,0)
            AND nvl(s.BZXRXM,0) = nvl(d.BZXRXM,0)
            AND nvl(s.LASJ,0) = nvl(d.LASJ,0)
            AND nvl(s.XB,0) = nvl(d.XB,0)
            AND nvl(s.LX,0) = nvl(d.LX,0)
            AND nvl(s.SFZYSFZD,0) = nvl(d.SFZYSFZD,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , NL
                , SF
                , SFZHM_QYZCH
                , AH
                , AJZT
                , ZXFY
                , ZXBD
                , BZXRXM
                , LASJ
                , XB
                , LX
                , SFZYSFZD
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.NL
            , s.SF
            , s.SFZHM_QYZCH
            , s.AH
            , s.AJZT
            , s.ZXFY
            , s.ZXBD
            , s.BZXRXM
            , s.LASJ
            , s.XB
            , s.LX
            , s.SFZYSFZD
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_gdjczxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_gdjczxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_GDJCZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_GDJCZXX d
    USING (SELECT
                QYENTID
                 , SJCZE
                 , CZRQ
                 , CZFS
                 , CZFSDM
                 , TYSHXYDM
                 , RJCZBZDM
                 , CZBL
                 , GDLX
                 , GDLXDM
                 , ZCZBBZ
                 , ZCH
                 , GDMC
                 , RJCZE
            FROM
                TBO.ZS_QYCXSJ_GDJCZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.SJCZE,0) = nvl(d.SJCZE,0)
            AND nvl(s.CZRQ,0) = nvl(d.CZRQ,0)
            AND nvl(s.CZFS,0) = nvl(d.CZFS,0)
            AND nvl(s.CZFSDM,0) = nvl(d.CZFSDM,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.RJCZBZDM,0) = nvl(d.RJCZBZDM,0)
            AND nvl(s.CZBL,0) = nvl(d.CZBL,0)
            AND nvl(s.GDLX,0) = nvl(d.GDLX,0)
            AND nvl(s.GDLXDM,0) = nvl(d.GDLXDM,0)
            AND nvl(s.ZCZBBZ,0) = nvl(d.ZCZBBZ,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0)
            AND nvl(s.GDMC,0) = nvl(d.GDMC,0)
            AND nvl(s.RJCZE,0) = nvl(d.RJCZE,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , SJCZE
                , CZRQ
                , CZFS
                , CZFSDM
                , TYSHXYDM
                , RJCZBZDM
                , CZBL
                , GDLX
                , GDLXDM
                , ZCZBBZ
                , ZCH
                , GDMC
                , RJCZE
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.SJCZE
            , s.CZRQ
            , s.CZFS
            , s.CZFSDM
            , s.TYSHXYDM
            , s.RJCZBZDM
            , s.CZBL
            , s.GDLX
            , s.GDLXDM
            , s.ZCZBBZ
            , s.ZCH
            , s.GDMC
            , s.RJCZE
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_gqczxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_gqczxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_GQCZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_GQCZXX d
    USING (SELECT
                QYENTID
                 , CZGQSE
                 , CZRZJH
                 , CZR
                 , GSRQ
                 , ZQCZSLDJRQ
                 , DJBH
                 , ZT
                 , ZQRZJH
                 , ZQRXM
                 , GLNR
            FROM
                TBO.ZS_QYCXSJ_GQCZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.CZGQSE,0) = nvl(d.CZGQSE,0)
            AND nvl(s.CZRZJH,0) = nvl(d.CZRZJH,0)
            AND nvl(s.CZR,0) = nvl(d.CZR,0)
            AND nvl(s.GSRQ,0) = nvl(d.GSRQ,0)
            AND nvl(s.ZQCZSLDJRQ,0) = nvl(d.ZQCZSLDJRQ,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0)
            AND nvl(s.ZT,0) = nvl(d.ZT,0)
            AND nvl(s.ZQRZJH,0) = nvl(d.ZQRZJH,0)
            AND nvl(s.ZQRXM,0) = nvl(d.ZQRXM,0)
            AND nvl(s.GLNR,0) = nvl(d.GLNR,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , CZGQSE
                , CZRZJH
                , CZR
                , GSRQ
                , ZQCZSLDJRQ
                , DJBH
                , ZT
                , ZQRZJH
                , ZQRXM
                , GLNR
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.CZGQSE
            , s.CZRZJH
            , s.CZR
            , s.GSRQ
            , s.ZQCZSLDJRQ
            , s.DJBH
            , s.ZT
            , s.ZQRZJH
            , s.ZQRXM
            , s.GLNR
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_gqczxx_zxxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_gqczxx_zxxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_GQCZXX_ZXXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_GQCZXX_ZXXX d
    USING (SELECT
                QYENTID
                 , GSRQ
                 , ZXYY
                 , GLNR
            FROM
                TBO.ZS_QYCXSJ_GQCZXX_ZXXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.GSRQ,0) = nvl(d.GSRQ,0)
            AND nvl(s.ZXYY,0) = nvl(d.ZXYY,0)
            AND nvl(s.GLNR,0) = nvl(d.GLNR,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, GSRQ, ZXYY, GLNR, ETL_DATE)
     VALUES(s.QYENTID
            , s.GSRQ
            , s.ZXYY
            , s.GLNR
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_xgxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_xgxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_XGXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_XGXX d
    USING (SELECT
                QYENTID
                 , XGSX
                 , BGHNR
                 , BGQNR
                 , GQBGRQ
                 , NBID
            FROM
                TBO.ZS_QYCXSJ_NB_XGXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.XGSX,0) = nvl(d.XGSX,0)
            AND nvl(s.BGHNR,0) = nvl(d.BGHNR,0)
            AND nvl(s.BGQNR,0) = nvl(d.BGQNR,0)
            AND nvl(s.GQBGRQ,0) = nvl(d.GQBGRQ,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , XGSX
                , BGHNR
                , BGQNR
                , GQBGRQ
                , NBID
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.XGSX
            , s.BGHNR
            , s.BGQNR
            , s.GQBGRQ
            , s.NBID
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_gqbgxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_gqbgxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_GQBGXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_GQBGXX d
    USING (SELECT
                QYENTID
                 , GQBGRQ
                 , NBID
                 , GDMC
                 , ZRHGQBL
                 , ZRQGQBL
            FROM
                TBO.ZS_QYCXSJ_NB_GQBGXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.GQBGRQ,0) = nvl(d.GQBGRQ,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0)
            AND nvl(s.GDMC,0) = nvl(d.GDMC,0)
            AND nvl(s.ZRHGQBL,0) = nvl(d.ZRHGQBL,0)
            AND nvl(s.ZRQGQBL,0) = nvl(d.ZRQGQBL,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , GQBGRQ
                , NBID
                , GDMC
                , ZRHGQBL
                , ZRQGQBL
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.GQBGRQ
            , s.NBID
            , s.GDMC
            , s.ZRHGQBL
            , s.ZRQGQBL
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_qynbjbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_qynbjbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_QYNBJBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_QYNBJBXX d
    USING (SELECT
                QYENTID
                 , SQDZ
                 , ZHNJRQ
                 , NBID
                 , ZHNJND
                 , JYZT
                 , TYSHXYDM
                 , YX
                 , QYMC
                 , QYKGQK
                 , QYZYYWHD
                 , YB
                 , ZCH
                 , LXRDH
                 , NXCYRS
            FROM
                TBO.ZS_QYCXSJ_NB_QYNBJBXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.SQDZ,0) = nvl(d.SQDZ,0)
            AND nvl(s.ZHNJRQ,0) = nvl(d.ZHNJRQ,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0)
            AND nvl(s.ZHNJND,0) = nvl(d.ZHNJND,0)
            AND nvl(s.JYZT,0) = nvl(d.JYZT,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.YX,0) = nvl(d.YX,0)
            AND nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.QYKGQK,0) = nvl(d.QYKGQK,0)
            AND nvl(s.QYZYYWHD,0) = nvl(d.QYZYYWHD,0)
            AND nvl(s.YB,0) = nvl(d.YB,0)
            AND nvl(s.ZCH,0) = nvl(d.ZCH,0)
            AND nvl(s.LXRDH,0) = nvl(d.LXRDH,0)
            AND nvl(s.NXCYRS,0) = nvl(d.NXCYRS,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , SQDZ
                , ZHNJRQ
                , NBID
                , ZHNJND
                , JYZT
                , TYSHXYDM
                , YX
                , QYMC
                , QYKGQK
                , QYZYYWHD
                , YB
                , ZCH
                , LXRDH
                , NXCYRS
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.SQDZ
            , s.ZHNJRQ
            , s.NBID
            , s.ZHNJND
            , s.JYZT
            , s.TYSHXYDM
            , s.YX
            , s.QYMC
            , s.QYKGQK
            , s.QYZYYWHD
            , s.YB
            , s.ZCH
            , s.LXRDH
            , s.NXCYRS
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_shbxxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_shbxxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_SHBXXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_SHBXXX d
    USING (SELECT
                QYENTID
                 , NBID
                 , CZZGJBYLBXCBRS
                 , SIYBXCBRS
                 , ZGJBYLBXCBRS
                 , GSBXCBRS
                 , SGYBXCBRS
                 , CJCZZGJBYLBXBQSJJFJE
                 , CJSIYBXBQSJJFJE
                 , CJZGJBYLBXBQSJJFJE
                 , CJGSBXBQSJJFJE
                 , CJSGYBXBQSJJFJE
                 , DWCJCZZGJBYLBXJFJS
                 , DWCJSIYBXJFJS
                 , DWCJZGJBYLBXJFJS
                 , DWCJGSBXJFJS
                 , DWCJSGYBXJFJS
                 , DWCJCZZGJBYLBXLJQJJE
                 , DWCJSIYBXLJQJJE
                 , DWCJZGJBYLBXLJQJJE
                 , CJGSBXLJQJJE
                 , DWCJSGYBXLJQJJE
            FROM
                TBO.ZS_QYCXSJ_NB_SHBXXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0)
            AND nvl(s.CZZGJBYLBXCBRS,0) = nvl(d.CZZGJBYLBXCBRS,0)
            AND nvl(s.SIYBXCBRS,0) = nvl(d.SIYBXCBRS,0)
            AND nvl(s.ZGJBYLBXCBRS,0) = nvl(d.ZGJBYLBXCBRS,0)
            AND nvl(s.GSBXCBRS,0) = nvl(d.GSBXCBRS,0)
            AND nvl(s.SGYBXCBRS,0) = nvl(d.SGYBXCBRS,0)
            AND nvl(s.CJCZZGJBYLBXBQSJJFJE,0) = nvl(d.CJCZZGJBYLBXBQSJJFJE,0)
            AND nvl(s.CJSIYBXBQSJJFJE,0) = nvl(d.CJSIYBXBQSJJFJE,0)
            AND nvl(s.CJZGJBYLBXBQSJJFJE,0) = nvl(d.CJZGJBYLBXBQSJJFJE,0)
            AND nvl(s.CJGSBXBQSJJFJE,0) = nvl(d.CJGSBXBQSJJFJE,0)
            AND nvl(s.CJSGYBXBQSJJFJE,0) = nvl(d.CJSGYBXBQSJJFJE,0)
            AND nvl(s.DWCJCZZGJBYLBXJFJS,0) = nvl(d.DWCJCZZGJBYLBXJFJS,0)
            AND nvl(s.DWCJSIYBXJFJS,0) = nvl(d.DWCJSIYBXJFJS,0)
            AND nvl(s.DWCJZGJBYLBXJFJS,0) = nvl(d.DWCJZGJBYLBXJFJS,0)
            AND nvl(s.DWCJGSBXJFJS,0) = nvl(d.DWCJGSBXJFJS,0)
            AND nvl(s.DWCJSGYBXJFJS,0) = nvl(d.DWCJSGYBXJFJS,0)
            AND nvl(s.DWCJCZZGJBYLBXLJQJJE,0) = nvl(d.DWCJCZZGJBYLBXLJQJJE,0)
            AND nvl(s.DWCJSIYBXLJQJJE,0) = nvl(d.DWCJSIYBXLJQJJE,0)
            AND nvl(s.DWCJZGJBYLBXLJQJJE,0) = nvl(d.DWCJZGJBYLBXLJQJJE,0)
            AND nvl(s.CJGSBXLJQJJE,0) = nvl(d.CJGSBXLJQJJE,0)
            AND nvl(s.DWCJSGYBXLJQJJE,0) = nvl(d.DWCJSGYBXLJQJJE,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , NBID
                , CZZGJBYLBXCBRS
                , SIYBXCBRS
                , ZGJBYLBXCBRS
                , GSBXCBRS
                , SGYBXCBRS
                , CJCZZGJBYLBXBQSJJFJE
                , CJSIYBXBQSJJFJE
                , CJZGJBYLBXBQSJJFJE
                , CJGSBXBQSJJFJE
                , CJSGYBXBQSJJFJE
                , DWCJCZZGJBYLBXJFJS
                , DWCJSIYBXJFJS
                , DWCJZGJBYLBXJFJS
                , DWCJGSBXJFJS
                , DWCJSGYBXJFJS
                , DWCJCZZGJBYLBXLJQJJE
                , DWCJSIYBXLJQJJE
                , DWCJZGJBYLBXLJQJJE
                , CJGSBXLJQJJE
                , DWCJSGYBXLJQJJE
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.NBID
            , s.CZZGJBYLBXCBRS
            , s.SIYBXCBRS
            , s.ZGJBYLBXCBRS
            , s.GSBXCBRS
            , s.SGYBXCBRS
            , s.CJCZZGJBYLBXBQSJJFJE
            , s.CJSIYBXBQSJJFJE
            , s.CJZGJBYLBXBQSJJFJE
            , s.CJGSBXBQSJJFJE
            , s.CJSGYBXBQSJJFJE
            , s.DWCJCZZGJBYLBXJFJS
            , s.DWCJSIYBXJFJS
            , s.DWCJZGJBYLBXJFJS
            , s.DWCJGSBXJFJS
            , s.DWCJSGYBXJFJS
            , s.DWCJCZZGJBYLBXLJQJJE
            , s.DWCJSIYBXLJQJJE
            , s.DWCJZGJBYLBXLJQJJE
            , s.CJGSBXLJQJJE
            , s.DWCJSGYBXLJQJJE
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_rjczxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_rjczxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_RJCZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_RJCZXX d
    USING (SELECT
                QYENTID
                 , NBID
                 , CZRQ
                 , CZFS
                 , GDMC
                 , LJRJE
            FROM
                TBO.ZS_QYCXSJ_NB_RJCZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0)
            AND nvl(s.CZRQ,0) = nvl(d.CZRQ,0)
            AND nvl(s.CZFS,0) = nvl(d.CZFS,0)
            AND nvl(s.GDMC,0) = nvl(d.GDMC,0)
            AND nvl(s.LJRJE,0) = nvl(d.LJRJE,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, NBID, CZRQ, CZFS, GDMC, LJRJE, ETL_DATE)
     VALUES(s.QYENTID
            , s.NBID
            , s.CZRQ
            , s.CZFS
            , s.GDMC
            , s.LJRJE
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_wzxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_wzxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_WZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_WZXX d
    USING (SELECT
                QYENTID
                 , NBID
                 , LJDZ
                 , WZHZWDMC
                 , WZWDLX
            FROM
                TBO.ZS_QYCXSJ_NB_WZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0)
            AND nvl(s.LJDZ,0) = nvl(d.LJDZ,0)
            AND nvl(s.WZHZWDMC,0) = nvl(d.WZHZWDMC,0)
            AND nvl(s.WZWDLX,0) = nvl(d.WZWDLX,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, NBID, LJDZ, WZHZWDMC, WZWDLX, ETL_DATE)
     VALUES(s.QYENTID
            , s.NBID
            , s.LJDZ
            , s.WZHZWDMC
            , s.WZWDLX
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_nb_qysjczxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_nb_qysjczxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_NB_QYSJCZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_NB_QYSJCZXX d
    USING (SELECT
                QYENTID
                 , NBID
                 , CZRQ
                 , CZFS
                 , GDMC
                 , LJSJE
            FROM
                TBO.ZS_QYCXSJ_NB_QYSJCZXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.NBID,0) = nvl(d.NBID,0)
            AND nvl(s.CZRQ,0) = nvl(d.CZRQ,0)
            AND nvl(s.CZFS,0) = nvl(d.CZFS,0)
            AND nvl(s.GDMC,0) = nvl(d.GDMC,0)
            AND nvl(s.LJSJE,0) = nvl(d.LJSJE,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID, NBID, CZRQ, CZFS, GDMC, LJSJE, ETL_DATE)
     VALUES(s.QYENTID
            , s.NBID
            , s.CZRQ
            , s.CZFS
            , s.GDMC
            , s.LJSJE
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_qyjbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_qyjbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_QYJBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_QYJBXX d
    USING (SELECT
                QYENTID
                 , XKJYXM
                 , ZHNJND
                 , HZRQ
                 , ZXRQ
                 , TYSHXYDM
                 , ZS
                 , ZSSZXZQH
                 , YX
                 , QYENTID2
                 , STLX
                 , QYMC
                 , CYM
                 , QYZT
                 , JYZTBM
                 , QYLX
                 , QYLXBM
                 , KYRQ
                 , FZR
                 , QYID
                 , GMJJXYDM
                 , GMJJXYMC
                 , QYLOGOTP
                 , JYQX_START
                 , JYQX_END
                 , ZZJGDM
                 , YB
                 , QYEWM
                 , SSZB
                 , ZCZB
                 , ZCZBBZ
                 , ZCZBBZDM
                 , SZCSBM
                 , ZCH
                 , DJJG
                 , SZCS
                 , ZCDZXZQBH
                 , SZQ
                 , SZSF
                 , DXRQ
                 , SJDBH
                 , JYYWFW
            FROM
                TBO.ZS_QYCXSJ_QYJBXX where QYENTID is not null) s
    ON(s.QYENTID = d.QYENTID)
WHEN MATCHED THEN
    UPDATE SET XKJYXM = s.XKJYXM
                , ZHNJND = s.ZHNJND
                , HZRQ = s.HZRQ
                , ZXRQ = s.ZXRQ
                , TYSHXYDM = s.TYSHXYDM
                , ZS = s.ZS
                , ZSSZXZQH = s.ZSSZXZQH
                , YX = s.YX
                , QYENTID2 = s.QYENTID2
                , STLX = s.STLX
                , QYMC = s.QYMC
                , CYM = s.CYM
                , QYZT = s.QYZT
                , JYZTBM = s.JYZTBM
                , QYLX = s.QYLX
                , QYLXBM = s.QYLXBM
                , KYRQ = s.KYRQ
                , FZR = s.FZR
                , QYID = s.QYID
                , GMJJXYDM = s.GMJJXYDM
                , GMJJXYMC = s.GMJJXYMC
                , QYLOGOTP = s.QYLOGOTP
                , JYQX_START = s.JYQX_START
                , JYQX_END = s.JYQX_END
                , ZZJGDM = s.ZZJGDM
                , YB = s.YB
                , QYEWM = s.QYEWM
                , SSZB = s.SSZB
                , ZCZB = s.ZCZB
                , ZCZBBZ = s.ZCZBBZ
                , ZCZBBZDM = s.ZCZBBZDM
                , SZCSBM = s.SZCSBM
                , ZCH = s.ZCH
                , DJJG = s.DJJG
                , SZCS = s.SZCS
                , ZCDZXZQBH = s.ZCDZXZQBH
                , SZQ = s.SZQ
                , SZSF = s.SZSF
                , DXRQ = s.DXRQ
                , SJDBH = s.SJDBH
                , JYYWFW = s.JYYWFW
                , ETL_DATE = '"|| v_etl_date ||"'
    WHERE
          d.QYENTID = s.QYENTID
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , XKJYXM
                , ZHNJND
                , HZRQ
                , ZXRQ
                , TYSHXYDM
                , ZS
                , ZSSZXZQH
                , YX
                , QYENTID2
                , STLX
                , QYMC
                , CYM
                , QYZT
                , JYZTBM
                , QYLX
                , QYLXBM
                , KYRQ
                , FZR
                , QYID
                , GMJJXYDM
                , GMJJXYMC
                , QYLOGOTP
                , JYQX_START
                , JYQX_END
                , ZZJGDM
                , YB
                , QYEWM
                , SSZB
                , ZCZB
                , ZCZBBZ
                , ZCZBBZDM
                , SZCSBM
                , ZCH
                , DJJG
                , SZCS
                , ZCDZXZQBH
                , SZQ
                , SZSF
                , DXRQ
                , SJDBH
                , JYYWFW
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.XKJYXM
            , s.ZHNJND
            , s.HZRQ
            , s.ZXRQ
            , s.TYSHXYDM
            , s.ZS
            , s.ZSSZXZQH
            , s.YX
            , s.QYENTID2
            , s.STLX
            , s.QYMC
            , s.CYM
            , s.QYZT
            , s.JYZTBM
            , s.QYLX
            , s.QYLXBM
            , s.KYRQ
            , s.FZR
            , s.QYID
            , s.GMJJXYDM
            , s.GMJJXYMC
            , s.QYLOGOTP
            , s.JYQX_START
            , s.JYQX_END
            , s.ZZJGDM
            , s.YB
            , s.QYEWM
            , s.SSZB
            , s.ZCZB
            , s.ZCZBBZ
            , s.ZCZBBZDM
            , s.SZCSBM
            , s.ZCH
            , s.DJJG
            , s.SZCS
            , s.ZCDZXZQBH
            , s.SZQ
            , s.SZSF
            , s.DXRQ
            , s.SJDBH
            , s.JYYWFW
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_qycxsj_dcdy_djxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_qycxsj_dcdy_djxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_QYCXSJ_DCDY_DJXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_QYCXSJ_DCDY_DJXX d
    USING (SELECT
                QYENTID
                 , BDBZQSE
                 , DJBH
                 , DJRQ
                 , DJJG
                 , XZQHBM
                 , ZT
                 , HGZXBZBM
            FROM
                TBO.ZS_QYCXSJ_DCDY_DJXX) s
    ON(nvl(s.QYENTID,0) = nvl(d.QYENTID,0)
            AND nvl(s.BDBZQSE,0) = nvl(d.BDBZQSE,0)
            AND nvl(s.DJBH,0) = nvl(d.DJBH,0)
            AND nvl(s.DJRQ,0) = nvl(d.DJRQ,0)
            AND nvl(s.DJJG,0) = nvl(d.DJJG,0)
            AND nvl(s.XZQHBM,0) = nvl(d.XZQHBM,0)
            AND nvl(s.ZT,0) = nvl(d.ZT,0)
            AND nvl(s.HGZXBZBM,0) = nvl(d.HGZXBZBM,0))
WHEN NOT MATCHED THEN
    INSERT(QYENTID
                , BDBZQSE
                , DJBH
                , DJRQ
                , DJJG
                , XZQHBM
                , ZT
                , HGZXBZBM
                , ETL_DATE)
     VALUES(s.QYENTID
            , s.BDBZQSE
            , s.DJBH
            , s.DJRQ
            , s.DJJG
            , s.XZQHBM
            , s.ZT
            , s.HGZXBZBM
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_ztmksj_qyzpjbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_ztmksj_qyzpjbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_ZTMKSJ_QYZPJBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_ZTMKSJ_QYZPJBXX d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , ZPQYMC
                 , ZPLY
                 , ZPLYBM
                 , GSGM
                 , GSXY
                 , GSDZ
                 , GSZY
                 , GSJS
                 , ZPZLNR
                 , FBRQ
            FROM
                TBO.ZS_ZTMKSJ_QYZPJBXX) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.ZPQYMC,0) = nvl(d.ZPQYMC,0)
            AND nvl(s.ZPLY,0) = nvl(d.ZPLY,0)
            AND nvl(s.ZPLYBM,0) = nvl(d.ZPLYBM,0)
            AND nvl(s.GSGM,0) = nvl(d.GSGM,0)
            AND nvl(s.GSXY,0) = nvl(d.GSXY,0)
            AND nvl(s.GSDZ,0) = nvl(d.GSDZ,0)
            AND nvl(s.GSZY,0) = nvl(d.GSZY,0)
            AND nvl(s.GSJS,0) = nvl(d.GSJS,0)
            AND nvl(s.ZPZLNR,0) = nvl(d.ZPZLNR,0)
            AND nvl(s.FBRQ,0) = nvl(d.FBRQ,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , ZPQYMC
                , ZPLY
                , ZPLYBM
                , GSGM
                , GSXY
                , GSDZ
                , GSZY
                , GSJS
                , ZPZLNR
                , FBRQ
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.ZPQYMC
            , s.ZPLY
            , s.ZPLYBM
            , s.GSGM
            , s.GSXY
            , s.GSDZ
            , s.GSZY
            , s.GSJS
            , s.ZPZLNR
            , s.FBRQ
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_ztmksj_sswf(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_ztmksj_sswf';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_ZTMKSJ_SSWF WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_ZTMKSJ_SSWF d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , NSRDJMC
                 , NSRSBH
                 , FRGYXX
                 , FYZJZRDCWFZRGYXX
                 , FYZJZRDZJJGGYXX
                 , AJXZ
                 , XGFLYJJSWCLCFQK
            FROM
                TBO.ZS_ZTMKSJ_SSWF) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.NSRDJMC,0) = nvl(d.NSRDJMC,0)
            AND nvl(s.NSRSBH,0) = nvl(d.NSRSBH,0)
            AND nvl(s.FRGYXX,0) = nvl(d.FRGYXX,0)
            AND nvl(s.FYZJZRDCWFZRGYXX,0) = nvl(d.FYZJZRDCWFZRGYXX,0)
            AND nvl(s.FYZJZRDZJJGGYXX,0) = nvl(d.FYZJZRDZJJGGYXX,0)
            AND nvl(s.AJXZ,0) = nvl(d.AJXZ,0)
            AND nvl(s.XGFLYJJSWCLCFQK,0) = nvl(d.XGFLYJJSWCLCFQK,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , NSRDJMC
                , NSRSBH
                , FRGYXX
                , FYZJZRDCWFZRGYXX
                , FYZJZRDZJJGGYXX
                , AJXZ
                , XGFLYJJSWCLCFQK
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.NSRDJMC
            , s.NSRSBH
            , s.FRGYXX
            , s.FYZJZRDCWFZRGYXX
            , s.FYZJZRDZJJGGYXX
            , s.AJXZ
            , s.XGFLYJJSWCLCFQK
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_ztmksj_trzxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_ztmksj_trzxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_ZTMKSJ_TRZXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_ZTMKSJ_TRZXX d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , PLRQ
                 , SJQC
                 , TZFMC
                 , RZFMCJC
                 , TZXZ_LC
                 , TZSJ
                 , TZGQBL
                 , TZJE
                 , BZ
                 , GICSDM
                 , GICSMC
                 , DQMC
                 , QYJD
                 , SFSS
            FROM
                TBO.ZS_ZTMKSJ_TRZXX) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.PLRQ,0) = nvl(d.PLRQ,0)
            AND nvl(s.SJQC,0) = nvl(d.SJQC,0)
            AND nvl(s.TZFMC,0) = nvl(d.TZFMC,0)
            AND nvl(s.RZFMCJC,0) = nvl(d.RZFMCJC,0)
            AND nvl(s.TZXZ_LC,0) = nvl(d.TZXZ_LC,0)
            AND nvl(s.TZSJ,0) = nvl(d.TZSJ,0)
            AND nvl(s.TZGQBL,0) = nvl(d.TZGQBL,0)
            AND nvl(s.TZJE,0) = nvl(d.TZJE,0)
            AND nvl(s.BZ,0) = nvl(d.BZ,0)
            AND nvl(s.GICSDM,0) = nvl(d.GICSDM,0)
            AND nvl(s.GICSMC,0) = nvl(d.GICSMC,0)
            AND nvl(s.DQMC,0) = nvl(d.DQMC,0)
            AND nvl(s.QYJD,0) = nvl(d.QYJD,0)
            AND nvl(s.SFSS,0) = nvl(d.SFSS,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , PLRQ
                , SJQC
                , TZFMC
                , RZFMCJC
                , TZXZ_LC
                , TZSJ
                , TZGQBL
                , TZJE
                , BZ
                , GICSDM
                , GICSMC
                , DQMC
                , QYJD
                , SFSS
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.PLRQ
            , s.SJQC
            , s.TZFMC
            , s.RZFMCJC
            , s.TZXZ_LC
            , s.TZSJ
            , s.TZGQBL
            , s.TZJE
            , s.BZ
            , s.GICSDM
            , s.GICSMC
            , s.DQMC
            , s.QYJD
            , s.SFSS
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_ztmksj_zlflxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_ztmksj_zlflxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_ZTMKSJ_ZLFLXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_ZTMKSJ_ZLFLXX d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , SQGGH
                 , SQH
                 , BZR
                 , FLZTGGR
                 , FLZT
                 , FLZTXX
            FROM
                TBO.ZS_ZTMKSJ_ZLFLXX) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.SQGGH,0) = nvl(d.SQGGH,0)
            AND nvl(s.SQH,0) = nvl(d.SQH,0)
            AND nvl(s.BZR,0) = nvl(d.BZR,0)
            AND nvl(s.FLZTGGR,0) = nvl(d.FLZTGGR,0)
            AND nvl(s.FLZT,0) = nvl(d.FLZT,0)
            AND nvl(s.FLZTXX,0) = nvl(d.FLZTXX,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , SQGGH
                , SQH
                , BZR
                , FLZTGGR
                , FLZT
                , FLZTXX
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.SQGGH
            , s.SQH
            , s.BZR
            , s.FLZTGGR
            , s.FLZT
            , s.FLZTXX
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_ztmksj_zljbxx(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_ztmksj_zljbxx';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_ZTMKSJ_ZLJBXX WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_ZTMKSJ_ZLJBXX d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , TYSHXYDM2
                 , GKH
                 , GKR
                 , SQH
                 , ZLLXBM
                 , ZLLX
                 , SQR
                 , ZLH
                 , ZLMC
                 , ZFLH
                 , FLH
                 , ZLR
                 , FMR
                 , DZ
                 , ZLDLJG
                 , ZLDLJGWYBSM
                 , DLR
                 , SCY
            FROM
                TBO.ZS_ZTMKSJ_ZLJBXX) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.TYSHXYDM2,0) = nvl(d.TYSHXYDM2,0)
            AND nvl(s.GKH,0) = nvl(d.GKH,0)
            AND nvl(s.GKR,0) = nvl(d.GKR,0)
            AND nvl(s.SQH,0) = nvl(d.SQH,0)
            AND nvl(s.ZLLXBM,0) = nvl(d.ZLLXBM,0)
            AND nvl(s.ZLLX,0) = nvl(d.ZLLX,0)
            AND nvl(s.SQR,0) = nvl(d.SQR,0)
            AND nvl(s.ZLH,0) = nvl(d.ZLH,0)
            AND nvl(s.ZLMC,0) = nvl(d.ZLMC,0)
            AND nvl(s.ZFLH,0) = nvl(d.ZFLH,0)
            AND nvl(s.FLH,0) = nvl(d.FLH,0)
            AND nvl(s.ZLR,0) = nvl(d.ZLR,0)
            AND nvl(s.FMR,0) = nvl(d.FMR,0)
            AND nvl(s.DZ,0) = nvl(d.DZ,0)
            AND nvl(s.ZLDLJG,0) = nvl(d.ZLDLJG,0)
            AND nvl(s.ZLDLJGWYBSM,0) = nvl(d.ZLDLJGWYBSM,0)
            AND nvl(s.DLR,0) = nvl(d.DLR,0)
            AND nvl(s.SCY,0) = nvl(d.SCY,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , TYSHXYDM2
                , GKH
                , GKR
                , SQH
                , ZLLXBM
                , ZLLX
                , SQR
                , ZLH
                , ZLMC
                , ZFLH
                , FLH
                , ZLR
                , FMR
                , DZ
                , ZLDLJG
                , ZLDLJGWYBSM
                , DLR
                , SCY
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.TYSHXYDM2
            , s.GKH
            , s.GKR
            , s.SQH
            , s.ZLLXBM
            , s.ZLLX
            , s.SQR
            , s.ZLH
            , s.ZLMC
            , s.ZFLH
            , s.FLH
            , s.ZLR
            , s.FMR
            , s.DZ
            , s.ZLDLJG
            , s.ZLDLJGWYBSM
            , s.DLR
            , s.SCY
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_ztmksj_zpzzq(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_ztmksj_zpzzq';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_ZTMKSJ_ZPZZQ WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_ZTMKSJ_ZPZZQ d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , ZPMC
                 , ZPLBWDM
                 , ZZQRXM
                 , GJ
                 , SF
                 , CS
                 , ZZXM
                 , CZWCRQ
                 , SCFBRQ
                 , DJH
                 , DJRQ
                 , FBRQ
                 , YWZJ
                 , ZPLB
            FROM
                TBO.ZS_ZTMKSJ_ZPZZQ) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.ZPMC,0) = nvl(d.ZPMC,0)
            AND nvl(s.ZPLBWDM,0) = nvl(d.ZPLBWDM,0)
            AND nvl(s.ZZQRXM,0) = nvl(d.ZZQRXM,0)
            AND nvl(s.GJ,0) = nvl(d.GJ,0)
            AND nvl(s.SF,0) = nvl(d.SF,0)
            AND nvl(s.CS,0) = nvl(d.CS,0)
            AND nvl(s.ZZXM,0) = nvl(d.ZZXM,0)
            AND nvl(s.CZWCRQ,0) = nvl(d.CZWCRQ,0)
            AND nvl(s.SCFBRQ,0) = nvl(d.SCFBRQ,0)
            AND nvl(s.DJH,0) = nvl(d.DJH,0)
            AND nvl(s.DJRQ,0) = nvl(d.DJRQ,0)
            AND nvl(s.FBRQ,0) = nvl(d.FBRQ,0)
            AND nvl(s.YWZJ,0) = nvl(d.YWZJ,0)
            AND nvl(s.ZPLB,0) = nvl(d.ZPLB,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , ZPMC
                , ZPLBWDM
                , ZZQRXM
                , GJ
                , SF
                , CS
                , ZZXM
                , CZWCRQ
                , SCFBRQ
                , DJH
                , DJRQ
                , FBRQ
                , YWZJ
                , ZPLB
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.ZPMC
            , s.ZPLBWDM
            , s.ZZQRXM
            , s.GJ
            , s.SF
            , s.CS
            , s.ZZXM
            , s.CZWCRQ
            , s.SCFBRQ
            , s.DJH
            , s.DJRQ
            , s.FBRQ
            , s.YWZJ
            , s.ZPLB
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

  PROCEDURE pro_data_zs_ztmksj_fygg(i_etl_date STRING) IS
	 l_trlg  cc.pkg_dw_util.r_trlg; --声明日志表变量组
	 v_etl_date   STRING;
	 v_sql         STRING; --动态sql语句
	 v_begin_time TIMESTAMP; 
     error_exception EXCEPTION; --声明错误的异常变量
BEGIN
     --设置环境
    set_env('transaction.type', 'inceptor');
    v_etl_date   := i_etl_date;
    v_begin_time := systimestamp;
    --日志参数初始化
    l_trlg.log_seq     := 0;
    l_trlg.begin_time  := systimestamp;  
    l_trlg.pro_name    := 'pro_data_zs_ztmksj_fygg';
    l_trlg.log_action  := 'Begin';
    l_trlg.log_code    := '0';
    l_trlg.log_desc    := '处理开始';
    l_trlg.etl_date    := v_etl_date; 
    l_trlg.status      :='9';   
    l_trlg.end_time    := systimestamp;
    l_trlg.time_cost   := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time);
    --初始日志
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
	
    --清除当天数据
    v_sql :="DELETE from OMI.ZS_ZTMKSJ_FYGG WHERE etl_date='"|| v_etl_date || "'";
    BEGIN
        l_trlg.log_desc   := '清空orc当天数据';
        l_trlg.log_action := 'DELETE';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
      END;
      
    --插入当天数据
    v_sql :="MERGE INTO OMI.ZS_ZTMKSJ_FYGG d
    USING (SELECT
                QYMC
                 , TYSHXYDM
                 , GGFL
                 , GGR
                 , DSR
                 , GGSJ
                 , ZXWSNR
                 , GGRDQBM
                 , GGRDQMC
            FROM
                TBO.ZS_ZTMKSJ_FYGG) s
    ON(nvl(s.QYMC,0) = nvl(d.QYMC,0)
            AND nvl(s.TYSHXYDM,0) = nvl(d.TYSHXYDM,0)
            AND nvl(s.GGFL,0) = nvl(d.GGFL,0)
            AND nvl(s.GGR,0) = nvl(d.GGR,0)
            AND nvl(s.DSR,0) = nvl(d.DSR,0)
            AND nvl(s.GGSJ,0) = nvl(d.GGSJ,0)
            AND nvl(s.ZXWSNR,0) = nvl(d.ZXWSNR,0)
            AND nvl(s.GGRDQBM,0) = nvl(d.GGRDQBM,0)
            AND nvl(s.GGRDQMC,0) = nvl(d.GGRDQMC,0))
WHEN NOT MATCHED THEN
    INSERT(QYMC
                , TYSHXYDM
                , GGFL
                , GGR
                , DSR
                , GGSJ
                , ZXWSNR
                , GGRDQBM
                , GGRDQMC
                , ETL_DATE)
     VALUES(s.QYMC
            , s.TYSHXYDM
            , s.GGFL
            , s.GGR
            , s.DSR
            , s.GGSJ
            , s.ZXWSNR
            , s.GGRDQBM
            , s.GGRDQMC
            , '"|| v_etl_date ||"')";
    BEGIN
        l_trlg.log_desc   := '插入orc当天数据';
        l_trlg.log_action := 'INSERT';
        l_trlg.log_seq    := l_trlg.log_seq + 1;
        l_trlg.begin_time := systimestamp;
        EXECUTE IMMEDIATE (v_sql);
        l_trlg.end_time   := systimestamp;
        l_trlg.time_cost  := unix_timestamp(l_trlg.end_time) - unix_timestamp(l_trlg.begin_time)
        cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
      EXCEPTION
        WHEN OTHERS THEN
          l_trlg.log_code := SQLCODE();
          l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
          RAISE error_exception;
    END;
    -- 结束日志
    l_trlg.log_desc   := '处理结束';
    l_trlg.log_action := 'End';
    l_trlg.log_seq    := 99;
    l_trlg.begin_time := v_begin_time;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
END;

END;