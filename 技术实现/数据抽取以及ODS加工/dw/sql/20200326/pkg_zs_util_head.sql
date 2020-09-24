!set plsqlUseSlash true
CREATE OR REPLACE PACKAGE omi.pkg_zs_util IS
  /*************************************************************************************************
  过程中文名：中数数据加工程序包
  功能描述： 基于TDH-inceptor实现通用类数据处理操作 
           
  创建人：    dk
  创建日期：  2020-03-04
  
  修改记录：   1. 2020-03-04 by dk 新建
  
  规范：
        函数统一使用fun_开头，函数变量p_开头
        存储过程统一使用pro_开头，传入变量i_开头，传出变量o_开头

        存储过程分为
        通用操作 comm
        数据处理 data
        数据检核 check
        数据服务 serv
        数据同步 sync

  **************************************************************************************************/
  
  --静态定义--

  --代码库层级--
  g_code_layer CONSTANT STRING DEFAULT 'omi';
  --包名
  g_pkg_name CONSTANT STRING DEFAULT 'pkg_zs_util';
  --成功返回码值的定义
  log_code_ok CONSTANT STRING DEFAULT '0';
  log_desc_ok CONSTANT STRING DEFAULT '完成';
  --定义警告类型的返回码值
  --当天已经处理过
  log_warn_exists_code CONSTANT STRING DEFAULT '1001';
  log_warn_exists_desc CONSTANT STRING DEFAULT '当天已经处理过';
  --源表没有数据
  log_warn_nodata_code CONSTANT STRING DEFAULT '1002';
  log_warn_nodata_desc CONSTANT STRING DEFAULT '源表为空，没有数据，请检查';
  --目标表不为空
  log_warn_notempty_code CONSTANT STRING DEFAULT '1003';
  log_warn_notempty_desc CONSTANT STRING DEFAULT '目标表数据不为空，删除失败，请检查';
  --开始日期大于结束日期
  log_warn_largedate_code CONSTANT STRING DEFAULT '1004';
  log_warn_largedate_desc CONSTANT STRING DEFAULT '开始日期大于结束日期，请检查';
  --月底快照表当前日期非月底警告
  log_warn_notlastmonth_code CONSTANT STRING DEFAULT '1005';
  log_warn_notlastmonth_desc CONSTANT STRING DEFAULT '当前日期非月底，不执行程序';
  --参数错误异常信息
  log_error_parawrong_code CONSTANT STRING DEFAULT '2001';
  log_error_parawrong_desc CONSTANT STRING DEFAULT '参数传入错误，不执行程序';
  --配置表信息有误
  log_error_configwrong_code CONSTANT STRING DEFAULT '2002';
  log_error_configwrong_desc CONSTANT STRING DEFAULT '配置表配置错误，不执行程序';

  --变量组--

  --日志记录变量
  TYPE r_trlg IS RECORD(
    log_seq     INT, --程序处理的序号
    system_flag STRING, --系统标识
    begin_time  TIMESTAMP, --操作开始时间
    end_time    TIMESTAMP, --操作结束时间
    time_cost   STRING, --操作耗时
    pro_name    STRING, --调用的存储过程名字
    log_object  STRING, --操作的对象
    log_action  STRING, --操作的元素
    row_count   INT, --操作的行数
    log_code    STRING, --操作的代码
    log_desc    STRING, --操作的描述
    etl_date    STRING, --操作的日期
    status      STRING --表的处理状态
    );

  --数据处理 data--

  --数据处理-
  PROCEDURE pro_data_zs_qykzlj_gd(i_etl_date STRING);
  PROCEDURE pro_data_zs_qykzlj_dwtz(i_etl_date STRING);
  PROCEDURE pro_data_zs_qykzlj_sjkzr(i_etl_date STRING);
  PROCEDURE pro_data_zs_qyzpzwxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_cpws(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_dcdy_bgxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_bgxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_yzwf(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_xzcfjbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_qydwtzxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_qydwtzxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_qyycml(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_fzjg(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_qyfddbrdwtzxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_fddbrqtgsrz(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_ccjc(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_sfxzjbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_sfxzxq(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_qsxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_ssgpjbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_dcdy_jbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_dcdy_zxxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_dcdy_bdbzzqxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_dcdy_dywxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_dcdy_dyqrxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_zyglry(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_sxbzxrxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_bzxrxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_jyzx_jbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_glsxbzxrxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_glbzxrxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_gdjczxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_gqczxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_gqczxx_zxxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_xgxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_gqbgxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_qynbjbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_shbxxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_rjczxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_wzxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_nb_qysjczxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_qyjbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_qycxsj_dcdy_djxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_ztmksj_qyzpjbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_ztmksj_sswf(i_etl_date STRING);
  PROCEDURE pro_data_zs_ztmksj_trzxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_ztmksj_zlflxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_ztmksj_zljbxx(i_etl_date STRING);
  PROCEDURE pro_data_zs_ztmksj_zpzzq(i_etl_date STRING);
  PROCEDURE pro_data_zs_ztmksj_fygg(i_etl_date STRING);
END;