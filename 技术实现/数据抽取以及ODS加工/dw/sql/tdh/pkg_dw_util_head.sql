!set plsqlUseSlash true
CREATE OR REPLACE PACKAGE cc.pkg_dw_util IS
  /*************************************************************************************************
  过程中文名：TDH平台公用程序包
  功能描述： 基于TDH-inceptor实现通用类数据处理操作 
           
  创建人：    guxn
  创建日期：  2019-11-25
  
  版本号：    V1.0.0
  版本日期：  2019-11-25
  版本号说明：1.基于HF版本整体拆解出通用产品化版本
  
  修改记录：   1. 2019-11-25 by guxn 新建
  
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
  g_code_layer CONSTANT STRING DEFAULT 'cc';
  --包名
  g_pkg_name CONSTANT STRING DEFAULT 'pkg_dw_util';
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

  --历史存储处理配置变量组
  TYPE r_hist IS RECORD(
    table_name    STRING, --表名
    system_flag   STRING, --系统标识
    table_hs_name STRING, --历史表的名字
    fields        STRING, --字段序列
    keys          STRING, --主键序列
    region_type     STRING, --分区方式 （不分区 none   月分区 yyyymm  日分区 yyyymmdd  年分区 yyyy ） 
    trans_type    STRING, --表的处理方式:chain or add
    hist_field       STRING, --add插入过滤条件字段
    sync_type     STRING --是否同步字段
    );
  
  --对外卸数配置表变量组
  TYPE r_unload IS RECORD(
    table_name STRING,--供数的表名
    file_seq     STRING,--文件序号
    separator  STRING,--字段分隔符
    fields     STRING,--供数的字段
    condition STRING, --约束条件
    status STRING --0不供数1供数2已废弃
    );

    
  --String类型的嵌套表类型
  TYPE str_tab IS TABLE OF STRING;

  --函数-- 

  --获取特定日期 
  FUNCTION fun_get_date(p_acct_date IN DATE DEFAULT SYSDATE,
                        p_date_type IN STRING) RETURN DATE;

  --获取与特定日期的间隔天数
  FUNCTION fun_get_days(p_acct_date DATE DEFAULT SYSDATE, p_type STRING)
    RETURN INT;

  --获取特定格式的毫秒时间
  FUNCTION fun_get_micro_time(p_time TIMESTAMP DEFAULT systimestamp)
    RETURN STRING;

  FUNCTION fun_if_valid_date(p_acct_date IN STRING) 
    RETURN INT;

  --账号、客户号类脱敏函数
  FUNCTION fun_des_acct(v_no STRING, v_type string)
    RETURN string;

  --证件号类脱敏函数
   FUNCTION fun_des_cert(v_cert STRING, v_type string)
    RETURN string;

  --金额类脱敏函数
  FUNCTION fun_des_fund(v_fund decimal, v_type string)
    RETURN decimal;

  --地址类脱敏函数
  FUNCTION fun_des_local(v_addr STRING, v_type string)
    RETURN string;

  --名称类脱敏函数
  FUNCTION fun_des_name(v_name STRING, v_type string)
    RETURN string;

  --联系方式类脱敏函数
  FUNCTION fun_des_rela(v_rela STRING, v_type string)
    RETURN string;
    
  --存储过程 按功能模块分类--

  --通用操作  comm --

  --通用操作-写入操作日志记录
  PROCEDURE pro_comm_trlg(t_trlg IN r_trlg); 

  --数据处理 data--

  --数据处理-按历史处理策略分类调度
  PROCEDURE pro_data_his_main(i_etl_date IN DATE, i_table_name IN STRING);

  --数据处理-历史拉链处理-全量
 -- PROCEDURE pro_data_chain(i_etl_date in date,t_hist in pkg_dw_util.r_hist,o_log_code out string,o_log_desc out string); 
 procedure pro_data_chain(i_etl_date in date,t_hist in r_hist,o_log_code out string,o_log_desc out string);
--数据处理-增量数据处理
  PROCEDURE pro_data_add(i_etl_date IN DATE, t_hist IN r_hist,o_log_code OUT STRING, o_log_desc OUT STRING);

  --数据处理--历史拉链处理-增量
  PROCEDURE pro_data_chain_add(i_etl_date IN DATE , i_table_name IN STRING,o_log_code OUT STRING, o_log_desc OUT STRING);

  --数据处理-模型字典标准化处理过程
  --PROCEDURE pro_data_standardize(i_acct_date IN DATE,i_table_name IN STRING,o_log_code OUT STRING, o_log_desc OUT STRING);
 
   --数据检核 check --
  
  --数据检核-供数文件记录数和数据库检查
  PROCEDURE pro_check_unload_num(i_file_seq IN INT, i_table_name IN STRING,
                         i_etl_date IN STRING);

  --数据检核-数据质量检查
  PROCEDURE pro_check_cols_quality(i_acct_date IN DATE  , i_table_name in string);
  
   --数据服务  serv --
               
  --数据服务-对外供数卸载文本
  PROCEDURE pro_serv_dsa_unload(i_file_seq IN INT, i_table_name IN STRING,
                                i_etl_date IN STRING);

  --数据服务-创建视图
  PROCEDURE pro_serv_view(i_table_name IN STRING, i_view_name IN string);

  --数据服务-添加视图注释    
  PROCEDURE pro_serv_view_desc(i_db_name IN STRING, i_view_name IN STRING);
                                
  --数据同步 sync --
  PROCEDURE pro_sync_cc_data(i_src_table IN STRING,i_tar_table IN STRING);
END;
/
