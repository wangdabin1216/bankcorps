CREATE OR REPLACE PROCEDURE   <pro_name>   (i_acct_date IN DATE) IS
  /************************************************************************
  过程中文名：<pro_chn_name>
  功能描述：  <func_desc>
  编写人：    <comp_pers>
  编写日期：  <comp_date>
  修改记录：   <modif_record>      
  *************************************************************************/
  --通用变量
  l_trlg cc.pkg_dw_util.r_trlg; --日志变量组
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
  v_log_code   STRING; --获取返回日志代码
  v_log_desc   STRING; --获取返回日志信息
BEGIN
  --0   处理准备
  --0.1  设置环境
  set_env('transaction.type', 'inceptor');
  set_env('plsql.catch.hive.exception','true');
  --0.2  初始化变量
  v_acct_date   := i_acct_date;
  v_object_name := '<pro_name>';
  v_begin_time  := systimestamp;
  v_system_flag := '<system_flag>';
  v_partid      := to_char(v_acct_date, 'yyyyMM');
  --0.3   日志变量组的初始化
  l_trlg.pro_name    := '<pro_name>';
  l_trlg.log_object  := '<system_flag>.<t_name>';
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
   EXECUTE IMMEDIATE ('truncate table ' || l_trlg.log_object);
<part1 end>
    --1.2创建临时表
    --1.3 数据处理1
<part2 start>
  --2  历史数据处理(如果需要进行历史数据处理)
  --2.0 数据标准程序处理程序
  
  --2.1 调用历史拉链程序
  BEGIN
    l_trlg.log_desc   := v_object_name || '-历史拉链处理';
    l_trlg.log_action := 'Chain';
    l_trlg.log_seq    := l_trlg.log_seq + 1;
    l_trlg.begin_time := systimestamp;
    cc.pkg_dw_util.pro_data_his_main(i_etl_date  => v_acct_date,
                                  i_table_name => l_trlg.log_object);
    IF l_trlg.log_code <> cc.pkg_dw_util.log_code_ok THEN
      RAISE error_exception;
    END IF;
    l_trlg.row_count := 0;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
  EXCEPTION
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
      RAISE error_exception;
  END;
  --3  处理结束
  --3.1  写处理结束的日志
  l_trlg.log_desc   := v_object_name || ' 处理结束';
  l_trlg.log_action := 'End';
  l_trlg.log_seq    := 99;
  l_trlg.begin_time := v_begin_time;
  l_trlg.row_count  := 0;
  cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
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
    put_line(l_trlg.log_desc);
  WHEN error_exception THEN
    o_log_code := l_trlg.log_code;
    o_log_desc := l_trlg.log_desc;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    put_line(l_trlg.log_code);
    put_line(l_trlg.log_desc);
  WHEN OTHERS THEN
    l_trlg.log_code := SQLCODE();
    l_trlg.log_desc := SQLERRM();
    o_log_code      := l_trlg.log_code;
    o_log_desc      := l_trlg.log_desc;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
    put_line(l_trlg.log_code);
    put_line(l_trlg.log_desc);
END;
<part2 end>
<log_head start>
begin
    l_trlg.log_desc   := v_object_name || '-<part_comment>';
    l_trlg.log_action := 'Insert';
    l_trlg.log_seq    := l_trlg.log_seq + 1;
    l_trlg.begin_time := systimestamp;
<log_head end>
<log_end start>
    l_trlg.row_count := SQL%ROWCOUNT;
    cc.pkg_dw_util.pro_comm_trlg(t_trlg => l_trlg);
  EXCEPTION
    WHEN OTHERS THEN
      l_trlg.log_code := SQLCODE();
      l_trlg.log_desc := l_trlg.log_desc || '异常，异常信息：' || SQLERRM();
      RAISE error_exception;
end;
<log_end end>

