DROP TABLE omi.dual;
CREATE  TABLE IF NOT EXISTS omi.dual(
  dummy string DEFAULT NULL COMMENT ''
);
INSERT INTO omi.dual SELECT 'TDH' FROM system.dual;

--日志信息表
CREATE TABLE IF NOT EXISTS OMI.ORC_SYS_LOG
(S_TIME VARCHAR(32) COMMENT '日志时间'
  , ETL_DATE VARCHAR(8) COMMENT 'etl日期'
  , S_LEVEL VARCHAR(32) COMMENT '日志级别'
  , S_PROCNAME VARCHAR(64) COMMENT '执行存储过程名称'
  , S_MSG VARCHAR(4000) COMMENT '日志信息')
COMMENT '日志信息表'
CLUSTERED BY(S_TIME)
INTO 20 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

--鉴权信息全量表:ln_auth_info
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_LN_AUTH_INFO
(protocol_code VARCHAR(30) COMMENT '鉴权协议编号:(支付鉴权协议)年份+16位序号如：20170000000000000020'
  , auth_acc_no VARCHAR(32) COMMENT '鉴权账号'
  , auth_acc_name VARCHAR(120) COMMENT '鉴权账号名称'
  , cert_no VARCHAR(32) COMMENT '鉴权证件号'
  , cust_bank_mobile VARCHAR(11) COMMENT '鉴权手机号'
  , cif_no DECIMAL(12, 0) COMMENT '客户号'
  , partner VARCHAR(20) COMMENT '合作方ID'
  , filler VARCHAR(50) COMMENT '备注'
  , auth_sts VARCHAR(4) COMMENT '鉴权状态: AS00-鉴权通过 AS01-鉴权失败/作废')
COMMENT '鉴权信息全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/ln_auth_info';

--贷款借据主表全量表:ln_due_mst
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_LN_DUE_MST
(APP_STS VARCHAR(4) COMMENT '审批状态 00:待审批 01:正常(审批通过) 99：审批不通过'
  , CONT_NO VARCHAR(20) COMMENT '合同编号 互金合同编号'
  , DUE_NO VARCHAR(30) COMMENT '借据号 年份+10位序号(供14位)'
  , CIF_NO VARCHAR(12) COMMENT '客户号 互金客户号'
  , CIF_NAME VARCHAR(80) COMMENT '客户名称'
  , APP_NO VARCHAR(20) COMMENT '申请号 同贷款申请放款编号'
  , NOTE_NO VARCHAR(20) COMMENT '通知单编号 核心返回通知单号'
  , OCCUR_TYPE VARCHAR(2) COMMENT '发生类型'
  , PRDT_NO VARCHAR(10) COMMENT '产品号'
  , CUR_NO VARCHAR(3) COMMENT '币种 同ln_cont_mst.cur_no'
  , LN_TERM_TYPE VARCHAR(4) COMMENT '贷款期限单位 Y年 M月 D日 ET00指定期限时必填必输'
  , BEG_DATE VARCHAR(8) COMMENT '贷款发放日期'
  , END_DATE VARCHAR(8) COMMENT '贷款到期日期(展期到期日期)'
  , BASE_RATE_TYPE VARCHAR(4) COMMENT '利率类型'
  , BASE_RATE DECIMAL(10, 6) COMMENT '基准列率'
  , LN_RATE DECIMAL(10, 6) COMMENT '执行利率'
  , FLOAT_RATIO DECIMAL(10, 6) COMMENT '浮动比率'
  , OVER_RATE DECIMAL(10, 6) COMMENT '逾期利率'
  , FINE_RATE DECIMAL(10, 6) COMMENT '罚息利率'
  , CMPD_RATE DECIMAL(10, 6) COMMENT '复利利率'
  , EXP_FLAG DECIMAL(22, 2) COMMENT '展期标志 0.不展期 其他代表次数'
  , EXP_RATE DECIMAL(10, 6) COMMENT '展期利率'
  , INNER_INTST DECIMAL(22, 2) COMMENT '表内欠息'
  , OUTER_INTST DECIMAL(22, 2) COMMENT '表外欠息'
  , DUE_AMT DECIMAL(22, 2) COMMENT '借据金额'
  , BAL DECIMAL(22, 2) COMMENT '借据余额'
  , ACC_NO VARCHAR(30) COMMENT '贷款账号'
  , ACC_HRT VARCHAR(10) COMMENT '科目号'
  , HX_PRDT_NO VARCHAR(10) COMMENT '核心产品号'
  , DEAL_FLAG VARCHAR(4) COMMENT '日终处理标志'
  , VOU_TYPE VARCHAR(4) COMMENT '担保类型'
  , PAY_ACC_NO VARCHAR(30) COMMENT '还款账号'
  , PUT_ACC_NO VARCHAR(30) COMMENT '入账账号'
  , PAY_TYPE VARCHAR(4) COMMENT '支付方式'
  , OVER_DAYS DECIMAL(22, 2) COMMENT '逾期天数'
  , TOT_OVER_TIMES DECIMAL(22, 2) COMMENT '逾期次数'
  , FIVE_STS VARCHAR(4) COMMENT '五级分类状态'
  , FOUR_STS VARCHAR(4) COMMENT '四级分类状态'
  , GRADE VARCHAR(4) COMMENT '信用等级'
  , DUE_STS VARCHAR(4) COMMENT '借据状态'
  , MANG_BRNO VARCHAR(10) COMMENT '管户机构'
  , MANG_NO VARCHAR(10) COMMENT '管户客户经理'
  , ACC_BR_NO VARCHAR(60) COMMENT '核算机构'
  , BR_NO VARCHAR(10) COMMENT '登记单位'
  , OP_NO VARCHAR(10) COMMENT '操作员'
  , TX_DATE VARCHAR(8) COMMENT '登记日期'
  , UP_DATE VARCHAR(8) COMMENT '更新日期'
  , FILLER VARCHAR(100) COMMENT '登记单位'
  , OLD_END_DATE VARCHAR(8) COMMENT '原到期日期'
  , IC_TYPE VARCHAR(4) COMMENT '计息方式'
  , REPAY_TYPE VARCHAR(4) COMMENT '还款方式'
  , PAYEE_NAME VARCHAR(80) COMMENT '收款人全称'
  , PAYEE_AC_NO VARCHAR(30) COMMENT '收款人账号'
  , PAYEE_BR_NAME VARCHAR(80) COMMENT '收款人开户银行全称'
  , CAP_TYPE VARCHAR(4) COMMENT '资金性质'
  , PUT_TYPE VARCHAR(4) COMMENT '发放形式'
  , IF_AUTO VARCHAR(4) COMMENT '是否自动扣款'
  , SPEC_CAP_TYPE VARCHAR(4) COMMENT '专项资金类型'
  , PUT_NAME VARCHAR(80) COMMENT '入账账户户名'
  , RATE_CHG_TYPE VARCHAR(3) COMMENT '利率调整方式'
  , REPAY_DAY_WAY VARCHAR(4) COMMENT '还款日方式'
  , REPAY_DAY VARCHAR(3) COMMENT '还款日'
  , REPAY_TERM_KIND VARCHAR(4) COMMENT '还款周期类型'
  , REPAY_TERM DECIMAL(22, 2) COMMENT '还款周期'
  , CONT_OVER_TIMES DECIMAL(22, 2) COMMENT '连续逾期次数'
  , FOUR_MOD_DATE VARCHAR(8) COMMENT '四级分类调整时间'
  , SUIT_PRESP VARCHAR(8) COMMENT '诉讼时效'
  , LN_STS VARCHAR(4) COMMENT '贷款状态'
  , LAST_PAY_DAY VARCHAR(8) COMMENT '最后结息日期'
  , PAY_AMT DECIMAL(22, 2) COMMENT '受托支付金额'
  , PAY_STATE VARCHAR(4) COMMENT '1:未支付 2：部分支付 3：支付完成 是否结清表示，3结清'
  , REINFORCE_FLAG VARCHAR(2) COMMENT '补登标志(核心用)'
  , ACNO_STATUS VARCHAR(10) COMMENT '账户状态(核心用)'
  , PAY_MON VARCHAR(6) COMMENT '按揭贷款还款月份'
  , BILL_NO VARCHAR(30) COMMENT '票据号码'
  , IS_COM_FLAG VARCHAR(2) COMMENT '是否计复利'
  , COM_ADDR VARCHAR(80) COMMENT '地址'
  , CIF_TEL VARCHAR(35) COMMENT '联系电话'
  , DAYS DECIMAL(22, 2) COMMENT '到期天数'
  , AC_ID VARCHAR(22) COMMENT '贷款账号id'
  , AC_SEQN VARCHAR(22) COMMENT '贷款账号序号'
  , FLOAT_TYPE VARCHAR(4) COMMENT '浮动方式'
  , LN_TERM DECIMAL(4) COMMENT '贷款期限'
  , ACCT_NO VARCHAR(40) COMMENT '核心贷款账号'
  , GRANT_NO VARCHAR(20) COMMENT '核心贷款发放序号'
  , LN_NO DECIMAL(20) COMMENT '贷款申请编号')
COMMENT '贷款借据主表全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/ln_due_mst';

--放款计划登记簿（蛋壳）全量表:ln_loan_plan_reg
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS OMI.EXT_LN_LOAN_PLAN_REG
(ln_no DECIMAL(20) COMMENT '贷款编号:年份+序列号 序列：SEQ_LN_REG_NO'
  , acc_id number(12) COMMENT '贷款账号ID'
  , acc_seqn number(6) COMMENT '贷款账号序号	'
  , loan_acc_no VARCHAR(32) COMMENT '放款账号'
  , loan_acc_seqn number(6) COMMENT '放款账号序号'
  , plan_pay_amt number(22, 2) COMMENT '计划放款总金额'
  , plan_total_cnt number(6) COMMENT '计划放款次数'
  , pay_amt number(22, 2) COMMENT '已放款金额'
  , pay_cnt number(6) COMMENT '已放款次数'
  , trust_amt number(22, 2) COMMENT '受托支付金额	'
  , loan_date VARCHAR(8) COMMENT '放款日期		'
  , loan_type VARCHAR(4) COMMENT '放款方式:LT00 按比例, LT01 按金额'
  , loan_amt number(22, 2) COMMENT '放款金额'
  , adj_amt number(22, 2) COMMENT '调整金额:用于贴现差额放款'
  , loan_ratio number(10, 6) COMMENT '放款比例:按百分比放款用'
  , loan_sts VARCHAR(4) COMMENT '放款状态:LS00 审批中,LS01 待放款,LS02 未放完,LS03 已放完,LS04 取消,LS05 需再次检查	'
  , reg_date VARCHAR(8) COMMENT '登记日期'
  , reg_trc_no number(12) COMMENT '登记流水'
  , reg_br_no VARCHAR(10) COMMENT '登记机构'
  , reg_tel VARCHAR(12) COMMENT '登记柜员'
  , pay_seqn NUMBER(4) COMMENT '放款序号:蛋壳公寓分批放款'
  , ln_term NUMBER(4) COMMENT '期限'
  , repay_date VARCHAR(8) COMMENT '还款到期日期	')
COMMENT '放款计划登记簿（蛋壳）全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/ln_loan_plan_reg';

--贷款还款计划全量表：ln_pay_plan
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_LN_PAY_PLAN
(LN_NO DECIMAL(20) COMMENT '贷款编号'
  , ACC_ID DECIMAL(12) COMMENT '账号ID'
  , ACC_SEQN DECIMAL(6) COMMENT '账号序号'
  , TOTAL_AMT DECIMAL(22, 2) COMMENT '总金额 注：原始本金，生成还款计划总本金'
  , TOTAL_CNT DECIMAL(6) COMMENT '总期数 贷款还款总期数'
  , CURR_CNT DECIMAL(6) COMMENT '当前期次'
  , CURR_AMT DECIMAL(22, 2) COMMENT '本期本金 当前期次应还本金'
  , CURR_INTS DECIMAL(22, 2) COMMENT '本期利息 当前期次应还利息'
  , LAST_TRADE_DATE VARCHAR(8) COMMENT '最后一次交易日期'
  , REG_DATE VARCHAR(8) COMMENT '登记日期 暂时不用'
  , BEG_DATE VARCHAR(8) COMMENT '开始归还日期/计划发放日期 注：决定银行何时扣款，与核心的每期的到期日对应'
  , END_DATE VARCHAR(8) COMMENT '到期日期 注：决定此记录何时转逾期或欠款。考虑到不同的产品可能宽限期不同的情况'
  , PAY_STS VARCHAR(4) COMMENT '还款状态 PS00 未还 PS01 已还 PS02 部分还'
  , OVER_FLAG VARCHAR(4) COMMENT '是否逾期 OF00 否 OF01 是'
  , CURR_OD_INT DECIMAL(22, 2) COMMENT '本期罚息'
  , SETL_PRCP DECIMAL(22, 2) COMMENT '本期已还本金'
  , SETL_INT DECIMAL(22, 2) COMMENT '本期已还利息'
  , SETL_OD_INT DECIMAL(22, 2) COMMENT '本期已还罚息'
  , SERVICE_FEE DECIMAL(22, 2) COMMENT '服务费'
  , MANAGER_FEE DECIMAL(22, 2) COMMENT '管理费'
  , RPRY_FLAG VARCHAR(4) COMMENT '代偿/核销标志 RF01：代偿 RF02：核销 为空时，表示没有发生代偿/核销'
  , RPFY_AMT DECIMAL(22, 2) COMMENT '代偿/核销本金'
  , RPFY_INTS DECIMAL(22, 2) COMMENT '代偿/核销利息'
  , RPRY_OD_INT DECIMAL(22, 2) COMMENT '代偿/核销罚息'
  , DUE_NO VARCHAR(30) COMMENT '借据号'
  , RECEIPT_NO VARCHAR(32) COMMENT '回收号'
  , SURP_AMT DECIMAL(22, 2) COMMENT '本期贷款剩余金额'
  , CURR_COM_INT DECIMAL(22, 2) COMMENT '本期复利'
  , SETL_COM_INT DECIMAL(22, 2) COMMENT '本期已还复利'
  , RPRY_COM_INT DECIMAL(22, 2) COMMENT '代偿/核销复利')
COMMENT '贷款还款计划全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/ln_pay_plan';

--贷款登记簿（蛋壳、友金普惠、优信二手车）全量表:ln_reg    
--抽取方式:全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_LN_REG
(ln_no DECIMAL(20) COMMENT '贷款编号:年份+序列号 序列：SEQ_LN_REG_NO'
  , cif_type VARCHAR(4) COMMENT '客户类型:CT01 个人,CT02 对公'
  , cif_cert_type VARCHAR(4) COMMENT '证件类型:ECIF_CERT_INFO.cert_type'
  , cif_cert_no VARCHAR(32) COMMENT '证件号码'
  , ln_prdt_no VARCHAR(32) COMMENT '贷款产品编号:PRDT_BASE_INFO.PRDT_NO'
  , mangr_no VARCHAR(6) COMMENT '客户经理编号'
  , ln_apply_date VARCHAR(8) COMMENT '申请日期:YYYYMMDD'
  , cur_no VARCHAR(3) COMMENT '币种:人民币CNY'
  , ln_amt DECIMAL(22, 2) COMMENT '贷款金额'
  , ln_term_type VARCHAR(4) COMMENT '贷款期限类型:LT00  年,LT01 月,LT02 日'
  , ln_term DECIMAL(4) COMMENT '贷款期限'
  , cred_score DECIMAL(6) COMMENT '信用评级'
  , enpay_flag VARCHAR(4) COMMENT '是否受托支付:EF00  否,EF01 是'
  , risk_lvl VARCHAR(4) COMMENT '风险等级:RL00 AA,RL01 A,RL02 BB,RL03 B,RL04 CC,RL05 C'
  , ln_auth_amt DECIMAL(22, 2) COMMENT '审批后贷款金额'
  , ln_auth_term_type VARCHAR(4) COMMENT '审批后贷款期限类型:LT00 年,LT01 月,LT02 日'
  , ln_auth_term DECIMAL(4) COMMENT '审批后贷款期限'
  , ln_use VARCHAR(100) COMMENT '贷款用途:装修、教育等'
  , ln_rep_type VARCHAR(4) COMMENT '还款方式:LT00 等额本息；LT01 等额本金；LT02 利随本清；LT03 按期还息到期还本等'
  , ln_loan_date VARCHAR(8) COMMENT '放款日'
  , ln_expire_date VARCHAR(8) COMMENT '到期日:放款日 + 到期期限'
  , ln_surp_amt DECIMAL(22, 2) COMMENT '剩余还款额:剩余本金'
  , ln_rep_intv VARCHAR(4) COMMENT '还款间隔:默认1个月'
  , ln_rep_term_day VARCHAR(4) COMMENT '每期还款日:LD00放款日对日；LD01固定还款日'
  , ln_rep_month_day DECIMAL(4) COMMENT '还款日	:固定日还款日时有效，如：每月20日等'
  , ln_acc_no VARCHAR(32) COMMENT '放款账号:若直接使用电子账户放款和还款，则不需要放款和还款账号等信息'
  , ln_acc_name VARCHAR(120) COMMENT '放款账号户名'
  , ln_acc_bank VARCHAR(32) COMMENT '放款账号开户行'
  , ln_rep_acc_no VARCHAR(32) COMMENT '还款账号'
  , ln_rep_acc_name VARCHAR(120) COMMENT '还款账号户名'
  , ln_rep_acc_bank VARCHAR(32) COMMENT '还款账号开户行'
  , ln_image_no VARCHAR(200) COMMENT '影像资料编号	:多份影像资料用分号隔开'
  , ln_cont_no VARCHAR(20) COMMENT '合同号:信贷返回'
  , ln_sts VARCHAR(4) COMMENT '贷款状态:LS07 待审核,LS08 已审核,LS09 已放款,LS10 审核拒绝,LS11 取消,LS12 处理中,LS13 核心放款失败,LS14 放款成功，代付失败'
  , ln_rate DECIMAL(10, 6) COMMENT '利率:年利率'
  , apply_no DECIMAL(20) COMMENT '申请编号'
  , ln_sts_check VARCHAR(4) COMMENT '审核意见:PC00 拒绝 ,PC01 同意'
  , pack_seqn VARCHAR(20) COMMENT '分包序号:从0开始'
  , out_trade_no VARCHAR(50) COMMENT '合作方交易流水号:由合作方系统内部生成'
  , base_acct_no VARCHAR(50) COMMENT '贷款号	:核心系统返回的贷款账号'
  , acct_seq_no VARCHAR(2) COMMENT '贷款序号:核心系统返回'
  , ln_ls_rep_date VARCHAR(8) COMMENT '上次还款日'
  , url VARCHAR(200) COMMENT '上传文件地址'
  , rar_pwd VARCHAR(200) COMMENT '上传文件解压密码'
  , protocol_code VARCHAR(20) COMMENT '协议编号:LN_AUTH_INFO.protocol_code 支付鉴权时生成'
  , expect_provide_date VARCHAR(8) COMMENT '期望放款日'
  , pay_date_sts VARCHAR(4) COMMENT '还款日期状态'
  , partner_user_id VARCHAR(50) COMMENT '合作方用户唯一标识')
COMMENT '贷款登记簿（蛋壳、友金普惠、优信二手车）全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/ln_reg';

--贷款明细增量表:ln_reg_hst
--抽取方式:全量
CREATE EXTERNAL TABLE IF NOT EXISTS OMI.EXT_LN_REG_HST
(TRC_NO VARCHAR(32) COMMENT '流水号'
  , TRC_CNT DECIMAL(6) COMMENT '流水笔次'
  , TX_DATE VARCHAR(8) COMMENT '交易日期'
  , TX_TIME VARCHAR(8) COMMENT '交易时间'
  , BUS_TYPE VARCHAR(4) COMMENT '业务类型:BT01 贷款申请,BT02 贷款发放,BT03 贷款还款,BT04 提前还款,BT05 逾期代偿,BT06 卡号、手机号变更,BT07 提前出单还款'
  , BP_NO VARCHAR(32) COMMENT ''
  , LN_NO DECIMAL(20) COMMENT '贷款编号'
  , DUE_NO VARCHAR(30) COMMENT '借据号'
  , BASE_ACCT_NO VARCHAR(50) COMMENT '贷款号'
  , ACCT_SEQ_NO VARCHAR(2) COMMENT '贷款序号'
  , LN_CONT_NO VARCHAR(20) COMMENT '合同号'
  , CUR_NO VARCHAR(3) COMMENT '币种'
  , LN_AMT DECIMAL(22, 2) COMMENT '贷款本金'
  , TX_AMT DECIMAL(22, 2) COMMENT '交易金额'
  , INTS DECIMAL(22, 2) COMMENT '利息金额'
  , FEE_AMT DECIMAL(22, 2) COMMENT '手续费金额'
  , PENALTY_INTS DECIMAL(22, 2) COMMENT '罚息金额'
  , LN_RATE DECIMAL(10, 6) COMMENT '利率'
  , LN_ACC_NO VARCHAR(32) COMMENT '放款账号'
  , LN_ACC_NAME VARCHAR(120) COMMENT '放款账号户名'
  , LN_ACC_BANK VARCHAR(32) COMMENT '放款账号开户行'
  , LN_REP_ACC_NAME VARCHAR(120) COMMENT '还款账号户名'
  , LN_REP_ACC_BANK VARCHAR(32) COMMENT '还款账号开户行'
  , PAY_ACC_NO VARCHAR(32) COMMENT '还款本金账号'
  , PAY_INTS_ACC_NO VARCHAR(32) COMMENT '还款利息账号'
  , PAY_FEE_ACC_NO VARCHAR(32) COMMENT '手续费账号'
  , PAY_PENAL_ACC_NO VARCHAR(32) COMMENT '罚息账号'
  , TX_STS VARCHAR(4) COMMENT '交易状态:TS00 处理中,TS01 交易成功,TS02 交易失败'
  , FAIL_REASON VARCHAR(100) COMMENT '失败原因'
  , DE_BRF VARCHAR(120) COMMENT '摘  要'
  , OPER_NO VARCHAR(12) COMMENT '操作员'
  , CHK_NO VARCHAR(12) COMMENT '复核员'
  , OPER_TRC_NO DECIMAL(12) COMMENT '操作员流水号'
  , CHNL_TYPE VARCHAR(4) COMMENT '渠道类型:CHNL_DEF.CHNL_CODE'
  , CHNL_DATE VARCHAR(8) COMMENT '渠道日期'
  , CHNL_TRC_NO VARCHAR(32) COMMENT '渠道流水'
  , BR_NO VARCHAR(10) COMMENT '合作机构编码:PRDT_PROX_BR_INFO.BR_NO'
  , PARTNER_TRC_NO VARCHAR(32) COMMENT '合作机构流水号:报文头中的THIRD_TRADE_NO'
  , CALL_SYSTEM VARCHAR(4) COMMENT '调用系统:CS01 信贷系统,CS02 核心系统,CS03 支付系统,CS04 核心放款,CS05 核心转账,CS06 查控系统'
  , CURR_CNT DECIMAL(6) COMMENT '当前期次'
  , HAVE_ACC_DEAL VARCHAR(1) COMMENT '是否有账务往来 Y：有,N：无'
  , ACCOUNT_STS VARCHAR(20) COMMENT '入账状态 S：正常 ,R：冲正'
  , DEPEND_TRC_NO VARCHAR(32) COMMENT '从流水:ESB专用'
  , RECEIPT_NO VARCHAR(32) COMMENT '回收号:还款交易专用'
  , PARTNER_USER_ID VARCHAR(50) COMMENT '合作方用户唯一标识'
  , REPAY_NO VARCHAR(32) COMMENT '回收号'
  , LN_REP_ACC_NO VARCHAR(30) COMMENT '还款账号'
  )
COMMENT '贷款明细全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/ln_reg_hst';

--贷款还款明细全量表:ln_repay_list
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS OMI.EXT_LN_REPAY_LIST
(REPAY_TRC_NO VARCHAR(32) COMMENT '互金流水号'
  , SLAVE_TRC_NO VARCHAR(32) COMMENT '互金调核心的全局流水号'
  , PARTNER_ID VARCHAR(8) COMMENT '合作方ID'
  , PRDT_NO VARCHAR(6) COMMENT '产品编号'
  , DUE_NO VARCHAR(32) COMMENT '互金借据号'
  , CONT_NO VARCHAR(32) COMMENT '互金合同号'
  , CERT_TYPE VARCHAR(32) COMMENT '证件类型同ECIF_CERT_INFO.CERT_TPE'
  , CERT_NO VARCHAR(32) COMMENT '证件号码'
  , CIF_NAME VARCHAR(120) COMMENT '客户名'
  , REPAY_DATE VARCHAR(8) COMMENT '还款日期	'
  , PCIPAL_AMT DECIMAL(16, 2) COMMENT '本金'
  , INTST_AMT DECIMAL(16, 2) COMMENT '正常息'
  , PUN_INTST_AMT DECIMAL(16, 2) COMMENT '罚息	'
  , COMP_INTST_AMT DECIMAL(16, 2) COMMENT '复利:1 等额本息 2 等额本金 4 一次还本付息（默认） 5 按频率付息、一次还本'
  , REPAY_ACC_NO VARCHAR(32) COMMENT '还款账号	'
  , REPAY_SOURCE VARCHAR(2) COMMENT '还款资金来源:（1-还款账户、2-客户保证金账户、3-平台保证金账户）'
  , CHNL_TRACE VARCHAR(32) COMMENT '三方请求流水')
COMMENT '贷款还款明细全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/ln_repay_list';

--账户基本信息全量表:mb_acct
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_MB_ACCT
(INTERNAL_KEY DECIMAL(15) COMMENT '账户标识符'
  , BASE_ACCT_NO VARCHAR(50) COMMENT '账号'
  , PROD_TYPE VARCHAR(50) COMMENT '产品类型'
  , CCY VARCHAR(3) COMMENT '账户币种，对于AIO账户和一本通账户，账户币种为XXX'
  , ACCT_SEQ_NO VARCHAR(8) COMMENT '账户序列号，采用顺序数字，表示在同一账号、账户类型、币种下的不同子账户，比如定期存款序列号'
  , CARD_NO VARCHAR(50) COMMENT '卡号'
  , BRANCH VARCHAR(20) COMMENT '开户行'
  , CLIENT_NO VARCHAR(20) COMMENT '客户号'
  , PROFIT_CENTRE VARCHAR(12) COMMENT '利润中心'
  , ACCT_OPEN_DATE VARCHAR(8) COMMENT '开户日期'
  , EFFECT_DATE VARCHAR(8) COMMENT '生效日期'
  , OPEN_TRAN_DATE VARCHAR(8) COMMENT '开户后首次交易日期'
  , ACCT_STATUS VARCHAR(1) COMMENT '账户状态  H：待激活 A：活动 D：睡眠  S：久悬  O：转营业外 C：关闭 P：逾期   N：新建   I：预开户  以下废弃： R：激活  C：自动关闭  E：手工关闭 M：到期已结清  B：未到期结清'
  , ACCT_STATUS_PREV VARCHAR(1) COMMENT '账户状态  H：待激活 A：活动 D：睡眠  S：久悬  O：转营业外 C：关闭 P：逾期     以下废弃： R：撤销  C：自动关闭  E：手工关闭 M：到期已结清  B：未到期结清'
  , ACCT_STATUS_UPD_DATE VARCHAR(8) COMMENT '账户状态变更日期'
  , ACCOUNTING_STATUS VARCHAR(3) COMMENT '核算状态'
  , ACCOUNTING_STATUS_PREV VARCHAR(3) COMMENT '前次核算状态'
  , ACCOUNT_STATUS_UPD_DATE VARCHAR(8) COMMENT '核算状态变更日期'
  , ACCT_NAME VARCHAR(200) COMMENT '账户名称，一般指中文账户名称'
  , ALT_ACCT_NAME VARCHAR(200) COMMENT '备用账户名称，一般指英文账户名称'
  , ACCT_DESC VARCHAR(150) COMMENT '账户描述'
  , ACCT_NATURE VARCHAR(4) COMMENT '账户属性 在fm_acct_nature_def中定义'
  , OWNERSHIP_TYPE VARCHAR(2) COMMENT '归属种类 AS-多客户联名  SG-独立账户 SU-继承账户'
  , ACCT_EXEC VARCHAR(30) COMMENT '客户经理'
  , ACCT_CLOSE_DATE VARCHAR(8) COMMENT '关闭日期'
  , ACCT_CLOSE_REASON VARCHAR(200) COMMENT '关闭原因'
  , ACCT_CLOSE_USER_ID VARCHAR(30) COMMENT '关闭柜员'
  , ALL_DEP_IND VARCHAR(1) COMMENT '通存标志'
  , ALL_DRA_IND VARCHAR(1) COMMENT '通兑标志'
  , ACCT_DUE_DATE VARCHAR(8) COMMENT '账户有效日期  特指临时户和验资户的到期日期'
  , ACCT_LICENSE_NO VARCHAR(20) COMMENT '账户许可证号'
  , ACCT_LICENSE_DATE VARCHAR(8) COMMENT '账户许可证签发日期'
  , PARENT_INTERNAL_KEY DECIMAL(15) COMMENT '上级账户标识符'
  , LEAD_ACCT_FLAG VARCHAR(1) COMMENT '是否顶层账户 Y:是 N:否'
  , MULTI_BAL_TYPE VARCHAR(1) COMMENT '是否多余额 Y:是 N:否'
  , OSA_FLAG VARCHAR(1) COMMENT '离岸标记 I: inland(本地) O: offshore(离岸)'
  , REGION_FLAG VARCHAR(1) COMMENT '区内区外标记 I: In region O: Out of region'
  , APPR_LETTER_NO VARCHAR(30) COMMENT '核准件编号'
  , TERM VARCHAR(5) COMMENT '期限,整型数字'
  , TERM_TYPE VARCHAR(1) COMMENT '期限类型 D:日 M:月 Y:年'
  , MATURITY_DATE VARCHAR(8) COMMENT '账户到期日'
  , APPLY_BRANCH VARCHAR(20) COMMENT '账户申请机构 LOAN_MANAGER'
  , HOME_BRANCH VARCHAR(20) COMMENT '账户归属机构 BOOK_BRANCH'
  , TERMINAL_ID VARCHAR(200) COMMENT '终端ID'
  , USER_ID VARCHAR(30) COMMENT '开户柜员ID'
  , LAST_CHANGE_USER_ID VARCHAR(30) COMMENT '上次修改柜员'
  , LAST_CHANGE_DATE VARCHAR(8) COMMENT '上次修改日期'
  , AUTO_RENEW_ROLLOVER VARCHAR(1) COMMENT '自动转存 W:本金自动转存 O:本息自动转存'
  , TIMES_RENEWED VARCHAR(5) COMMENT '已本金转存次数'
  , TIMES_ROLLEDOVER VARCHAR(5) COMMENT '已本息转存次数'
  , RENEW_NO VARCHAR(5) COMMENT '本金转存次数'
  , ROLLOVER_NO VARCHAR(5) COMMENT '本息转存次数'
  , PARTIAL_RENEW_ROLL VARCHAR(1) COMMENT '部分允许本金转存 Y或者N'
  , ADDTL_PRINCIPAL VARCHAR(1) COMMENT '是否允许增加本金 Y或者N'
  , LAST_MVMT_STATUS VARCHAR(1) COMMENT '定期账户上一次更改状态 I:利息支出 O:本息转存 P:部分提前支取 R:到期支取 W:部分提前支取 U:到期转入活期 F:全部提前支取 A:本金增加'
  , NOTICE_PERIOD VARCHAR(5) COMMENT '通知期限'
  , ORIG_ACCT_OPEN_DATE VARCHAR(8) COMMENT '账户原始开立日期，即第一次开立日期，未进行过转存的首次开立日期'
  , ORI_MATURITY_DATE VARCHAR(8) COMMENT '账户原始到期日期，即第一次开立时的到期日期，未进行期限变更时的到期日'
  , ACCT_VERIFICATION_IND VARCHAR(1) COMMENT '对账标识 Y-已对账 N-未对账'
  , LENDER VARCHAR(20) COMMENT '贷款人'
  , SUB_PROJECT_NO VARCHAR(30) COMMENT '子项目号'
  , FIVE_CATEGORY VARCHAR(6) COMMENT '贷款五级分类'
  , DAC_VALUE VARCHAR(32) COMMENT 'DAC值  防篡改加密'
  , COMPANY VARCHAR(20) COMMENT '法人代码'
  , CMISLOAN_NO VARCHAR(50) COMMENT '借据号'
  , XRATE DECIMAL(15, 8) COMMENT '汇率'
  , XRATE_ID VARCHAR(1) COMMENT '汇兑方式'
  , REASON_CODE VARCHAR(6) COMMENT '原因代码'
  , MM_REF_NO VARCHAR(50) COMMENT '资金交易参考号'
  , SETTLE VARCHAR(1) COMMENT '结算标志'
  , SETTLE_USER_ID VARCHAR(30) COMMENT '结算柜员'
  , SETTLE_DATE VARCHAR(8) COMMENT '结算日期'
  , APPROVAL_STATUS VARCHAR(1) COMMENT '复核标志'
  , APPR_USER_ID VARCHAR(30) COMMENT '复核柜员'
  , APPROVAL_DATE VARCHAR(8) COMMENT '复核日期'
  , SCHED_MODE VARCHAR(3) COMMENT '计划方式'
  , SOURCE_TYPE VARCHAR(10) COMMENT '开户渠道'
  , SOURCE_MODULE VARCHAR(2) COMMENT '源模块'
  , BUSINESS_UNIT VARCHAR(10) COMMENT '账套'
  , CLIENT_TYPE VARCHAR(3) COMMENT '客户类型'
  , AUTO_SETTLE VARCHAR(1) COMMENT '自动结算标志'
  , INT_IND VARCHAR(1) COMMENT '是否计息标志'
  , MAIN_BAL_FLAG VARCHAR(1) COMMENT '主账户是否带余额'
  , MAIN_INT_FLAG VARCHAR(1) COMMENT '主账户是否带利息'
  , ACCT_REAL_FLAG VARCHAR(1) COMMENT '账户虚实标志  Y-实账户  N-虚账户'
  , SUB_SCHED_MODE VARCHAR(3) COMMENT '当前子计划方式'
  , CUR_STAGE_NO VARCHAR(30) COMMENT '当前期次'
  , GL_TYPE VARCHAR(2) COMMENT '总账类型  I-内部账  N-往帐  V-来帐'
  , ACCT_CLASS VARCHAR(2) COMMENT '账户类别  1：一类账户  2：二类账户  3：三类账户'
  , ACCT_TYPE VARCHAR(2) COMMENT '账户类型：  A-AIO账户,C-结算账户,S-储蓄账户,T-定期账户,M-普通贷款,D-垫款,U-贴现贷款,E-委托贷款,Y-银团贷款,G-保函'
  , FIXED_CALL VARCHAR(1) COMMENT '定期账户细类  A协议存款  B定期一本通  C通知存款  D定活两便  E教育储蓄  F整存争取  L零存整取'
  , LAST_TRAN_DATE VARCHAR(8) COMMENT '最后交易日期'
  , DORMANT_DATE VARCHAR(8) COMMENT '转不动户日期'
  , ACCT_STOP_PAY VARCHAR(1) COMMENT '账户余额止付标志 Y-止付 N-可付'
  , REGEN_SCHEDULE VARCHAR(1) COMMENT '重新生成还款计划标志'
  , BAL_TYPE VARCHAR(2) COMMENT '账户余额标志  TT：汇户  CA：钞户')
COMMENT '账户基本信息全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/mb_acct';

--账户余额全量表  mb_acct_balance
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_MB_ACCT_BALANCE
(INTERNAL_KEY DECIMAL(15) COMMENT '账户标识符'
  , AMT_TYPE VARCHAR(10) COMMENT '金额类型 PRI:本金 BAL:余额 OSL:未到期本金 PD:逾期本金 PF:净本金 DDA:发放金额'
  , TOTAL_AMOUNT DECIMAL(17, 2) COMMENT '汇总金额'
  , CA_AMOUNT DECIMAL(17, 2) COMMENT '当日钞余额'
  , TT_AMOUNT DECIMAL(17, 2) COMMENT '当日汇余额'
  , TOTAL_AMOUNT_PREV DECIMAL(17, 2) COMMENT '上日总金额'
  , CA_AMOUNT_PREV DECIMAL(17, 2) COMMENT '上日钞余额'
  , TT_AMOUNT_PREV DECIMAL(17, 2) COMMENT '上日汇余额'
  , LAST_CHANGE_USER_ID VARCHAR(30) COMMENT '上次动户柜员编号'
  , LAST_CHANGE_DATE VARCHAR(8) COMMENT '上次动户日期'
  , DAC_VALUE VARCHAR(32) COMMENT 'DAC值  防篡改加密'
  , COMPANY VARCHAR(20) COMMENT '法人代码')
COMMENT '账户余额全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/mb_acct_balance';

--产品基本信息全量表 prdt_base_info
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_PRDT_BASE_INFO
(PRDT_NO VARCHAR(32) COMMENT '产品编号 前两位：DE存款,FN理财,LN贷款,IN内部,PO积分,VI虚拟币,CO优惠券,RE红包,FU 基金 后四位：数字序号'
  , PRDT_NAME VARCHAR(60) COMMENT '产品名称'
  , PRDT_BELONG VARCHAR(4) COMMENT '产品归属 PB00:资产类 PB01:负债类 PB02 :所有者权益类 PB03:资产负债共同类 PB04:损益类 PB05:表外类'
  , PRDT_CUR VARCHAR(3) COMMENT '产品币种 对应COM_CUR_CODE.cur_no'
  , PRDT_OBJ VARCHAR(4) COMMENT '适用对象 PO00:个人和对公 PO01:个人 PO02:对公'
  , PRDT_MAGR VARCHAR(20) COMMENT '产品经理'
  , PRDT_BEG_DATE VARCHAR(8) COMMENT '开发日期'
  , PRDT_VER VARCHAR(20) COMMENT '产品版本'
  , PRDT_STS VARCHAR(4) COMMENT '产品销售状态 PS00:新增 PS01:审核中 PS02:待发布 PS03:发布(只能看，不能买，用于抢购) PS04:预售中 PS05:预售结束，划款计息 PS06:锁定期中(不可购买，不可赎回) PS07:锁定结束 PS08:利息结算，分润 PS09:关闭购买 PS10:零金额关闭 PS11:禁用 PS12:待下架 PS13:下架审核完毕 PS14:已下架 PS21:预约认购期[理财] PS22:募集期[理财] PS23:冻结期[理财] PS24:封闭期[理财] PS25:开放期[理财]'
  , PRDT_DESC VARCHAR(200) COMMENT '产品描述'
  , PRDT_AGMT_PATH VARCHAR(256) COMMENT '产品合同或协议模板地址 文件服务器存储'
  , PRDT_APPR_CERT_NO VARCHAR(40) COMMENT '产品审批人身份证'
  , PRDT_APPR_NAME VARCHAR(120) COMMENT '产品审批人姓名'
  , PRDT_DESGR_CERT_NO VARCHAR(40) COMMENT '产品设计人身份证'
  , PRDT_DESGR_NAME VARCHAR(120) COMMENT '产品设计人姓名'
  , PRDT_SEQN_TYPE VARCHAR(32) COMMENT '产品序号类型 用于产品编号的生成和此产品下的账户生成规则。对应表ACC_SEQN_RULE.acc_seqn_type'
  , PRDT_SELF_FLAG VARCHAR(4) COMMENT '自营标志'
  , PRDT_CIF_LVL VARCHAR(30) COMMENT '客户级别'
  , BR_NO VARCHAR(10) COMMENT '机构编码')
COMMENT '产品基本信息全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/prdt_base_info';

--代收付_主表增量表 tpay_main
--抽取方式：增量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_TPAY_MAIN
(REQ_DATE VARCHAR(8) COMMENT '请求日期'
  , REQ_SYSCODE VARCHAR(36) COMMENT '请求系统标识'
  , REQ_CHANNEL VARCHAR(36) COMMENT '请求渠道'
  , TX_DATE VARCHAR(8) COMMENT '交易日期 YYYYMMDD'
  , TX_TIME VARCHAR(12) COMMENT '交易时间 hhmmss'
  , SND_CHANNEL VARCHAR(36) COMMENT '发起渠道'
  , BR_NO VARCHAR(36) COMMENT '交易机构'
  , TELLER VARCHAR(36) COMMENT '交易柜员'
  , MCHNT_CD VARCHAR(128) COMMENT '商户编码'
  , BUSTYPE VARCHAR(11) COMMENT '业务类型 1000 充值'
  , GLOBAL_TRACENO VARCHAR(50) COMMENT '全局流水号'
  , TX_STAT VARCHAR(4) COMMENT '交易状态 代扣状态： 已登记:D000 已组包:D001 已发送:D002 回执未记账:D003 回执记账中:D004 回执已记账:D005 回执已挂账:D006 回执记账失败:D007 回执已拒绝:D008 代付状态： 已登记:D000 记账中:D009 已记账:D010 记账失败:D011 已删除:D012 已组包:D001 已发送:D002 已成功:D013 已拒绝未冲正:D014 已拒绝冲正中:D015 已拒绝已冲正:D016 冲正失败:D017'
  , TX_TH_TRACE VARCHAR(50) COMMENT '合作方流水号 兴业银行：商户交易流水号 民生银行: 合作方流水号'
  , SW_NO VARCHAR(50) COMMENT '平台流水号'
  , TX_TYPE VARCHAR(2) COMMENT '交易类型 00：本行发起代扣 01：他行发起代扣 10：本行发起代付 11：他行发起代付'
  , PRODUCT_TYPE VARCHAR(36) COMMENT '产品类型'
  , PROT_NO VARCHAR(128) COMMENT '协议号'
  , PROT_CHNL VARCHAR(6) COMMENT '协议通道'
  , TX_OBJECT VARCHAR(6) COMMENT '公私标识'
  , PAYER_BANKCODE VARCHAR(14) COMMENT '付款银行编码'
  , PAYER_BANKNAME VARCHAR(280) COMMENT '付款银行名称'
  , PAYER_TYPE VARCHAR(11) COMMENT '付款账户类型'
  , PAYER_NO VARCHAR(32) COMMENT '付款账号'
  , PAYER_NM VARCHAR(280) COMMENT '付款户名'
  , PAYEE_BANKCODE VARCHAR(14) COMMENT '收款银行编码'
  , PAYEE_BANKNAME VARCHAR(280) COMMENT '收款银行名称'
  , PAYEE_TYPE VARCHAR(11) COMMENT '收款账户类型'
  , PAYEE_NO VARCHAR(32) COMMENT '收款账号'
  , PAYEE_NM VARCHAR(256) COMMENT '收款户名'
  , CURRENCY VARCHAR(11) COMMENT '币种'
  , TX_AMT VARCHAR(32) COMMENT '交易金额 单位元'
  , TX_CHARGE VARCHAR(32) COMMENT '费用金额 单位元'
  , ID_TYPE VARCHAR(2) COMMENT '证件类型'
  , ID_NO VARCHAR(32) COMMENT '证件号'
  , PROV_CODE VARCHAR(32) COMMENT '省份编码'
  , CITY_CODE VARCHAR(32) COMMENT '城市编码'
  , PHONE_NO VARCHAR(11) COMMENT '手机号'
  , BUSCODE VARCHAR(120) COMMENT '业务号码 比如 充值时候的手机号'
  , HOST_DATE VARCHAR(8) COMMENT '主机日期'
  , HOST_TRACE VARCHAR(50) COMMENT '主机流水'
  , HOST_RETCODE VARCHAR(36) COMMENT '主机返回码'
  , HOST_RETMSG VARCHAR(128) COMMENT '主机返回码信息'
  , CHECK_STAT VARCHAR(2) COMMENT '对账状态 空值或00:未对账 01或1:三方不对，主机平 02:三方不对，主机不平 2或20或21:三方未对平 12:主机未对平 11:对账平，参与对账'
  , CHECK_IDENT VARCHAR(1) COMMENT '对账标识 N:不参与对账 S:记账 R:冲正'
  , RESP_TRACENO VARCHAR(50) COMMENT '报文流水号 兴业: 交易凭证号 民生: 银行处理流水号    跨行:平台处理流水'
  , RESP_DATE VARCHAR(10) COMMENT '银行交易日期 民生跨行代扣：清算日期'
  , RESP_TIME VARCHAR(14) COMMENT '银行交易时间'
  , USAGE VARCHAR(256) COMMENT '用途说明'
  , RESP_CHARGE VARCHAR(32) COMMENT '渠道手续费'
  , REMARK VARCHAR(512) COMMENT '附言说明'
  , SYSCODE VARCHAR(36) COMMENT '交易系统'
  , CHECK_TH_STS VARCHAR(12) COMMENT '明细对账状态 00:初始未对账 01:行内有，行外无 10:行内无，行外有 11:明细对账平 12:明细对账不平 13:对账日后对平'
  , CHECK_HT_STS VARCHAR(12) COMMENT '主机核对状态 00初始未对账 01核心有，支付无 10核心无，支付有 11主机对账平 12主机对账不平'
  , CHECK_DATE VARCHAR(12) COMMENT '对账日期'
  , CLEAR_DATE VARCHAR(12) COMMENT '清算日期'
  , CLEAR_STS VARCHAR(12) COMMENT '清算状态 00:未清算 01:对平待清算 02:未对平待处理 03:清算中 04:已清算'
  , CLEAR_TYPE VARCHAR(12) COMMENT '清差方式 00:自动清差 01:人工清差')
COMMENT '代收付_主表增量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/tpay_main';

--协议表全量表 tpay_protocol
--抽取方式：全量
CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_TPAY_PROTOCOL
(AUTH_CHANNEL VARCHAR(36) COMMENT '鉴权渠道类型'
  , AUTH_BANKCODE VARCHAR(14) COMMENT '鉴权银行编码'
  , AUTH_BANKNAME VARCHAR(280) COMMENT '鉴权银行名称'
  , SIGN_DATE VARCHAR(8) COMMENT '签约日期 YYYYMMDD'
  , SIGN_TIME VARCHAR(12) COMMENT '签约时间 hhmmss'
  , SIGN_BRNO VARCHAR(35) COMMENT '签约机构号'
  , SIGN_TELLER VARCHAR(35) COMMENT '签约柜员号'
  , SIGN_GLOBAL_TRACENO VARCHAR(50) COMMENT '业务流水号'
  , SIGN_TH_TRACE VARCHAR(50) COMMENT '合作方流水号 兴业银行：商户交易流水号 民生银行: 合作方流水号'
  , SIGN_SW_NO VARCHAR(50) COMMENT '签约平台流水号'
  , SURR_DATE VARCHAR(8) COMMENT '解约日期 解约是登记'
  , SURR_TIME VARCHAR(12) COMMENT '解约时间 解约是登记'
  , SURR_G_TRACENO VARCHAR(50) COMMENT '解约全局流水号 解约是登记'
  , SURR_SW_NO VARCHAR(50) COMMENT '解约平台流水号 解约是登记'
  , SURR_BRNO VARCHAR(36) COMMENT '解约机构号 三方解约时为虚拟机构 柜面解约为发起机构 解约是登记'
  , SURR_TELLER VARCHAR(36) COMMENT '解约柜员号 三方解约时为虚拟柜员 柜面解约为发起柜员 解约是登记'
  , PROT_TYPE VARCHAR(64) COMMENT '协议类型 01：代扣 02：代付 03：快捷支付'
  , PRODUCT_TYPE VARCHAR(36) COMMENT '产品类型'
  , PROT_NO VARCHAR(128) COMMENT '协议号'
  , PROT_STAT VARCHAR(5) COMMENT '协议状态 已登记:D000 已组包:D001 已发送:D002 认证失败:D017 认证成功:D018 认证中:D019 已解约:D020'
  , PROT_ROUTE VARCHAR(11) COMMENT '协议渠道 CIB:兴业银银 CMBCB：民生厦门本行 CMBCO：民生厦门他行 CHAN：畅捷支付'
  , OTH_ACC_TYPE VARCHAR(2) COMMENT '账号类型 00:不确定 10：个人户 11：个人一类户 12：个人二类户 13：个人三类户 20：对公 30：内部户'
  , OTH_ACC_NAME VARCHAR(280) COMMENT '他行户名'
  , OTH_ACC_NO VARCHAR(32) COMMENT '他行账号'
  , ID_TYPE VARCHAR(2) COMMENT '证件类型'
  , ID_NO VARCHAR(32) COMMENT '证件号'
  , PHONE_NO VARCHAR(11) COMMENT '手机号'
  , RESP_FLOW VARCHAR(32) COMMENT '银行处理流水号'
  , RESP_DATE VARCHAR(8) COMMENT '银行交易日期 以该日期进行对账'
  , RESP_TIME VARCHAR(6) COMMENT '银行交易时间'
  , MCHNT_CD VARCHAR(128) COMMENT '商户编码'
  , REQ_DATE VARCHAR(8) COMMENT '请求日期'
  , SIGN_SYSCODE VARCHAR(36) COMMENT '签约系统标识'
  , SURR_SYSCODE VARCHAR(36) COMMENT '解约系统标识'
  , SURR_CHANNEL VARCHAR(36) COMMENT '解约渠道')
COMMENT '协议表全量表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES('input.delimited' = '|+|')
STORED AS TEXTFILE
LOCATION '/dw/tbo/tpay_protocol';

--贷款产品信息中间表_实际-MID_LN_PRDT_BASE_INFO_ACTUAL
--全量
CREATE TABLE IF NOT EXISTS omi.MID_LN_PRDT_BASE_INFO_ACTUAL
(PR_NO VARCHAR(6) COMMENT '序号，用于计算放款额'
  , DUE_NO VARCHAR(30) COMMENT '借据号 年份+10位序号(供14位)'
  , PRDT_NO VARCHAR(32) COMMENT '产品编号 前两位：DE存款,FN理财,LN贷款,IN内部,PO积分,VI虚拟币,CO优惠券,RE红包,FU 基金 后四位：数字序号'
  , PRDT_NAME VARCHAR(60) COMMENT '产品名称'
  , LN_RATE DECIMAL(10, 6) COMMENT '借款利率'
  , PCIPAL_AMT DECIMAL(16, 2) COMMENT '本金'
  , INTST_AMT DECIMAL(16, 2) COMMENT '正常息'
  , PUN_INTST_AMT DECIMAL(16, 2) COMMENT '罚息	'
  , COMP_INTST_AMT DECIMAL(16, 2) COMMENT '复利:1 等额本息 2 等额本金 4 一次还本付息（默认） 5 按频率付息、一次还本'
  , DUE_AMT DECIMAL(22, 2) COMMENT '放款额'
  , BEG_DATE VARCHAR(8) COMMENT '放款日'
  , END_DATE VARCHAR(8) COMMENT '到期日'
  , PAY_STATE VARCHAR(4) COMMENT '支付状态 1:未支付 2:部分支付 3:支付完成 是否结清表示，3结清'
  , REPAY_DATE VARCHAR(8) COMMENT '还款日期'
  , DIF_DAYS DECIMAL(6) COMMENT '贷款放款日至到期日的实际天数'
  , PRJT_NAME VARCHAR(60) COMMENT '项目名称')
COMMENT '贷款产品信息中间表_实际'
CLUSTERED BY(DUE_NO)
INTO 10 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

--贷款产品信息中间表_计划-MID_LN_PRDT_BASE_INFO_PLAN
--全量
CREATE TABLE IF NOT EXISTS omi.MID_LN_PRDT_BASE_INFO_PLAN
(PR_NO VARCHAR(6) COMMENT '序号，用于计算放款额'
  , DUE_NO VARCHAR(30) COMMENT '借据号 年份+10位序号(供14位)'
  , PRDT_NO VARCHAR(32) COMMENT '产品编号 前两位：DE存款,FN理财,LN贷款,IN内部,PO积分,VI虚拟币,CO优惠券,RE红包,FU 基金 后四位：数字序号'
  , PRDT_NAME VARCHAR(60) COMMENT '产品名称'
  , LN_RATE DECIMAL(10, 6) COMMENT '借款利率'
  , SETL_INT DECIMAL(22, 2) COMMENT '正常还息(计划)'
  , SETL_OD_INT DECIMAL(22, 2) COMMENT '罚息(计划)'
  , SETL_COM_INT DECIMAL(22, 2) COMMENT '复利(计划)'
  , DUE_AMT DECIMAL(22, 2) COMMENT '放款额'
  , BEG_DATE VARCHAR(8) COMMENT '放款日'
  , END_DATE VARCHAR(8) COMMENT '到期日'
  , PAY_STATE VARCHAR(4) COMMENT '支付状态 1:未支付 2:部分支付 3:支付完成 是否结清表示，3结清'
  , LAST_TRADE_DATE VARCHAR(8) COMMENT '最后交易日'
  , DIF_DAYS DECIMAL(6) COMMENT '贷款放款日至到期日的实际天数'
  , PRJT_NAME VARCHAR(60) COMMENT '项目名称')
COMMENT '贷款产品信息中间表_计划'
CLUSTERED BY(DUE_NO)
INTO 10 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

--理财魔方日存款余额中间表-MID_MB_ACCT_BALANCE hbase表
--增量
CREATE TABLE IF NOT EXISTS omi.MID_MB_ACCT_BALANCE
(DATA_DATE VARCHAR(8) COMMENT '数据日期'
  , BEG_QUA VARCHAR(6) COMMENT '数据日期所属季度:YYYYQ1/YYYYQ2/YYYYQ3/YYYYQ4'
  , TOTAL_AMOUNT_PREV DECIMAL(17, 2) COMMENT '上日总金额')
COMMENT '理财魔方日存款余额中间表'
CLUSTERED BY(DATA_DATE)
INTO 10 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

--hbase表
CREATE TABLE IF NOT EXISTS omi.ORC_PRO_RELATION  
(PRDT_NAME VARCHAR(60) COMMENT '产品名称'
  , PRJT_NAME VARCHAR(60) COMMENT '项目名称')
COMMENT '项目产品关系表(手动维护)'
CLUSTERED BY(PRDT_NAME)
INTO 1 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

--代收付_主表全量表:orc_tpay_main
--增量
CREATE TABLE IF NOT EXISTS omi.ORC_TPAY_MAIN
(REQ_DATE VARCHAR(8) COMMENT '请求日期'
  , REQ_SYSCODE VARCHAR(36) COMMENT '请求系统标识'
  , REQ_CHANNEL VARCHAR(36) COMMENT '请求渠道'
  , TX_DATE VARCHAR(8) COMMENT '交易日期 YYYYMMDD'
  , TX_TIME VARCHAR(12) COMMENT '交易时间 hhmmss'
  , SND_CHANNEL VARCHAR(36) COMMENT '发起渠道'
  , BR_NO VARCHAR(36) COMMENT '交易机构'
  , TELLER VARCHAR(36) COMMENT '交易柜员'
  , MCHNT_CD VARCHAR(128) COMMENT '商户编码'
  , BUSTYPE VARCHAR(11) COMMENT '业务类型 1000 充值'
  , GLOBAL_TRACENO VARCHAR(50) COMMENT '全局流水号'
  , TX_STAT VARCHAR(4) COMMENT '交易状态 代扣状态： 已登记:D000 已组包:D001 已发送:D002 回执未记账:D003 回执记账中:D004 回执已记账:D005 回执已挂账:D006 回执记账失败:D007 回执已拒绝:D008 代付状态： 已登记:D000 记账中:D009 已记账:D010 记账失败:D011 已删除:D012 已组包:D001 已发送:D002 已成功:D013 已拒绝未冲正:D014 已拒绝冲正中:D015 已拒绝已冲正:D016 冲正失败:D017'
  , TX_TH_TRACE VARCHAR(50) COMMENT '合作方流水号 兴业银行：商户交易流水号 民生银行: 合作方流水号'
  , SW_NO VARCHAR(50) COMMENT '平台流水号'
  , TX_TYPE VARCHAR(2) COMMENT '交易类型 00：本行发起代扣 01：他行发起代扣 10：本行发起代付 11：他行发起代付'
  , PRODUCT_TYPE VARCHAR(36) COMMENT '产品类型'
  , PROT_NO VARCHAR(128) COMMENT '协议号'
  , PROT_CHNL VARCHAR(6) COMMENT '协议通道'
  , TX_OBJECT VARCHAR(6) COMMENT '公私标识'
  , PAYER_BANKCODE VARCHAR(14) COMMENT '付款银行编码'
  , PAYER_BANKNAME VARCHAR(280) COMMENT '付款银行名称'
  , PAYER_TYPE VARCHAR(11) COMMENT '付款账户类型'
  , PAYER_NO VARCHAR(32) COMMENT '付款账号'
  , PAYER_NM VARCHAR(280) COMMENT '付款户名'
  , PAYEE_BANKCODE VARCHAR(14) COMMENT '收款银行编码'
  , PAYEE_BANKNAME VARCHAR(280) COMMENT '收款银行名称'
  , PAYEE_TYPE VARCHAR(11) COMMENT '收款账户类型'
  , PAYEE_NO VARCHAR(32) COMMENT '收款账号'
  , PAYEE_NM VARCHAR(256) COMMENT '收款户名'
  , CURRENCY VARCHAR(11) COMMENT '币种'
  , TX_AMT VARCHAR(32) COMMENT '交易金额 单位元'
  , TX_CHARGE VARCHAR(32) COMMENT '费用金额 单位元'
  , ID_TYPE VARCHAR(2) COMMENT '证件类型'
  , ID_NO VARCHAR(32) COMMENT '证件号'
  , PROV_CODE VARCHAR(32) COMMENT '省份编码'
  , CITY_CODE VARCHAR(32) COMMENT '城市编码'
  , PHONE_NO VARCHAR(11) COMMENT '手机号'
  , BUSCODE VARCHAR(120) COMMENT '业务号码 比如 充值时候的手机号'
  , HOST_DATE VARCHAR(8) COMMENT '主机日期'
  , HOST_TRACE VARCHAR(50) COMMENT '主机流水'
  , HOST_RETCODE VARCHAR(36) COMMENT '主机返回码'
  , HOST_RETMSG VARCHAR(128) COMMENT '主机返回码信息'
  , CHECK_STAT VARCHAR(2) COMMENT '对账状态 空值或00:未对账 01或1:三方不对，主机平 02:三方不对，主机不平 2或20或21:三方未对平 12:主机未对平 11:对账平，参与对账'
  , CHECK_IDENT VARCHAR(1) COMMENT '对账标识 N:不参与对账 S:记账 R:冲正'
  , RESP_TRACENO VARCHAR(50) COMMENT '报文流水号 兴业: 交易凭证号 民生: 银行处理流水号    跨行:平台处理流水'
  , RESP_DATE VARCHAR(10) COMMENT '银行交易日期 民生跨行代扣：清算日期'
  , RESP_TIME VARCHAR(14) COMMENT '银行交易时间'
  , USAGE VARCHAR(256) COMMENT '用途说明'
  , RESP_CHARGE VARCHAR(32) COMMENT '渠道手续费'
  , REMARK VARCHAR(512) COMMENT '附言说明'
  , SYSCODE VARCHAR(36) COMMENT '交易系统'
  , CHECK_TH_STS VARCHAR(12) COMMENT '明细对账状态 00:初始未对账 01:行内有，行外无 10:行内无，行外有 11:明细对账平 12:明细对账不平 13:对账日后对平'
  , CHECK_HT_STS VARCHAR(12) COMMENT '主机核对状态 00初始未对账 01核心有，支付无 10核心无，支付有 11主机对账平 12主机对账不平'
  , CHECK_DATE VARCHAR(12) COMMENT '对账日期'
  , CLEAR_DATE VARCHAR(12) COMMENT '清算日期'
  , CLEAR_STS VARCHAR(12) COMMENT '清算状态 00:未清算 01:对平待清算 02:未对平待处理 03:清算中 04:已清算'
  , CLEAR_TYPE VARCHAR(12) COMMENT '清差方式 00:自动清差 01:人工清差'
  , ETL_DATE VARCHAR(8) COMMENT '数据插入日期')
COMMENT '代收付_主表全量表'
CLUSTERED BY(GLOBAL_TRACENO)
INTO 20 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");
CREATE TABLE IF NOT EXISTS omi.RPT_PARTNER_FEE_HYEAR
(RPT_NO VARCHAR(32) COMMENT '项目编号'
  , PRJT_NAME VARCHAR(60) COMMENT '项目名称'
  , SERVICE_FEE DECIMAL(22, 2) COMMENT '收费金额'
  , DUE_AMT DECIMAL(20) COMMENT '个人客户代付量'
  , AVDY_AMOUNT DECIMAL(20) COMMENT '个人客户代扣量'
  , UNIT_AMT DECIMAL(20) COMMENT '单位客户代付量'
  , AU_TRC DECIMAL(20) COMMENT '鉴权业务量'
  , COUNT_YEAR VARCHAR(6) COMMENT '统计年度:2019Y1表示2019上半年'
  , UPDT_DATE VARCHAR(8) COMMENT '报表统计日期:YYYYMMDD'
  , REMARK VARCHAR(2000) COMMENT '备注')
COMMENT '半年结合作方服务费统计表'
CLUSTERED BY(PRJT_NAME)
INTO 20 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

CREATE TABLE IF NOT EXISTS omi.RPT_PARTNER_FEE_MONTH
(RPT_NO VARCHAR(32) COMMENT '项目编号'
  , PRJT_NAME VARCHAR(60) COMMENT '项目名称'
  , SERVICE_FEE DECIMAL(22, 2) COMMENT '收费金额'
  , SETL_INT DECIMAL(22, 2) COMMENT '月偿还正常利息总额'
  , COUNT_MON VARCHAR(6) COMMENT '统计月份YYYYMM'
  , UPDT_DATE VARCHAR(8) COMMENT '报表统计日期YYYYMMDD'
  , REMARK VARCHAR(2000) COMMENT '备注')
COMMENT '月结合作方服务费统计表'
CLUSTERED BY(PRJT_NAME)
INTO 20 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

CREATE TABLE IF NOT EXISTS omi.RPT_PARTNER_FEE_QUART
(RPT_NO VARCHAR(32) COMMENT '项目编号'
  , PRJT_NAME VARCHAR(60) COMMENT '项目名称'
  , SERVICE_FEE DECIMAL(22, 2) COMMENT '收费金额'
  , DUE_AMT DECIMAL(22, 2) COMMENT '季度放款总额'
  , AVDY_AMOUNT DECIMAL(22, 2) COMMENT '日均存款额'
  , COUNT_QUA VARCHAR(6) COMMENT '统计季度YYYYQ1/YYYYQ2/YYYYQ3/YYYYQ4'
  , UPDT_DATE VARCHAR(8) COMMENT '报表统计日期YYYYMMDD'
  , REMARK VARCHAR(2000) COMMENT '备注')
COMMENT '季结合作方服务费统计表'
CLUSTERED BY(PRJT_NAME)
INTO 20 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional" = "true");

