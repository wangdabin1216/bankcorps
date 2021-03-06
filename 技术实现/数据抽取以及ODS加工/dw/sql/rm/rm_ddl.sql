--集市表 DMM.集市简称(2位)_表名
----TABLE:RM_JINJZB---------
DROP TABLE IF EXISTS DMM.RM_JINJZB_HS;
CREATE TABLE DMM.RM_JINJZB_HS ( 
 BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
APPLY_DATE DATE COMMENT '申请日期',
CIF_NO STRING COMMENT '客户号',
APPLY_NO STRING NOT NULL COMMENT '申请件编号',
PRDT_NO STRING NOT NULL COMMENT '产品编号',
JJL DECIMAL(17,0) COMMENT '进件量',
CHECK_NUM   DECIMAL(17,0) COMMENT '核准件量',
REJECT_NUM  DECIMAL(17,0) COMMENT '拒绝件量',
CANCEL_NUM  DECIMAL(17,0) COMMENT '取消件量',
D_COUNT     DECIMAL(17,0) COMMENT '待审件量',
APPR_LIMIT  DECIMAL(17,2) COMMENT '申请额度',
LIMIT_AMT   DECIMAL(17,2) COMMENT '授信额度'
) 
COMMENT '进件指标' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (CIF_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');

----TABLE:RM_JINJWD---------
DROP TABLE IF EXISTS DMM.RM_JINJWD_HS;
CREATE TABLE DMM.RM_JINJWD_HS ( 
 BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
APPLY_NO STRING NOT NULL COMMENT '申请件编号',
JCFS STRING COMMENT '决策方式',
CIF_NO STRING COMMENT '客户号',
CERT_NO STRING NOT NULL COMMENT '证件号码',
CERT_TYPE STRING NOT NULL COMMENT '证件类型',
PRDT_NO   STRING NOT NULL COMMENT '产品编号',
PTNER_ID  STRING COMMENT '合作方',
TX_DATE   DATE NOT NULL COMMENT '进件日期',
APP_DATE  DATE COMMENT '审批日期',
SEX       STRING NOT NULL COMMENT '性别',
BIRTH_DT  STRING NOT NULL COMMENT '出生年月'
) 
COMMENT '进件维度' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (CIF_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');

----TABLE:RM_JIEJZB---------
DROP TABLE IF EXISTS DMM.RM_JIEJZB_HS;
CREATE TABLE DMM.RM_JIEJZB_HS ( 
 BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
CIF_NO STRING NOT NULL COMMENT '客户号',
DUE_NO STRING NOT NULL COMMENT '借据号',
PRDT_NO   STRING NOT NULL COMMENT '产品编号',
DUE_COUNT DECIMAL(17,0) COMMENT '贷款笔数',
DUE_AMT DECIMAL(17,2) COMMENT '贷款金额',
ZDYE   DECIMAL(17,2) COMMENT '在贷余额',
ZDYEBJ DECIMAL(17,2) COMMENT '在贷余额本金',
ZDYELX DECIMAL(17,2) COMMENT '在贷余额利息',
YQYE   DECIMAL(17,2) COMMENT '逾期余额',
YQBJ   DECIMAL(17,2) COMMENT '逾期本金'
) 
COMMENT '借据指标' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');


----TABLE:RM_JIEJWD---------
DROP TABLE IF EXISTS DMM.RM_JIEJWD_HS;
CREATE TABLE DMM.RM_JIEJWD_HS ( 
 BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
DUE_NO STRING NOT NULL COMMENT '借据号',
CONT_NO STRING COMMENT '合同编号',
CIF_NO STRING  COMMENT '互金客户号',
PRDT_NO  STRING COMMENT '产品编号',
BASE_ACCT_NO STRING COMMENT '核心账号',
ACCT_SEQ_NO       STRING COMMENT '核心账户序列号',
CMISLOAN_NO       STRING COMMENT '核心借据号',
CLIENT_NO         STRING COMMENT '核心客户号',
PROD_TYPE         STRING COMMENT '核心产品类型',
REVERSAL          STRING COMMENT '冲正标识',
STATUS            STRING COMMENT '借据状态',
REPAY_TYPE        STRING  COMMENT '还款方式',
REPAY_PERIOD      STRING  COMMENT '还款周期',
REPAY_DAY_TYP     STRING  COMMENT '还款日方式',
GRACE_DAYS        DECIMAL(17,0)  COMMENT '宽限期天数',
LN_USE            STRING  COMMENT '贷款用途',
REPAY_SOURCE      STRING  COMMENT '还款来源',
BEG_DATE          DATE COMMENT '发放日期',
END_DATE          DATE COMMENT '到期日期',
STAGE_NO          STRING COMMENT '期数',
LN_RATE           DECIMAL(17,2)  COMMENT '执行利率',
OVER_RATE         DECIMAL(17,2)  COMMENT '逾期利率',
CURR_OVERDAYS     DECIMAL(17,0)  COMMENT '当前逾期天数',
CURR_OVERMONTHS   DECIMAL(17,0)  COMMENT '当前逾期期数',
LAST_PAY_DATE     DATE  COMMENT '最近一次还款日期',
HIS_MAX_PAYDAYS   STRING  COMMENT '历史最高逾期天数',
HIS_MAX_PAYMONTHS STRING  COMMENT '历史最高逾期期数',
FIRST_OVER_DATE   STRING  COMMENT '首次逾期日期'
) 
COMMENT '借据维度' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');

----TABLE:RM_HKZB---------
DROP TABLE IF EXISTS DMM.RM_HKZB_HS;
CREATE TABLE DMM.RM_HKZB_HS ( 
 BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
CIF_NO STRING COMMENT '互金客户号',
DUE_NO STRING NOT NULL COMMENT '借据号',
STAGE_NO STRING COMMENT '期次',
PRDT_NO STRING  COMMENT '产品编号',
Y_SUM DECIMAL(17,2) COMMENT '应还总额',
Y_SUM_PRI DECIMAL(17,2) COMMENT '应还本金',
Y_SUM_INT DECIMAL(17,2) COMMENT '应还利息',
S_SUM     DECIMAL(17,2) COMMENT '实还总额',
S_SUM_PRI DECIMAL(17,2) COMMENT '实还本金',
S_SUM_INT DECIMAL(17,2) COMMENT '实还利息',
D_SUM     DECIMAL(17,2) COMMENT '已到期总额',
D_SUM_PRI DECIMAL(17,2) COMMENT '已到期本金',
D_SUM_INT DECIMAL(17,2) COMMENT '已到期利息',
W_SUM     DECIMAL(17,2) COMMENT '未到期总额',
W_SUM_PRI DECIMAL(17,2) COMMENT '未到期本金',
W_SUM_INT DECIMAL(17,2) COMMENT '未到期利息'
) 
COMMENT '还款指标' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');

----TABLE:RM_HKWD---------
DROP TABLE IF EXISTS DMM.RM_HKWD_HS;
CREATE TABLE DMM.RM_HKWD_HS ( 
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
OVER_DAYS DECIMAL(17,0) COMMENT '逾期天数',
OVER_MONTHS DECIMAL(17,0) COMMENT '逾期期数',
HIS_MAX_OVERDAYS DECIMAL(17,0) COMMENT '历史最高逾期天数',
HIS_MAX_OVERMONTHS DECIMAL(17,0) COMMENT '历史最高逾期期数'
) 
COMMENT '还款维度' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');


----TABLE:RM_KHZB---------
DROP TABLE IF EXISTS DMM.RM_KHZB_HS;
CREATE TABLE DMM.RM_KHZB_HS ( 
 BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
CIF_NO STRING COMMENT '客户号',
CLIENT_NO STRING COMMENT '核心客户号',
PRDT_NUM  DECIMAL(17,0) COMMENT '持有产品数量',
LIMIT_AMT DECIMAL(17,2) COMMENT '授信额度',
CURR_USE_AMT   DECIMAL(17,2) COMMENT '当前可用额度',
SUM_APPLY_NUM  DECIMAL(17,0) COMMENT '累计授信申请次数',
SUM_CHECK_NUM  DECIMAL(17,0) COMMENT '累计授信核准次数',
SUM_REJECT_NUM DECIMAL(17,0) COMMENT '累计授信拒绝次数',
SUM_DUE_NUM    DECIMAL(17,0) COMMENT '累计贷款次数',
SUM_DUE_AMT    DECIMAL(17,2) COMMENT '累计贷款金额',
ZDYE DECIMAL(17,2) COMMENT '在贷余额',
ZDBJ DECIMAL(17,2) COMMENT '在贷本金',
ZDLX DECIMAL(17,2) COMMENT '在贷利息',
ZDBS DECIMAL(17,0) COMMENT '在贷笔数',
ZDCPS DECIMAL(17,0) COMMENT '在贷产品数',
YQBS DECIMAL(17,0) COMMENT '逾期笔数',
YQBJ DECIMAL(17,2) COMMENT '逾期本金',
JQBS DECIMAL(17,0) COMMENT '结清笔数',
JQBJ DECIMAL(17,2) COMMENT '结清本金',
ZCZDBS DECIMAL(17,0) COMMENT '正常在贷笔数',
ZCZDBJ DECIMAL(17,2) COMMENT '正常在贷本金'
) 
COMMENT '客户指标' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (CIF_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');


----TABLE:RM_KHWD---------
DROP TABLE IF EXISTS DMM.RM_KHWD_HS;
CREATE TABLE DMM.RM_KHWD_HS ( 
 BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
CIF_NO STRING COMMENT '互金客户号',
CLIENT_NO STRING COMMENT '核心客户号',
CERT_NO           STRING COMMENT '证件号', 
CLIENT_TYPE       STRING COMMENT '客户类型', 
CATEGORY_TYPE     STRING COMMENT '客户细分类型', 
CH_CLIENT_NAME    STRING COMMENT '客户姓名', 
ADDRESS           STRING COMMENT '地址', 
INTERNAL_IND      STRING COMMENT '是否为内部客户', 
TRAN_STATUS       STRING COMMENT '客户交易状态', 
CLIENT_INDICATOR  STRING COMMENT '客户标识', 
SOURCE_TYPE       STRING COMMENT '渠道', 
TEMP_CLIENT       STRING COMMENT '是否为临时客户', 
RESIDENT_STATUS   STRING COMMENT '居住状态', 
RACE              STRING COMMENT '种族', 
BIRTH_DATE        STRING COMMENT '出生日期', 
SEX               STRING COMMENT '性别', 
MARITAL_STATUS    STRING COMMENT '婚姻状况', 
OCCUPATION_CODE   STRING COMMENT '职业', 
RESIDENT          STRING COMMENT '居住类型', 
QUALIFICATION     STRING COMMENT '职称', 
EDUCATION         STRING COMMENT '学历', 
MON_SALARY        STRING COMMENT '月收入', 
POST              STRING COMMENT '职务', 
MAX_DEGREE        STRING COMMENT '最高学位', 
YEARLY_INCOME     STRING COMMENT '年收入', 
EMPLOYER_INDUSTRY STRING COMMENT '行业', 
FIRST_APPLY_DT     DATE  COMMENT '首次申请日期', 
FIRST_SX_DT       DATE  COMMENT '首次授信日期', 
FIRST_FK_DT        DATE  COMMENT '首次放款日期', 
FIRST_OVER_DT      DATE  COMMENT '首次逾期日期', 
FIRST_APPLY_PRD_NO STRING COMMENT '首次申请产品编号', 
FIRST_SX_PRD_NO    STRING COMMENT '首次授信产品编号', 
FIRST_FK_PRD_NO    STRING COMMENT '首次放款产品编号', 
FIRST_OVER_PRD_NO  STRING COMMENT '首次逾期产品编号', 
HIS_MAX_SXED       DECIMAL(17,2) COMMENT '历史最高授信额度', 
HIS_MAX_JKJE       DECIMAL(17,2) COMMENT '历史最高借款金额', 
MAX_OVER_DAYS      DECIMAL(17,0) COMMENT '当前最高逾期天数', 
HIS_MAX_OVER_DAYS  DECIMAL(17,0) COMMENT '历史最高逾期天数', 
LATE_OVER_MONTHS   DECIMAL(17,0) COMMENT '最近一次逾期月份'
) 
COMMENT '客户维度' 
--PARTITIONED BY (PARTID STRING) 
CLUSTERED BY (CIF_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true');

----TABLE:RM_JIEJWD_HS_TEMP---------
DROP TABLE IF EXISTS DMM.RM_JIEJWD_HS_TEMP;
CREATE TABLE DMM.RM_JIEJWD_HS_TEMP(
BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
DUE_NO STRING NOT NULL COMMENT '借据号',
CONT_NO STRING COMMENT '合同编号',
CIF_NO STRING COMMENT '互金客户号',
PRDT_NO  STRING COMMENT '产品编号',
BASE_ACCT_NO STRING COMMENT '核心账号',
ACCT_SEQ_NO       STRING COMMENT '核心账户序列号',
CMISLOAN_NO       STRING COMMENT '核心借据号',
CLIENT_NO         STRING COMMENT '核心客户号',
PROD_TYPE         STRING COMMENT '核心产品类型',
REVERSAL          STRING COMMENT '冲正标识',
REPAY_TYPE        STRING  COMMENT '还款方式',
REPAY_PERIOD      STRING  COMMENT '还款周期',
REPAY_DAY_TYP     STRING  COMMENT '还款日方式',
GRACE_DAYS        DECIMAL(17,0)  COMMENT '宽限期天数',
LN_USE            STRING  COMMENT '贷款用途',
REPAY_SOURCE      STRING  COMMENT '还款来源',
BEG_DATE          DATE COMMENT '发放日期',
END_DATE          DATE COMMENT '到期日期',
STAGE_NO          STRING  COMMENT '期数',
LN_RATE           DECIMAL(17,2)  COMMENT '执行利率',
OVER_RATE         DECIMAL(17,2)  COMMENT '逾期利率',
CURR_OVERDAYS     DECIMAL(17,0)  COMMENT '当前逾期天数',
CURR_OVERMONTHS   DECIMAL(17,0)  COMMENT '当前逾期期数',
LAST_PAY_DATE     DATE  COMMENT '最近一次还款日期'
) 
COMMENT '借据维度临时表' 
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true')
;

----TABLE:RM_HKZB_HS_TEMP---------
DROP TABLE IF EXISTS DMM.RM_HKZB_HS_TEMP;
CREATE TABLE DMM.RM_HKZB_HS_TEMP(
BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
CIF_NO STRING COMMENT '互金客户号',
DUE_NO STRING NOT NULL COMMENT '借据号',
STAGE_NO STRING COMMENT '期次',
PRDT_NO STRING  COMMENT '产品编号',
Y_SUM DECIMAL(17,2) COMMENT '应还总额',
Y_SUM_PRI DECIMAL(17,2) COMMENT '应还本金',
Y_SUM_INT DECIMAL(17,2) COMMENT '应还利息',
S_SUM     DECIMAL(17,2) COMMENT '实还总额',
S_SUM_PRI DECIMAL(17,2) COMMENT '实还本金',
S_SUM_INT DECIMAL(17,2) COMMENT '实还利息',
D_SUM     DECIMAL(17,2) COMMENT '已到期总额',
D_SUM_PRI DECIMAL(17,2) COMMENT '已到期本金',
D_SUM_INT DECIMAL(17,2) COMMENT '已到期利息',
W_SUM     DECIMAL(17,2) COMMENT '未到期总额',
W_SUM_PRI DECIMAL(17,2) COMMENT '未到期本金',
W_SUM_INT DECIMAL(17,2) COMMENT '未到期利息'
) 
COMMENT '还款指标临时表'
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true')
;

----TABLE:RM_HKWD_HS_TEMP---------
DROP TABLE IF EXISTS DMM.RM_HKWD_HS_TEMP;
CREATE TABLE DMM.RM_HKWD_HS_TEMP(
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
;


----TABLE:RM_JINJ_PUBLIC---------
DROP TABLE IF EXISTS DMM.RM_JINJ_PUBLIC;
CREATE TABLE DMM.RM_JINJ_PUBLIC(
BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
APPLY_DATE DATE COMMENT '申请日期',
CIF_NO STRING COMMENT '客户号',
APPLY_NO STRING COMMENT '申请件编号',
PRDT_NO STRING  COMMENT '产品编号',
APPR_RESULT STRING COMMENT '审批结果',
APPR_LIMIT  DECIMAL(17,2) COMMENT '申请额度',
CERT_NO STRING COMMENT '证件号码',
CERT_TYPE STRING COMMENT '证件类型',
PTNER_ID  STRING COMMENT '合作方',
TX_DATE   DATE COMMENT '进件日期',
APP_DATE  DATE COMMENT '审批日期'
) 
COMMENT '进件公共表'
CLUSTERED BY (CIF_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true')
;


----TABLE:RM_JIEJHK_PUBLIC---------
DROP TABLE IF EXISTS DMM.RM_JIEJHK_PUBLIC;
CREATE TABLE DMM.RM_JIEJHK_PUBLIC(
BEGNDT DATE COMMENT '生效日期',
OVERDT DATE COMMENT '失效日期' , 
DATA_DT DATE COMMENT '统计日期',
CIF_NO STRING NOT NULL COMMENT '客户号',
DUE_NO STRING NOT NULL COMMENT '借据号',
PRDT_NO   STRING NOT NULL COMMENT '产品编号',
DUE_AMT DECIMAL(17,2) COMMENT '贷款金额',
CONT_NO STRING COMMENT '合同编号',
BASE_ACCT_NO STRING COMMENT '核心账号',
ACCT_SEQ_NO       STRING COMMENT '核心账户序列号',
CMISLOAN_NO       STRING COMMENT '核心借据号',
CLIENT_NO         STRING COMMENT '核心客户号',
PROD_TYPE         STRING COMMENT '核心产品类型',
REVERSAL          STRING COMMENT '冲正标识',
REPAY_TYPE        STRING  COMMENT '还款方式',
REPAY_PERIOD      STRING  COMMENT '还款周期',
REPAY_DAY_TYP     STRING  COMMENT '还款日方式',
GRACE_DAYS        DECIMAL(17,0)  COMMENT '宽限期天数',
LN_USE            STRING  COMMENT '贷款用途',
REPAY_SOURCE      STRING  COMMENT '还款来源',
BEG_DATE          DATE COMMENT '发放日期',
END_DATE          DATE COMMENT '到期日期',
LN_RATE           DECIMAL(17,2)  COMMENT '执行利率',
OVER_RATE         DECIMAL(17,2)  COMMENT '逾期利率',
INTERNAL_KEY      STRING  COMMENT '账户标志符'
) 
COMMENT '借据、还款公共表'
CLUSTERED BY (DUE_NO) INTO 11 BUCKETS 
STORED AS ORC 
TBLPROPERTIES ('transactional'='true')
;