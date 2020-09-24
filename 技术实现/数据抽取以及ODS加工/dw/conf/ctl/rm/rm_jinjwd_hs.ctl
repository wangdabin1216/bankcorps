
LOAD DATA
        CHARACTERSET UTF8
        INFILE '/data/bigdata/tmp/rm_jiejwd_hs.txt'
        truncate
        INTO TABLE sds.rm_jiejwd_hs FIELDS TERMINATED BY "~"
        TRAILING NULLCOLS
        (
DATA_DT DATE'YYYY-MM-DD',
DUE_NO,
CONT_NO,
CIF_NO,
PRDT_NO ,
BASE_ACCT_NO,
ACCT_SEQ_NO      ,
CMISLOAN_NO      ,
CLIENT_NO        ,
PROD_TYPE        ,
REVERSAL         ,
STATUS           ,
REPAY_TYPE       ,
REPAY_PERIOD     ,
REPAY_DAY_TYP    ,
GRACE_DAYS       ,
LN_USE           ,
REPAY_SOURCE     ,
BEG_DATE         DATE'YYYY-MM-DD',
END_DATE         DATE'YYYY-MM-DD',
STAGE_NO         ,
LN_RATE          ,
OVER_RATE        ,
CURR_OVERDAYS    ,
CURR_OVERMONTHS  ,
LAST_PAY_DATE    DATE'YYYY-MM-DD',
HIS_MAX_PAYDAYS  ,
HIS_MAX_PAYMONTHS,
FIRST_OVER_DATE 
)
