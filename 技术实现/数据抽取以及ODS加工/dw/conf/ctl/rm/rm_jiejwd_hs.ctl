
LOAD DATA
        CHARACTERSET UTF8
        INFILE '/data/bigdata/tmp/rm_jinjwd_hs.txt'
        truncate
        INTO TABLE sds.rm_jinjwd_hs FIELDS TERMINATED BY "~"
        TRAILING NULLCOLS
        (
DATA_DT DATE'YYYY-MM-DD',
APPLY_NO,
JCFS,
CIF_NO,
CERT_NO,
CERT_TYPE,
PRDT_NO  ,
PTNER_ID ,
TX_DATE DATE'YYYY-MM-DD',
APP_DATE DATE'YYYY-MM-DD',
SEX      ,
BIRTH_DT 
)
