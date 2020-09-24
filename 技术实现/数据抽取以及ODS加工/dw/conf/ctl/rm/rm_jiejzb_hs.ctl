
LOAD DATA
        CHARACTERSET UTF8
        INFILE '/data/bigdata/tmp/rm_jiejzb_hs.txt'
        truncate
        INTO TABLE sds.rm_jiejzb_hs FIELDS TERMINATED BY "~"
        TRAILING NULLCOLS
        (
DATA_DT DATE'YYYY-MM-DD',
CIF_NO,
DUE_NO,
PRDT_NO  ,
DUE_COUNT,
DUE_AMT,
ZDYE  ,
ZDYEBJ,
ZDYELX,
YQYE  ,
YQBJ  
)
