
LOAD DATA
        CHARACTERSET UTF8
        INFILE '/data/bigdata/tmp/rm_hkzb_hs.txt'
        truncate
        INTO TABLE sds.rm_hkzb_hs FIELDS TERMINATED BY "~"
        TRAILING NULLCOLS
        (
DATA_DT DATE'YYYY-MM-DD',
CIF_NO,
DUE_NO,
STAGE_NO,
PRDT_NO,
Y_SUM,
Y_SUM_PRI,
Y_SUM_INT,
S_SUM    ,
S_SUM_PRI,
S_SUM_INT,
D_SUM    ,
D_SUM_PRI,
D_SUM_INT,
W_SUM    ,
W_SUM_PRI,
W_SUM_INT
)
