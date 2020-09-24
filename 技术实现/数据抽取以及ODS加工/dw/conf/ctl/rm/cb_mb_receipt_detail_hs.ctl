
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/cb_mb_receipt_detail_hs.txt'
	truncate
	INTO TABLE dds.cb_mb_receipt_detail_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
receipt_no	,
invoice_tran_no	,
acct_internal_key	,
stage_no	,
amt_type	,
rec_ccy	,
rec_xrate	,
rec_xrate_id	,
rec_amt	,
company	,
tran_timestamp	,
tran_time	,
router_key	
)
