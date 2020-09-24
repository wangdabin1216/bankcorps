
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/cb_mb_drawdown_hs.txt'
	truncate
	INTO TABLE dds.cb_mb_drawdown_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
internal_key	,
counter	,
base_acct_no	,
prod_type	,
ccy	,
acct_seq_no	,
client_no	,
lender	,
dd_method	,
dd_date	,
dd_amt	,
distinct_int	,
maturity_date	,
tran_branch	,
user_id	,
tran_date	,
tran_timestamp	,
cmisloan_no	,
reference	,
dac_value	,
company	,
event_type	,
tran_type	,
reversal	,
reversal_reason	,
reversal_user_id	,
tran_time	,
router_key	
)
