
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/cb_mb_invoice_hs.txt'
	truncate
	INTO TABLE dds.cb_mb_invoice_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
invoice_tran_no	,
internal_key	,
stage_no	,
amt_type	,
int_rate	,
ccy	,
billed_amt	,
outstanding	,
due_date	,
grace_period_date	,
tran_date	,
final_settle_date	,
fully_settled	,
int_from	,
int_to	,
narrative	,
user_id	,
last_change_date	,
company	,
gl_posted	,
tax_type	,
tax_rate	,
tax_amt	,
orig_amt_type	,
invoice_gen_mode	,
tran_timestamp	,
tran_time	,
int_cap	,
router_key	,
reference	
)
