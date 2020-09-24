
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/cb_mb_acct_schedule_detail_hs.txt'
	truncate
	INTO TABLE dds.cb_mb_acct_schedule_detail_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
sched_seq_no	,
internal_key	,
stage_no	,
amt_type	,
start_date	,
end_date	,
sched_amt	,
paid_amt	,
pri_outstanding	,
company	,
tran_timestamp	,
tran_time	,
router_key	
)
