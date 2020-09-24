
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/ib_ln_credit_apply_mst_hs.txt'
	truncate
	INTO TABLE dds.ib_ln_credit_apply_mst_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
credit_apply_no	,
cert_type	,
cert_no	,
ptner_id	,
cif_name	,
prdt_no	,
appr_result	,
appr_limit	,
appr_rate	,
trace_no	,
chnl_trace	,
slave_trace_no	,
app_brno	,
app_op_no	,
app_date	,
app_time	,
tx_date	,
up_date	,
filler	,
send_sts	,
tx_time	,
up_time	,
legalman_id_type	,
legalman_id_no	,
legalman_name	,
legalman_phone	,
bind_bank_card	,
bind_bank_mobile	,
linkman_type	,
linkman_name	,
linkman_mobile	
)
