
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/cb_cif_client_document_hs.txt'
	truncate
	INTO TABLE dds.cb_cif_client_document_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
client_no	,
iss_country	,
document_type	,
document_id	,
iss_date	,
expiry_date	,
iss_place	,
iss_authority	,
pref_flag	,
last_change_date	,
last_change_user_id	,
dist_code	,
iss_state	,
iss_city	,
new_document_id	,
company	,
inspect_date	,
tran_timestamp	,
tran_time	,
router_key	
)
