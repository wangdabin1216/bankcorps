
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/ib_ecif_cert_info_hs.txt'
	truncate
	INTO TABLE dds.ib_ecif_cert_info_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
br_no	,
cif_no	,
cert_type	,
cert_no	,
cert_name	,
cert_desc	,
cert_lic	,
cert_chk	,
cert_net_flag	,
cert_supt	,
cert_main_flag	,
cert_sts	,
cert_beg_date	,
cert_end_date	,
cert_last_date	,
cert_address	,
cert_chk_name	,
cert_chk_vedio	
)
