
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/ib_ln_cif_limit_hs.txt'
	truncate
	INTO TABLE dds.ib_ln_cif_limit_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
cert_type	,
cert_no	,
ptner_id	,
cif_name	,
cif_no	,
prdt_no	,
limit_amt	,
stat	,
filler	
)
