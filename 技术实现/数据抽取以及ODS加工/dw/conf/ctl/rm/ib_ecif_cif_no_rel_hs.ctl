
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/ib_ecif_cif_no_rel_hs.txt'
	truncate
	INTO TABLE dds.ib_ecif_cif_no_rel_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
cif_no	,
cif_type	,
rel_sys	,
rel_cif_no	,
rel_time	,
rel_desc	,
br_no	
)
