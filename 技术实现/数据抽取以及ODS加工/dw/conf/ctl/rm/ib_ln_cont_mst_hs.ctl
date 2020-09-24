
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/ib_ln_cont_mst_hs.txt'
	truncate
	INTO TABLE dds.ib_ln_cont_mst_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
cont_no	,
loan_cont_no	,
acc_no	,
ln_no	,
cif_no	,
cif_name	,
ptner_id	,
prdt_no	,
cur_no	,
cont_amt	,
cont_bal	,
tot_loan_amt	,
tot_repay_amt	,
cycle_flag	,
bus_knd	,
bus_sub_knd	,
occur_type	,
cap_type	,
term_type	,
cont_term	,
sign_date	,
chg_date	,
beg_date	,
end_date	,
rate_type	,
ln_rate	,
over_rate	,
deft_flag	,
comp_flag	,
vou_type	,
ln_use	,
repay_source	,
pay_mode	,
loan_acc_no	,
repay_acc_no	,
entrusted_acc_no	,
margin_acc_no	,
repay_type	,
repay_period	,
repay_day_typ	,
repay_day	,
grace_days	,
cont_stat	,
app_brno	,
app_op_no	,
app_date	,
mang_brno	,
mang_no	,
acc_brno	,
br_no	,
op_no	,
tx_date	,
up_date	,
filler	,
is_other_entrusted	,
entrusted_acc_name	,
entrusted_bank_dept	,
entrusted_acc_type	,
filler1	,
filler2	,
filler3	,
filler4	,
filler5	,
lpr_date	,
lpr_value	,
add_point	,
sub_point	
)
