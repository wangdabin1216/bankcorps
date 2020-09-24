
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/ib_ln_due_mst_hs.txt'
	truncate
	INTO TABLE dds.ib_ln_due_mst_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
app_sts	,
cont_no	,
due_no	,
cif_no	,
cif_name	,
app_no	,
note_no	,
occur_type	,
prdt_no	,
cur_no	,
ln_term_type	,
beg_date	,
end_date	,
base_rate_type	,
base_rate	,
ln_rate	,
float_ratio	,
over_rate	,
fine_rate	,
cmpd_rate	,
exp_flag	,
exp_rate	,
inner_intst	,
outer_intst	,
due_amt	,
bal	,
acc_no	,
acc_hrt	,
hx_prdt_no	,
deal_flag	,
vou_type	,
pay_acc_no	,
put_acc_no	,
pay_type	,
over_days	,
tot_over_times	,
five_sts	,
four_sts	,
grade	,
due_sts	,
mang_brno	,
mang_no	,
acc_br_no	,
br_no	,
op_no	,
tx_date	,
up_date	,
filler	,
old_end_date	,
ic_type	,
repay_type	,
payee_name	,
payee_ac_no	,
payee_br_name	,
cap_type	,
put_type	,
if_auto	,
spec_cap_type	,
put_name	,
rate_chg_type	,
repay_day_way	,
repay_day	,
first_repay_date	,
repay_term_kind	,
repay_term	,
cont_over_times	,
four_mod_date	,
suit_presp	,
ln_sts	,
last_pay_day	,
pay_amt	,
pay_state	,
reinforce_flag	,
acno_status	,
pay_mon	,
bill_no	,
is_com_flag	,
com_addr	,
cif_tel	,
days	,
ac_id	,
ac_seqn	,
float_type	,
ln_term	,
acct_no	,
grant_no	,
ln_no
)
