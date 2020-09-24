
LOAD DATA
	CHARACTERSET UTF8
	INFILE '/data/bigdata/tmp/cb_cif_client_corp_hs.txt'
	truncate
	INTO TABLE dds.cb_cif_client_corp_hs FIELDS TERMINATED BY "~"
	TRAILING NULLCOLS
	(
begndt	DATE "YYYY-MM-DD",
overdt	DATE "YYYY-MM-DD",
client_no	,
corp_size	,
tax_file_no	,
local_tax_file_no	,
registered_date	,
non_resident_ctrl	,
ownership	,
legal_rep	,
rep_document_type	,
rep_document_id	,
origin_country	,
operation_country	,
emp_num	,
incor_date	,
investor	,
business_scope	char(4000),
basic_acct_no	,
basic_acct_openat	,
corp_plan	,
fx_register_id	,
fx_iss_place	,
capital_ccy	,
auth_capital	,
paid_up_capital	,
organ	,
fx_iss_organ	,
swift_id	,
central_bank_ref	,
bank_no	,
bank_code	,
fitch	,
inp_exp_no	,
higher_organ	,
organ_code	,
econ_dist	,
register_no_type	,
register_no	,
loan_card_id	,
cessation	,
cessation_date	,
off_website	,
director_ind	,
sub_director_ind	,
loan_grade	,
lending_officer_ind	,
sub_lending_officer_ind	,
sp_rate	,
company_secretary	,
minority_interest	,
exposure_cap	,
ref_intermediary	,
phone_fax	,
phone_fax_acct	,
moody_rate	,
pd	,
borrower_grade	,
market_participant	,
last_change_date	,
last_change_user_id	,
paid_capital_ccy	,
rep_expiry_date	,
foreign_app_no	,
fin_app_code	,
tran_email	,
econ_type	,
check_year	,
special_app_no	,
busilicence_status	,
tax_cer_avai	,
fore_remit_cer_avai	,
company	,
rep_iss_date	,
tran_timestamp	,
tran_time	,
router_key	,
cou_higt_tech_type	,
cou_higt_tech_no	,
zgc_higt_tech_type	,
zgc_higt_tech_no	,
oth_higt_tech_type	,
oth_higt_tech_no	,
sci_tech_industry	,
oth_sci_tech_desc	,
tot_assets_ccy	,
tot_assets_com	,
research_ccy	,
research_fee	,
research_inc_rate	
)
