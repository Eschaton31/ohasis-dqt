##  ICAP Data Sharing Controller -----------------------------------------------

# define datasets
if (!exists("icap"))
   icap <- new.env()

icap$wd <- file.path(getwd(), "src", "official", "dsa_icap")

##  Begin linkage of datasets --------------------------------------------------

source(file.path(icap$wd, "01_load_reqs.R"))
source(file.path(icap$wd, "02-00_data-tx_indicators.R"))
source(file.path(icap$wd, "02-01_data-tx_curr.R"))
source(file.path(icap$wd, "02-02_data-tx_new.R"))
source(file.path(icap$wd, "02-03_data-tx_ml.R"))
source(file.path(icap$wd, "02-04_data-tx_rtt.R"))
# source(file.path(icap$wd, "02-05_data-tx_pvls_eligible.R"))
# source(file.path(icap$wd, "02-06_data-tx_pvls.R"))
source(file.path(icap$wd, "03-01_data-prep_curr.R"))
source(file.path(icap$wd, "03-02_data-prep_new.R"))
source(file.path(icap$wd, "03-03_data-prep_ct.R"))
source(file.path(icap$wd, "03-04_data-prep_screen.R"))
source(file.path(icap$wd, "03-05_data-prep_elig.R"))
source(file.path(icap$wd, "03-06_data-prep_ineligible.R"))
source(file.path(icap$wd, "04-01_data-hts_tst.R"))
source(file.path(icap$wd, "04-02_data-kp_prev.R"))
source(file.path(icap$wd, "04-03_hts_advanced.R"))
source(file.path(icap$wd, "05-01_data-tx_new_verify.R"))
source(file.path(icap$wd, "05-02_data-hts_tst_verify.R"))
source(file.path(icap$wd, "06_conso_flat.R"))
source(file.path(icap$wd, "07_export.R"))
