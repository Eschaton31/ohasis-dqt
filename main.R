################################################################################
# Project name: > OHASIS Data Wrangling
# Author(s):    > Palo, John Benedict
# Date:         > 2022-01-27
# Description:  > This program serves as the primary controller for the various
#                 data pipelines of the NHSSS Unit.
################################################################################

rm(list = ls())
source("autoload.R")

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB$new("2024", "01", "finalize harp", "2")

##  HARP Diagnosis -------------------------------------------------------------

harp_dx$steps$`01_load_reqs`$.init(harp_dx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_dx$steps$`02_data_hts_tst_pos`$.init(harp_dx, run_checks = "1", upload = "1", exclude_drops = "1", save = "1")
harp_dx$steps$x1_dedup_new$.init(harp_dx, upload = "1")
harp_dx$steps$x2_dedup_old$.init(harp_dx, upload = "1")

harp_dx$steps$y2_saccl_logsheet$.init("R:/File requests/SACCL Submissions/logsheet/logsheet_hiv/saccl-hiv-logsheet_2023.10.01-2023.10.31 Jay Chinjen.xls", "new")
harp_dx$steps$y2_saccl_logsheet$import_data(harp_dx$steps$y2_saccl_logsheet$tables)
harp_dx$steps$y3_saccl_recency$.init()
harp_dx$steps$y3_saccl_recency$import_data(harp_dx$steps$y3_saccl_recency$tables)

##  HARP Treatment -------------------------------------------------------------

harp_tx$steps$`01_load_reqs`$.init(harp_tx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "1", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_tx$steps$`02_data_tx_new`$.init(harp_tx, run_checks = "1", upload = "1", exclude_drops = "1")
harp_tx$steps$`03_data_tx_curr`$.init(harp_tx, run_checks = "1", upload = "1", save = "1")
harp_tx$steps$x1_dedup_new$.init(harp_tx, upload = "1")
harp_tx$steps$x2_dedup_old$.init(harp_tx, upload = "1")
harp_tx$steps$x3_dedup_dx$.init(harp_tx, upload = "1")

##  HARP Mortality -------------------------------------------------------------

harp_dead$steps$`01_load_reqs`$.init(harp_dead, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "1", update_harp = "1", harp_reprocess = "1")
harp_dead$steps$`02_data_mortality`$.init(harp_dead, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")
harp_dead$steps$x1_dedup_new$.init(harp_dead, upload = "1")
harp_dead$steps$x2_dedup_old$.init(harp_dead, upload = "1")
harp_dead$steps$x3_dedup_dx$.init(harp_dead, upload = "1")

##  PrEP -----------------------------------------------------------------------

prep$steps$`01_load_reqs`$.init(prep, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_init = "1", update_link = "1", update_harp = "1", harp_reprocess = "1")
prep$steps$`02_data_prep_offer`$.init(prep, run_checks = "2", upload = "1", exclude_drops = "1")
prep$steps$`03_data_prep_curr`$.init(prep, run_checks = "2", upload = "1", save = "1")
prep$steps$x1_dedup_new$.init(prep, upload = "1")
prep$steps$x2_dedup_old$.init(prep, upload = "1")
prep$steps$x3_dedup_dx$.init(prep, upload = "1")
prep$steps$x4_dedup_tx$.init(prep, upload = "1")

##  Recency --------------------------------------------------------------------

recency$steps$`01_load_reqs`$.init(recency, get_harp = "1", dl_forms = "1", update_lw = "1", update_rt = "1")
recency$steps$`02_data_hts_recent`$.init(recency, run_checks = "2", upload = "1", save = "2")
recency$steps$`03_upload`$.init(recency)

## END -------------------------------------------------------------------------
