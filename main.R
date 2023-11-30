################################################################################
# Project name: > OHASIS Data Wrangling
# Author(s):    > Palo, John Benedict
# Date:         > 2022-01-27
# Description:  > This program serves as the primary controller for the various
#                 data pipelines of the NHSSS Unit.
################################################################################

rm(list = ls())

##  Load credentials and authentications ---------------------------------------

# dependencies
source("src/dependencies/options.R")
source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/full_tables.R")
source("src/dependencies/cloud.R")
source("src/dependencies/dedup.R")
source("src/dependencies/googlesheets.R")
source("src/dependencies/excel.R")
source("src/dependencies/latex.R")
source("src/dependencies/reports.R")

# accounts
source("src/dependencies/auth_acct.R")
# source("src/dependencies/gmail.R")

# classes
source("src/classes/Project.R")
source("src/classes/DB.R")

# register pipelines
source("src/pipeline/pipeline.R", chdir = TRUE)
flow_register()

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB("2023", "09", "harp", "2")

##  example flow pipeline ------------------------------------------------------

flow_register()

# diagnosis
harp_dx$steps$`01_load_reqs`$.init(harp_dx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_harp = "1", harp_reprocess = "1")
harp_dx$steps$`02_data_hts_tst_pos`$.init(harp_dx, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")
harp_dx$steps$x1_dedup_new$.init(harp_dx, upload = "1")
harp_dx$steps$x2_dedup_old$.init(harp_dx, upload = "1")

harp_dx$steps$y2_saccl_logsheet$.init()
harp_dx$steps$y2_saccl_logsheet$import_data(harp_dx$steps$y2_saccl_logsheet$tables)
harp_dx$steps$y3_saccl_recency$.init()
harp_dx$steps$y3_saccl_recency$import_data(harp_dx$steps$y3_saccl_recency$tables)

# treatment
harp_tx$steps$`01_load_reqs`$.init(harp_tx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_tx$steps$`02_data_tx_new`$.init(harp_tx, run_checks = "2", upload = "1", exclude_drops = "1")
harp_tx$steps$`03_data_tx_curr`$.init(harp_tx, run_checks = "2", upload = "1", save = "2")
harp_tx$steps$x1_dedup_new$.init(harp_tx, upload = "1")
harp_tx$steps$x2_dedup_old$.init(harp_tx, upload = "1")
harp_tx$steps$x3_dedup_dx$.init(harp_tx, upload = "1")

# dead
harp_dead$steps$`01_load_reqs`$.init(harp_dead, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "1", update_harp = "1", harp_reprocess = "1")
harp_dead$steps$`02_data_mortality`$.init(harp_dead, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")
harp_dead$steps$x1_dedup_new$.init(harp_dead, upload = "1")
harp_dead$steps$x2_dedup_old$.init(harp_dead, upload = "1")
harp_dead$steps$x3_dedup_dx$.init(harp_dead, upload = "1")

# prep
prep$steps$`01_load_reqs`$.init(prep, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "2", update_lw = "2", update_visits = "2", update_init = "2", update_link = "2", update_harp = "1", harp_reprocess = "1")
prep$steps$`02_data_prep_offer`$.init(prep, run_checks = "2", upload = "1", exclude_drops = "1")
prep$steps$`03_data_prep_curr`$.init(prep, run_checks = "2", upload = "1", save = "1")
prep$steps$x1_dedup_new$.init(prep, upload = "1")
prep$steps$x2_dedup_old$.init(prep, upload = "1")
prep$steps$x3_dedup_dx$.init(prep, upload = "1")
prep$steps$x4_dedup_tx$.init(prep, upload = "1")

# recency
recency$steps$`01_load_reqs`$.init(recency, get_harp = "1", dl_forms = "1", update_lw = "2", update_rt = "2")
recency$steps$`02_data_hts_recent`$.init(recency, run_checks = "2", upload = "1", save = "1")
recency$steps$`03_upload`$.init(recency)
