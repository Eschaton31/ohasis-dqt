ohasis <- DB$new("2024", "02", "finalize harp", "2")

harp_dx$steps$`01_load_reqs`$.init(harp_dx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_dx$steps$`02_data_hts_tst_pos`$.init(harp_dx, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")

harp_dead$steps$`01_load_reqs`$.init(harp_dead, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_harp = "1", harp_reprocess = "1")
harp_dead$steps$`02_data_mortality`$.init(harp_dead, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")

harp_tx$steps$`01_load_reqs`$.init(harp_tx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_tx$steps$`02_data_tx_new`$.init(harp_tx, run_checks = "2", upload = "1", exclude_drops = "1")
harp_tx$steps$`03_data_tx_curr`$.init(harp_tx, run_checks = "2", upload = "1", save = "1")

prep$steps$`01_load_reqs`$.init(prep, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_init = "2", update_link = "2", update_harp = "1", harp_reprocess = "1")
prep$steps$`02_data_prep_offer`$.init(prep, run_checks = "2", upload = "1", exclude_drops = "1")
prep$steps$`03_data_prep_curr`$.init(prep, run_checks = "2", upload = "1", save = "1")
