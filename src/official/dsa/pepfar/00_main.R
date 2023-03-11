flow_register()
pepfar$steps$`01_load_reqs`$.init(yr = 2022, mo = 12, report = "QR", envir = pepfar)
pepfar$steps$`02_prepare_tx`$.init(pepfar)
pepfar$steps$`03_prepare_prep`$.init(pepfar)
pepfar$steps$`04_prepare_reach`$.init(pepfar)
pepfar$steps$`05_aggregate_data`$.init(pepfar)
pepfar$steps$`06_conso_flat`$.init(pepfar)

