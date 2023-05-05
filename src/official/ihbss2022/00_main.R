##  Primary Controller for the IHBSS 2022 Linkage ------------------------------

flow_register()
ihbss$`2022`$steps$`01_load_reqs`$.init(envir = ihbss$`2022`)
ihbss$`2022`$steps$`02_download_odk`$.init(envir = ihbss$`2022`, survey = "msm")
ihbss$`2022`$steps$`02_download_odk`$.init(envir = ihbss$`2022`, survey = "medtech")
ihbss$`2022`$steps$`02_download_odk`$.init(envir = ihbss$`2022`, survey = "fsw")
ihbss$`2022`$steps$`02_download_odk`$.init(envir = ihbss$`2022`, survey = "pwid_m")
ihbss$`2022`$steps$`02_download_odk`$.init(envir = ihbss$`2022`, survey = "pwid_f")
ihbss$`2022`$steps$`03_flag_monitoring`$.init(envir = ihbss$`2022`, survey = "msm")
ihbss$`2022`$steps$`03_flag_monitoring`$.init(envir = ihbss$`2022`, survey = "medtech")
ihbss$`2022`$steps$`03_flag_monitoring`$.init(envir = ihbss$`2022`, survey = "fsw")
ihbss$`2022`$steps$`03_flag_monitoring`$.init(envir = ihbss$`2022`, survey = "pwid_m")
ihbss$`2022`$steps$`03_flag_monitoring`$.init(envir = ihbss$`2022`, survey = "pwid_f")

## Run Pipeline
source(file.path(ihbss$`2022`$wd, "00_fns.R"))
source(file.path(ihbss$`2022`$wd, "02_download_odk.R"))
source(file.path(ihbss$`2022`$wd, "03_flag_monitoring.R"))
source(file.path(ihbss$`2022`$wd, "03_export_site_data.R"))
source(file.path(ihbss$`2022`$wd, "04_upload_drive.R"))
source(file.path(ihbss$`2022`$wd, "05_upload_conso.R"))
