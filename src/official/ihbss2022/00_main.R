##  Primary Controller for the IHBSS 2022 Linkage ------------------------------

if (!exists("ihbss"))
   ihbss <- new.env()

## Get ODK Configuartion
ihbss$`2022`            <- new.env()
ihbss$`2022`$wd         <- file.path(getwd(), "src", "official", "ihbss2022")
ihbss$`2022`$odk$config <- read_sheet(
   "1SNB43JjGOB-uEivT5n8bfGpxiTEKkhNM7izo-KCQKPY",
   "forms"
)

ihbss$`2022`$gdrive <- list(
   "issues"         = "1QqwgW9eAEVFvSNHyp0twL0Ft4oV5jqKuEW8xy20IH64",
   "issues_archive" = "1w8emNjKE562yE69OjbAlfhFpu-2O4qqe",
   "data"           = "1v16uY_V8Y7JWepwltntlKioDmGH4IHx_",
   "data_dir"       = "1xYi3thblJkhL4tajwFJB79wynOwUyZfi",
   "data_archive"   = "106qbNfgxxR1cRe11JT1dUGL8-87gvL6e",
   "submissions"    = "1J_qu2gnazPlUBUWKWYSSExyylH-f78Gv"
)

## Run Pipeline
source(file.path(ihbss$`2022`$wd, "00_fns.R"))
source(file.path(ihbss$`2022`$wd, "01_download_submissions.R"))
source(file.path(ihbss$`2022`$wd, "02_flag_issues.R"))
source(file.path(ihbss$`2022`$wd, "03_export_site_data.R"))
source(file.path(ihbss$`2022`$wd, "04_upload_drive.R"))
source(file.path(ihbss$`2022`$wd, "05_upload_conso.R"))
