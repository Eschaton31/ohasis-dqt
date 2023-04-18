##  Primary Controller for the IHBSS 2022 Linkage ------------------------------

if (!exists("ihbss"))
   ihbss <- new.env()

## Get ODK Configuartion
ihbss$`2022` <- new.env()
local(envir = ihbss$`2022`, {
   wd <- file.path(getwd(), "src", "official", "ihbss2022")

   odk             <- list()
   odk$config      <- list()
   odk$config$ss   <- as_id("1SNB43JjGOB-uEivT5n8bfGpxiTEKkhNM7izo-KCQKPY",)
   odk$config$msm  <- read_sheet(odk$config$ss, "msm") %>% mutate(survey = "MSM", .before = 1)
   odk$config$pwid <- read_sheet(odk$config$ss, "pwid") %>% mutate(survey = "PWID", .before = 1)
   odk$config$fsw  <- read_sheet(odk$config$ss, "fsw") %>% mutate(survey = "FSW", .before = 1)
})

ihbss$`2022`$gdrive <- list(
   monitoring   = list(
      msm     = "1QqwgW9eAEVFvSNHyp0twL0Ft4oV5jqKuEW8xy20IH64",
      archive = "1w8emNjKE562yE69OjbAlfhFpu-2O4qqe"
   ),
   data_archive = list(
      msm = "106qbNfgxxR1cRe11JT1dUGL8-87gvL6e"
   ),
   data         = list(
      archive = "106qbNfgxxR1cRe11JT1dUGL8-87gvL6e",
      main     = "1xYi3thblJkhL4tajwFJB79wynOwUyZfi"
   ),
   submissions  = "1J_qu2gnazPlUBUWKWYSSExyylH-f78Gv"
)
ihbss$`2022`$conso  <- list()

## Run Pipeline
source(file.path(ihbss$`2022`$wd, "00_fns.R"))
source(file.path(ihbss$`2022`$wd, "01_download_submissions.R"))
source(file.path(ihbss$`2022`$wd, "02_flag_monitoring.R"))
source(file.path(ihbss$`2022`$wd, "03_export_site_data.R"))
source(file.path(ihbss$`2022`$wd, "04_upload_drive.R"))
source(file.path(ihbss$`2022`$wd, "05_upload_conso.R"))
