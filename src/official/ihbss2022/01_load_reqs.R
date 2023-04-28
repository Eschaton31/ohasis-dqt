##  Set GDrive Endpoints & Instantiate lists -----------------------------------

get_odk_config <- function(ss) {
   ss         <- as_id(ss)
   odk        <- list()
   odk$config <- list(
      msm     = read_sheet(ss, "msm") %>% mutate(survey = "MSM", .before = 1),
      pwid_m  = read_sheet(ss, "pwid_m") %>% mutate(survey = "PWID_M", .before = 1),
      pwid_f  = read_sheet(ss, "pwid_f") %>% mutate(survey = "PWID_F", .before = 1),
      fsw     = read_sheet(ss, "fsw") %>% mutate(survey = "FSW", .before = 1),
      medtech = read_sheet(ss, "medtech") %>% mutate(survey = "Med Tech", .before = 1)
   )

   return(odk)
}

get_gdrive_endpoints <- function() {
   gdrive <- list(
      monitoring  = list(
         archive = "1w8emNjKE562yE69OjbAlfhFpu-2O4qqe",
         msm     = "1QqwgW9eAEVFvSNHyp0twL0Ft4oV5jqKuEW8xy20IH64",
         fsw     = "19xDlbyehoHpl4rZmDFyuKRFkUnkHUAiF-jHaviqkh9g",
         pwid_m  = "1XtBH6DsGtASOelo7CCVYnnvgCybgGe8YGU9leYXAvWA",
         pwid_f  = "17DJf8CKkAQFltuozRVWr6wc9Jq_LcFjRfQpqvxXqaVM",
         medtech = "1oE8vzhxbZ0-CABaPrVNzHIh5POgjrJJOCvhwkR1x4sk"
      ),
      data        = list(
         archive = "106qbNfgxxR1cRe11JT1dUGL8-87gvL6e",
         main    = "1xYi3thblJkhL4tajwFJB79wynOwUyZfi"
      ),
      submissions = "1J_qu2gnazPlUBUWKWYSSExyylH-f78Gv"
   )

   return(gdrive)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment())) {
   envir$odk    <- get_odk_config("1SNB43JjGOB-uEivT5n8bfGpxiTEKkhNM7izo-KCQKPY")
   envir$gdrive <- get_gdrive_endpoints()
   envir$conso  <- list()
}