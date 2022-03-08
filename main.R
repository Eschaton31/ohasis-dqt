shell("cls")
################################################################################
# Project name: > OHASIS Data Wrangling
# Author(s):    > Palo, John Benedict
# Date:         > 2022-01-27
# Description:  > This program creates the various OHASIS extracted data sets.
#
# Updates:      > See Changelog
#
# Input:        > OHASIS SQL Tables
#               > HARP Datasets
#
# Notes:        >
################################################################################

rm(list = ls())
Sys.setenv(TZ = "Asia/Hong_Kong")
options(
   # browser = Sys.getenv("BROWSER"),
   browser             = function(url) {
      if (grepl('^https?:', url)) {
         if (!.Call('.jetbrains_processBrowseURL', url)) {
            browseURL(url, .jetbrains$ther_old_browser)
         }
      } else {
         .Call('.jetbrains_showFile', url, url)
      }
   },
   help_type           = "html",
   RStata.StataPath    = Sys.getenv("STATA_PATH"),
   RStata.StataVersion = as.integer(Sys.getenv("STATA_VER"))
)

##  Load Environment Variables -------------------------------------------------

# check if OS is Windows, if not use the mounted Rlib volumne
if (Sys.info()['sysname'] == "Linux")
   .libPaths("/dqt/Rlib")

source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/classes.R")

##  Load credentials and authentications ---------------------------------------

# Google
# trigger auth on purpose --> store a token in the specified cache
options(
   gargle_oauth_cache = ".secrets",
   gargle_oauth_email = "nhsss@doh.gov.ph",
   gargle_oob_default = TRUE
)
drive_auth(cache = ".secrets")
gs4_auth(cache = ".secrets")

# Dropbox
# trigger auth on purpose --> store a token in the specified cache
if (!file.exists(".secrets/hivregistry.nec@gmail.com.RDS")) {
   token <- drop_auth(new_user = TRUE)
   saveRDS(token, ".secrets/hivregistry.nec@gmail.com.RDS")
   rm('token')
} else {
   drop_auth(rdstoken = ".secrets/hivregistry.nec@gmail.com.RDS")
}

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB()

########

# run registry
source("src/official/harp_dx/00_main.R")

#######

df  <- get_ei("2022.02")
try <- df %>%
   filter(Form %in% c("Form A", "HTS Form"),
          !is.na(`Record ID`)) %>%
   mutate(
      `Encoder` = stri_replace_first_fixed(encoder, "2022.02_", "")
   ) %>%
   select(
      `Facility ID`,
      `Facility Name`,
      `Page ID`,
      `Record ID`,
      `ID Type`,
      `Identifier`,
      `Issues`,
      `Validation`,
      `Encoder`
   )

write_xlsx(try, "H:/Feb 2022 Form A EI.xlsx")
#########
