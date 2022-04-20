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

# classes
source("src/classes/Project.R")
source("src/classes/DB.R")

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

#####

# run registry
source("src/official/harp_dx/00_main.R")

### import duplicates registry
df <- googlesheets4::read_sheet("1I_v83Nb4r1R2fNK39GCWQ3RhoRB8LpxzWLpd4D5uiwc", "For 2022-Qr1")
df %<>%
   select(
      dup_group,
      idnum
   ) %>%
   arrange(desc(idnum)) %>%
   group_by(dup_group) %>%
   mutate(dup_num = row_number()) %>%
   ungroup() %>%
   left_join(
      y = nhsss$harp_dx$official$old %>% select(idnum, CENTRAL_ID),
      by = "idnum"
   ) %>%
   pivot_wider(
      id_cols     = dup_group,
      names_from  = dup_num,
      values_from = c(idnum, CENTRAL_ID)
   )