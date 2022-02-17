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
   browser   = function(url) {
      if (grepl('^https?:', url)) {
         if (!.Call('.jetbrains_processBrowseURL', url)) {
            browseURL(url, .jetbrains$ther_old_browser)
         }
      } else {
         .Call('.jetbrains_showFile', url, url)
      }
   },
   help_type = "html"
)

##------------------------------------------------------------------------------
##  Load Environment Variables
##------------------------------------------------------------------------------

# check if OS is Windows, if not use the mounted Rlib volumne
if (Sys.info()['sysname'] == "Linux")
   .libPaths("/dqt/Rlib")

source("src/dependencies/libraries.R")
source("src/dependencies/classes.R")

##------------------------------------------------------------------------------
##  Load credentials and authentications
##------------------------------------------------------------------------------

# GMail
# trigger auth on purpose --> store a token in the specified cache
options(
   gargle_oauth_cache = ".secrets",
   gargle_oauth_email = "nhsss@doh.gov.ph",
   gargle_oob_default = TRUE
)
drive_auth(cache = ".secrets")

# Dropbox
# trigger auth on purpose --> store a token in the specified cache
if (!file.exists(".secrets/hivregistry.nec@gmail.com.RDS")) {
   token <- drop_auth()
   saveRDS(token, ".secrets/hivregistry.nec@gmail.com.RDS")
   rm('token')
} else {
   drop_auth(rdstoken = ".secrets/hivregistry.nec@gmail.com.RDS")
}

##------------------------------------------------------------------------------
##  Load primary classes
##------------------------------------------------------------------------------

# initiate the project & database
ohasis <- DB()

# example warehouse
# Form A
ohasis$warehouse(table = "form_a", path = "src/data_warehouse/upsert")

# HTS Forms
ohasis$warehouse(table = "form_hts", path = "src/data_warehouse/upsert")
