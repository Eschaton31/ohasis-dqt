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
Sys.setenv(TZ = "UTC")
options(
   # browser = Sys.getenv("BROWSER"),
   browser = NULL,
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
if (!file.exists(".secrets/hivregistry.nec.RDS")) {
   token <- drop_auth()
   saveRDS(token, ".secrets/hivregistry.nec.RDS")
   rm('token')
} else {
   drop_auth(rdstoken = ".secrets/hivregistry.nec.RDS")
}

##------------------------------------------------------------------------------
##  Load primary classes
##------------------------------------------------------------------------------

# initiate the project & database
ohasis <- DB()

