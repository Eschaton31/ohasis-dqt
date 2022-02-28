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
   token <- drop_auth()
   saveRDS(token, ".secrets/hivregistry.nec@gmail.com.RDS")
   rm('token')
} else {
   drop_auth(rdstoken = ".secrets/hivregistry.nec@gmail.com.RDS")
}

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB()


df_1 <- readRDS("C:/Users/johnb/Downloads/forms_final (2).RDS")
df_1 <- read_dta("C:/Users/johnb/Downloads/JAN 2022.dta") %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   )
df_2 <- nhsss$harp_dx$converted$data

df_check <- df_1 %>%
   mutate(kath        = 1,
          who_staging = as.character(baseline_cd4)) %>%
   select(idnum, labcode, dxlab_kath = class2022) %>%
   full_join(
      y  = df_2 %>%
         mutate(bene = 1) %>%
         select(REC_ID, labcode, dxlab_bene = class2022),
      by = "labcode"
   )

df_check %>%
   filter(dxlab_kath != dxlab_bene) %>%
   select(idnum, dxlab_bene)

df_1 %>% select(idnum, starts_with('baseline'))
df_2 %>% select(idnum, starts_with('baseline'))

df <- read_csv('C:/Users/johnb/Downloads/data.csv') %>% mutate(DATE = as.Date(DATE))
df %<>%
   distinct_all() %>%
   arrange(id, DATE) %>%
   group_by(id) %>%
   mutate(
      row_id   = row_number(),
      timediff = c(NA, diff(DATE))
   )


df$two_month <- ""
for (i in nrow(df)) {
   prev_date <- df[i - 1,]$DATE %>% as.Date()
   curr_date <- df[i,]$DATE %>% as.Date()

   if (difftime(curr_date, prev_date, units = "days") <= 60)
      df[i, "two_month"] <- "yes"
}

.tab(df, two_month)


write_dta(readRDS('H:/Software/OHASIS/Consolidation/Output/2022.01/2022.02.21.173316 (COB Export)/Forms - Death.RDS'),
        'H:/System/HARP/3_Mortality/Consolidation/input/2022.01/Forms - Death.dta')