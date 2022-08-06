shell("cls")
################################################################################
# Project name: > OHASIS Data Wrangling
# Author(s):    > Palo, John Benedict
# Date:         > 2022-01-27
# Description:  > This program serves as the primary controller for the various
#                 data pipelines of the NHSSS Unit.
################################################################################

rm(list = ls())

##  Load credentials and authentications ---------------------------------------

# dependencies
source("src/dependencies/options.R")
source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/pipeline.R")
source("src/dependencies/full_tables.R")
source("src/dependencies/cloud.R")
source("src/dependencies/dedup.R")

# accounts
source("src/dependencies/auth_acct.R")
source("src/dependencies/gmail.R")

# classes
source("src/classes/Project.R")
source("src/classes/DB.R")

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB()

df     <- XLConnect::loadWorkbook("C:/Users/Administrator/Downloads/ZCMC-ZTH-MASTERLIST 2022.xlsx")
data   <- XLConnect::readWorksheet(df, sheet = "Sheet1", startRow = 5, colTypes = "character")
zcm_ml <- data %>%
   select(
	  num                 = `No.`,
	  px_code             = `CODE`,
	  confirm_date        = DATE.DX,
	  confirmatory_code   = SACCL,
	  birthdate           = BIRTHDAY,
	  address             = ADDRESS,
	  baseline_cd4_result = INITIAL.CD4,
	  baseline_cd4_date   = DATE.CD4,
	  artstart_date       = DATE.ART.STARTED,
	  latest_regimen      = ART.REGIMEN,
	  transin_date        = X1st.Hub.visit,
	  faci_outcome        = REMARKS
   ) %>%
   mutate(
	  outcome = case_when(
		 stri_detect_fixed(faci_outcome, "ALIVE") ~ "onart",
		 stri_detect_fixed(faci_outcome, "EXPIRE") ~ "dead",
		 stri_detect_fixed(faci_outcome, "LOST") ~ "ltfu",
		 stri_detect_fixed(faci_outcome, "TO START") ~ "(for start)",
		 stri_detect_fixed(faci_outcome, "TRANS") ~ "transout",
		 stri_detect_fixed(faci_outcome, "DID NOT") ~ "(did not get result)",
	  )
   ) %>%
   mutate_at(
	  .vars = vars(contains("date")),
	  ~case_when(
		 stri_detect_fixed(., "-") ~ as.Date(., format = "%Y-%m-%d"),
		 stri_detect_fixed(., "/") ~ as.Date(., format = "%m/%d/%Y"),
		 TRUE ~ NA_Date_
	  )
   )

write_sheet(zcm_ml, "11102RM2tFpT6rtOgzGbhcRYV-6UJKjbZhFlYVL18paM", "ZCMC Masterlist (2022-06)")

conso_zcm <- read_sheet("11102RM2tFpT6rtOgzGbhcRYV-6UJKjbZhFlYVL18paM", "onart", col_types = "c") %>%
   bind_rows(
	  read_sheet("11102RM2tFpT6rtOgzGbhcRYV-6UJKjbZhFlYVL18paM", "transout", col_types = "c"),
	  read_sheet("11102RM2tFpT6rtOgzGbhcRYV-6UJKjbZhFlYVL18paM", "dead", col_types = "c"),
	  read_sheet("11102RM2tFpT6rtOgzGbhcRYV-6UJKjbZhFlYVL18paM", "ltfu", col_types = "c"),
   ) %>%
   mutate(
	  final_px = if_else(!is.na(`ZCMC Patient Code`), `ZCMC Patient Code`, `Patient Code`, `Patient Code`)
   )

write_sheet(conso_zcm, "11102RM2tFpT6rtOgzGbhcRYV-6UJKjbZhFlYVL18paM", "EB Conso")
