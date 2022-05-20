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

##  Load credentials and authentications ---------------------------------------

# dependencies
source("src/dependencies/options.R")
source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/full_tables.R")
source("src/dependencies/cloud.R")

# accounts
source("src/dependencies/auth_acct.R")

# classes
source("src/classes/Project.R")
source("src/classes/DB.R")

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB()

################################################################################

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
      y  = nhsss$harp_dx$official$old %>% select(idnum, CENTRAL_ID),
      by = "idnum"
   ) %>%
   pivot_wider(
      id_cols     = dup_group,
      names_from  = dup_num,
      values_from = c(idnum, CENTRAL_ID)
   )

epic_sites <- read_xlsx("H:/Software/OHASIS/Data Sets/20211109_DevPartner_Supported_Sites.xlsx")
epic_tld   <- nhsss$harp_tx$official$new_outcome %>%
   filter(!is.na(hub)) %>%
   left_join(
      y  = epic_sites,
      by = c("hub" = "FACI_CODE")
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         distinct(FACI_CODE, .keep_all = TRUE) %>%
         select(FACI_CODE, facility = FACI_NAME, tx_region = FACI_NHSSS_REG),
      by = c("hub" = "FACI_CODE")
   ) %>%
   # filter(site_epic_2022 == 1) %>%
   mutate(
      tld = if_else(stri_detect_fixed(art_reg, "dtg"), 1, 0, 0)
   ) %>%
   group_by(tx_region, facility, hub) %>%
   summarise(
      total  = n(),
      new    = if_else(newonart == 1, 1, 0, 0) %>% sum(na.rm = TRUE),
      visits = if_else(latest_ffupdate >= as.Date("2022-03-01"), 1, 0, 0) %>% sum(na.rm = TRUE),
      alive  = if_else(outcome == "alive on arv", 1, 0, 0) %>% sum(na.rm = TRUE),
      on_tld = if_else(outcome == "alive on arv" & tld == 1, 1, 0, 0) %>% sum(na.rm = TRUE),
   ) %>%
   ungroup() %>%
   mutate(
      perc_alive  = round((alive / total) * 100, digits = 2),
      perc_visits = round((visits / total) * 100, digits = 2),
      perc_new    = round((new / visits) * 100, digits = 2),
      perc_tld    = round((on_tld / alive) * 100, digits = 2),
   )

slackr_csv(channels = "dqt", epic_tld)

ohasis$db_checks$duped_rec_id %>%
   group_by(REC_ID) %>%
   mutate(dup_id = row_number()) %>%
   ungroup() %>%
   filter(dup_id > 1) %>%
   mutate(
      px_record = glue(r"(DELETE FROM px_record WHERE REC_ID = '{REC_ID}' AND PATIENT_ID = '{PATIENT_ID}';)"),
      px_name   = glue(r"(DELETE FROM px_name WHERE REC_ID = '{REC_ID}' AND PATIENT_ID = '{PATIENT_ID}';)"),
      px_info   = glue(r"(DELETE FROM px_info WHERE REC_ID = '{REC_ID}' AND PATIENT_ID = '{PATIENT_ID}';)"),
   ) %>%
   select(px_info) %>%
   write_clip()

nhsss$harp_tx$official$old_reg %>%
   get_dupes(CENTRAL_ID) %>%
   group_by(CENTRAL_ID) %>%
   mutate(dup_group = cur_group_id()) %>%
   ungroup() %>%
   select(
      art_id,
      dup_group,
      idnum,
      confirmatory_code,
      px_code,
      uic,
      first,
      middle,
      last,
      suffix,
      age,
      birthdate,
      sex,
      initials,
      philsys_id,
      philhealth_no,
      artstart_date,
      PATIENT_ID
   ) %>%
   left_join(
      y  = nhsss$harp_dx$official$new %>%
         select(idnum) %>%
         mutate(registry = 1),
      by = "idnum"
   ) %>%
   arrange(dup_group, idnum, art_id) %>%
   write_clip()


### update old registry data w/ gender_identity
df         <- read_dta(ohasis$get_data("harp_dx", ohasis$prev_yr, ohasis$prev_mo))
gi_sheet   <- read_sheet("190C7fSDIOODpir6dboVReII6mvioVXLympR5XM-BS54")
rs         <- dbSendQuery(ohasis$conn("db"), "SELECT REC_ID, SELF_IDENT_OTHER FROM ohasis_interim.px_profile")
px_profile <- dbFetch(rs)
dbClearResult(rs)
update_gi <- df %>%
   left_join(
      y  = px_profile,
      by = "REC_ID"
   ) %>%
   select(-gender_identity) %>%
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   mutate(
      SEX                       = case_when(
         sex == "MALE" ~ "1_MALE",
         sex == "FEMALE" ~ "2_FEMALE"
      ),
      self_identity             = case_when(
         self_identity == "MAN" ~ "MALE",
         self_identity == "WOMAN" ~ "FEMALE",
         TRUE ~ self_identity
      ),
      self_identity_other       = toupper(SELF_IDENT_OTHER),
      self_identity_other       = if_else(
         condition = self_identity == "OTHERS",
         true      = self_identity_other,
         false     = NA_character_
      ),
      self_identity_other_sieve = if_else(
         condition = !is.na(self_identity_other),
         true      = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),
         false     = NA_character_
      ),
   ) %>%
   left_join(
      y  = gi_sheet %>% select(SEX, self_identity, self_identity_other_sieve, gender_identity),
      by = c("SEX", "self_identity", "self_identity_other_sieve")
   ) %>%
   mutate(
      gender_identity = if_else(
         condition = !is.na(gender_identity),
         true      = gender_identity,
         false     = "(no data)"
      ),
   ) %>%
   select(
      -any_of(
         c(
            'byr', 'bmo', 'bdy', 'age_pregnant', 'age_vertical', 'age_unknown', 'self_identity_other_sieve'
         )
      )
   )

write_dta(update_gi, ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo))