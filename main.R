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
source("src/dependencies/dedup.R")

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

lw_conn     <- ohasis$conn("lw")
id_registry <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry"))
sail_art    <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")) %>%
   filter(
      FACI_ID %in% c("130748", "040211", "040200") | SERVICE_FACI %in% c("130748", "040211", "040200", "130025")
   ) %>%
   # get latest central ids
   left_join(
      y  = id_registry %>%
         select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   select(
      CENTRAL_ID,
      UIC,
      PATIENT_CODE
   ) %>%
   distinct_all() %>%
   collect()

uic_data <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")) %>%
   filter(
      !is.na(UIC)
   ) %>%
   # get latest central ids
   left_join(
      y  = id_registry %>%
         select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   select(
      CENTRAL_ID,
      UIC,
      PATIENT_CODE
   ) %>%
   distinct_all() %>%
   collect()

sail_prep <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_prep")) %>%
   filter(
      FACI_ID %in% c("130748", "040211", "040200") | SERVICE_FACI %in% c("130748", "040211", "040200", "130025")
   ) %>%
   # get latest central ids
   left_join(
      y  = id_registry %>%
         select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   select(
      CENTRAL_ID,
      UIC,
      PATIENT_CODE
   ) %>%
   distinct_all() %>%
   collect()

df_reg <- select(id_registry, PATIENT_ID, CENTRAL_ID) %>% collect()

onart <- ohasis$get_data("harp_tx-reg", ohasis$yr, ohasis$mo) %>%
   read_dta() %>%
   select(-starts_with("CENTRAL_ID")) %>%
   left_join(
      y  = df_reg,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   left_join(
      y  = ohasis$get_data("harp_tx-outcome", ohasis$yr, ohasis$mo) %>%
         read_dta() %>%
         select(
            art_id,
            realhub,
            realhub_branch,
            latest_ffupdate,
            latest_nextpickup,
            outcome
         ),
      by = "art_id"
   ) %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   )

onprep <- ohasis$get_data("prep-reg", ohasis$yr, ohasis$mo) %>%
   read_dta() %>%
   select(-starts_with("CENTRAL_ID")) %>%
   left_join(
      y  = df_reg,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   left_join(
      y  = ohasis$get_data("prep-outcome", ohasis$yr, ohasis$mo) %>%
         read_dta() %>%
         select(
            prep_id,
            faci,
            branch,
            latest_ffupdate,
            latest_nextpickup,
            outcome
         ),
      by = "prep_id"
   ) %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   )

match_art  <- read_xlsx("C:/Users/johnb/Downloads/LIST OF LTFU CLIENTS (PLHIV) (1).xlsx", "LTFU")
match_prep <- read_xlsx("C:/Users/johnb/Downloads/UIC LTFU Sail Cavite.xlsx", "Prep")

linked_art <- match_art %>%
   rename(
      PATIENT_CODE = `PATIENT CODE`
   ) %>%
   mutate(
      row_id = row_number()
   ) %>%
   left_join(
      y  = sail_art,
      by = c("PATIENT_CODE", "UIC")
   ) %>%
   left_join(
      y  = uic_data %>%
         select(
            UIC_PID = CENTRAL_ID,
            UIC
         ),
      by = "UIC"
   ) %>%
   mutate(
      CENTRAL_ID = case_when(
         !is.na(UIC_PID) ~ UIC_PID,
         PATIENT_CODE == "SC-FOL-30" ~ "202201181306055X35",
         UIC == 'FLAR0506041999' & PATIENT_CODE == 'AFGC-22' ~ '202111091306053E58',
         UIC == 'LUMA0308141994' & PATIENT_CODE == 'AGD-27' ~ '20210828040200U661',
         UIC == 'EVAL0602181995' & PATIENT_CODE == 'AKFK-26' ~ '202109061300006C71',
         UIC == 'TERA0204261993' & PATIENT_CODE == 'RRLM-28' ~ 'HARP08011816047878',
         UIC == 'GLRE0401291983' & PATIENT_CODE == 'JGP-38' ~ '20210610130000K029',
         UIC == 'ERGI011062003' & PATIENT_CODE == 'KG-18' ~ '20220404130000106N',
         UIC == 'RHJE0204211995' & PATIENT_CODE == 'MIGB-25' ~ '2022011904020083T0',
         UIC == 'LIRI0101011992' & PATIENT_CODE == 'JRLP-30' ~ '20210918130605743E',
         # UIC == '' & PATIENT_CODE == '' ~ '',
         # UIC == '' & PATIENT_CODE == '' ~ '',
         # UIC == '' & PATIENT_CODE == '' ~ '',
         # UIC == '' & PATIENT_CODE == '' ~ '',
         # UIC == '' & PATIENT_CODE == '' ~ '',
         UIC == 'ELIS0703021959' & PATIENT_CODE == 'SMR-63' ~ 'HARP08011816020024',
         UIC == 'RHGA0111021994' & PATIENT_CODE == 'CJDNC-27' ~ '20220203130605P024',
         UIC == 'RORO0407291994' & PATIENT_CODE == 'CDS-27' ~ '2022010313060575W8',
         UIC == 'PUJO0308181991' & PATIENT_CODE == 'MJDO-30' ~ '2021111513060502U1',
         UIC == 'ERLE0902141992' & PATIENT_CODE == 'VCN-29' ~ '2021072813060534D5',
         UIC == 'AMNE0408281985' & PATIENT_CODE == 'RLM-36' ~ '20211022130605568H',
         UIC == 'ELTE0104151983' & PATIENT_CODE == 'GBN-38' ~ '2021052413060589Q0',
         UIC == 'LIJO0207192001' & PATIENT_CODE == 'MEB-20' ~ '20211110130605Q439',
         UIC == 'ELAN0105061987' & PATIENT_CODE == 'MAPA-33' ~ '2021051813060502W1',
         UIC == 'PRCA0508151999' & PATIENT_CODE == 'CQR-22' ~ '202109221306056I15',
         UIC == 'CAMA0211091999' & PATIENT_CODE == 'JATM-25' ~ '2021102313060521G5',
         UIC == 'ELLA0312141992' & PATIENT_CODE == 'RP-29' ~ '202202221306059S08',
         UIC == 'ROWI0808131995' & PATIENT_CODE == 'RBG-26' ~ '2021120513060540P0',
         UIC == 'JANI0407042003' & PATIENT_CODE == 'GGB-18' ~ '20220422130605S522',
         UIC == 'TEBO0411271996' & PATIENT_CODE == 'JKBG-25' ~ '20220502130605730D',
         UIC == 'COME0901221991' & PATIENT_CODE == 'JMA-31' ~ '20220216130605O454',
         TRUE ~ CENTRAL_ID
      )
   ) %>%
   rename(PATIENT_ID = CENTRAL_ID) %>%
   left_join(
      y  = df_reg,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )


linked_prep <- match_prep %>%
   slice(-1) %>%
   rename(
      PATIENT_CODE = 1,
      UIC          = 2
   ) %>%
   mutate(
      row_id = row_number()
   ) %>%
   left_join(
      y  = sail_prep,
      by = c("PATIENT_CODE", "UIC")
   ) %>%
   mutate(
      CENTRAL_ID = case_when(
         PATIENT_CODE == "SC-FOL-30" ~ "202201181306055X35",
         PATIENT_CODE == "SC-JFC-27" ~ "20211127130748F292",
         PATIENT_CODE == "SC-JNDGR-28" ~ "2021041113060530D8",
         PATIENT_CODE == "SC-KDT-22" ~ "20210522130748157F",
         PATIENT_CODE == "SC-JCVV-22" ~ "20210328130605054R",
         PATIENT_CODE == "SC-EJBE-36" ~ "2022010613074866O8",
         PATIENT_CODE == "SC-HC-22" ~ "202201301307485V43",
         TRUE ~ CENTRAL_ID
      )
   )


status_art <- linked_art %>%
   left_join(
      y  = onart,
      by = "CENTRAL_ID"
   ) %>%
   select(
      row_id,
      `NO.`,
      `PATIENT CODE` = PATIENT_CODE,
      UIC,
      `REASON FOR NOT COMING BACK`,
      `REMARKS`,
      `CONTACT NO.`,
      `UPDATE`,
      UIC,
      CENTRAL_ID,
      faci           = realhub,
      branch         = realhub_branch,
      latest_ffupdate,
      latest_nextpickup,
      outcome
   ) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         mutate(
            FACI_CODE     = case_when(
               stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
               stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
               stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
               TRUE ~ FACI_CODE
            ),
            SUB_FACI_CODE = if_else(
               condition = nchar(SUB_FACI_CODE) == 3,
               true      = NA_character_,
               false     = SUB_FACI_CODE
            ),
            SUB_FACI_CODE = case_when(
               FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
               FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
               FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
               TRUE ~ SUB_FACI_CODE
            ),
         ) %>%
         select(
            faci     = FACI_CODE,
            branch   = SUB_FACI_CODE,
            facility = FACI_NAME,
         ) %>%
         distinct_all(),
      by = c("faci", "branch")
   ) %>%
   mutate(
      outcome = case_when(
         is.na(outcome) ~ "(no record of treatment)",
         TRUE ~ outcome
      )
   )

status_prep <- linked_prep %>%
   left_join(
      y  = onprep,
      by = "CENTRAL_ID"
   ) %>%
   select(
      row_id,
      PATIENT_CODE,
      UIC,
      CENTRAL_ID,
      faci,
      branch,
      latest_ffupdate,
      latest_nextpickup,
      outcome
   ) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         mutate(
            FACI_CODE     = case_when(
               stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
               stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
               stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
               TRUE ~ FACI_CODE
            ),
            SUB_FACI_CODE = if_else(
               condition = nchar(SUB_FACI_CODE) == 3,
               true      = NA_character_,
               false     = SUB_FACI_CODE
            ),
            SUB_FACI_CODE = case_when(
               FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
               FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
               FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
               TRUE ~ SUB_FACI_CODE
            ),
         ) %>%
         select(
            faci     = FACI_CODE,
            branch   = SUB_FACI_CODE,
            facility = FACI_NAME,
         ) %>%
         distinct_all(),
      by = c("faci", "branch")
   ) %>%
   mutate(
      outcome = remove_code(outcome)
   )

write_xlsx(
   list(
      "PrEP" = status_prep %>%
         arrange(row_id) %>%
         select(-row_id),
      "ART"  = status_art %>%
         arrange(row_id) %>%
         select(-row_id)
   ),
   glue(r"(H:/{format(Sys.time(), "%Y%m%d")}_SAIL-Clients-Status_2022-05.xlsx)")
)

write_clip(
   status_art %>%
      arrange(row_id) %>%
      select(-row_id) %>%
      distinct_all() %>%
      filter(CENTRAL_ID != "HARP08011816052463") %>%
      select(
         -CENTRAL_ID,
         -faci,
         -branch
      )
)