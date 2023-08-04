get_faci_ids <- function(harp, oh) {
   data    <- list()
   data$dx <- harp$dx %>%
      get_cid(oh$id_reg, PATIENT_ID) %>%
      # match facility ids using list consolidated with encoded dxlab_standard &
      # dx_address;
      dxlab_to_id(
         c("HARP_FACI", "HARP_SUB_FACI"),
         c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
         ohasis$ref_faci
      ) %>%
      mutate(
         dxlab_standard = case_when(
            idnum %in% c(166980, 166981, 166982, 166983) ~ "MANDAUE SHC",
            TRUE ~ dxlab_standard
         ),
         confirmlab     = case_when(
            confirmlab == "RITM" ~ "RIT",
            TRUE ~ confirmlab
         ),
         confirm_branch = NA_character_
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(CONFIRM_FACI = "confirmlab", CONFIRM_SUB_FACI = "confirm_branch")
      )

   data$tx <- harp$tx %>%
      get_cid(oh$id_reg, PATIENT_ID) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(TX_FACI = "hub", TX_SUB_FACI = "branch")
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(REAL_FACI = "realhub", REAL_SUB_FACI = "realhub_branch")
      ) %>%
      mutate_at(
         .vars = vars(TX_FACI, REAL_FACI),
         ~if_else(. == "130000", NA_character_, ., .),
      ) %>%
      distinct(art_id, .keep_all = TRUE)

   return(data)
}

# attach facility names
add_faci_info <- function(data) {
   data$dx %<>%
      ohasis$get_faci(
         list(CONFIRM_LAB = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         "name"
      ) %>%
      mutate(FACI_ID = HARP_FACI) %>%
      ohasis$get_faci(
         list(DX_LAB = c("HARP_FACI", "HARP_SUB_FACI")),
         "name",
         c("DX_REG", "DX_PROV", "DX_MUNC")
      )

   data$tx %<>%
      mutate(CURR_FACI = REAL_FACI) %>%
      ohasis$get_faci(
         list(TX_HUB = c("TX_FACI", "TX_SUB_FACI")),
         "name",
         c("TX_REG", "TX_PROV", "TX_MUNC")
      ) %>%
      ohasis$get_faci(
         list(REAL_HUB = c("REAL_FACI", "REAL_SUB_FACI")),
         "name",
         c("REAL_REG", "REAL_PROV", "REAL_MUNC")
      )

   return(data)
}

# attach address names
add_addr_info <- function(data) {
   data$dx %<>%
      # perm address
      harp_addr_to_id(
         ohasis$ref_addr,
         c(
            PERM_PSGC_REG  = "region",
            PERM_PSGC_PROV = "province",
            PERM_PSGC_MUNC = "muncity"
         )
      ) %>%
      ohasis$get_addr(
         c(
            PERM_NAME_REG  = "PERM_PSGC_REG",
            PERM_NAME_PROV = "PERM_PSGC_PROV",
            PERM_NAME_MUNC = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      )

   return(data)
}

# disaggregations
gen_disagg <- function(data, params) {
   data$dx %<>%
      mutate(
         # dx age
         dx_age_c1    = gen_agegrp(age, "harp"),
         dx_age_c2    = gen_agegrp(age, "5yr"),

         #sex
         sex          = case_when(
            StrLeft(toupper(sex), 1) == "M" ~ "Male",
            StrLeft(toupper(sex), 1) == "F" ~ "Female",
            TRUE ~ "(no data)"
         ),

         # MOT
         mot          = case_when(
            transmit == "SEX" & sexhow == "BISEXUAL" ~ "Sex w/ Males & Females",
            transmit == "SEX" & sexhow == "HETEROSEXUAL" ~ "Male-Female Sex",
            transmit == "SEX" & sexhow == "HOMOSEXUAL" ~ "Male-Male Sex",
            transmit == "IVDU" ~ "Sharing of infected needles",
            transmit == "PERINATAL" ~ "Mother-to-child",
            transmit == "TRANSFUSION" ~ "Blood transfusion",
            transmit == "OTHERS" ~ "Others",
            transmit == "UNKNOWN" ~ "(no data)",
            TRUE ~ transmit
         ),

         msm          = if_else(
            sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"),
            1,
            0,
            0
         ),
         tgw          = if_else(
            sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"),
            1,
            0,
            0
         ),
         kap_type     = case_when(
            transmit == "IVDU" ~ "PWID",
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "MSM-TGW",
            sex == "Male" ~ "Other Males",
            # sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
            sex == "Female" ~ "Other Females",
            TRUE ~ "Other"
         ),

         confirm_type = case_when(
            confirmlab == "OTHERS" ~ "OTHERS",
            is.na(rhivda_done) ~ "SACCL",
            rhivda_done == 0 ~ "SACCL",
            rhivda_done == 1 ~ "CrCL",
         ),

         mortality    = if_else(
            (dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"),
            0,
            1,
            1
         ),
         dx           = if_else(!is.na(idnum), 1, 0, 0),
         dx_plhiv     = if_else(dx == 1 & mortality == 0, 1, 0, 0),
      )

   data$tx %<>%
      mutate(
         # dx age
         curr_age_c1           = gen_agegrp(curr_age, "harp"),
         curr_age_c2           = gen_agegrp(curr_age, "5yr"),

         # sex
         sex                   = case_when(
            StrLeft(toupper(sex), 1) == "M" ~ "Male",
            StrLeft(toupper(sex), 1) == "F" ~ "Female",
            TRUE ~ "(no data)"
         ),

         artlen_days           = floor(interval(artstart_date, as.Date(params$max)) / days(1)),
         artlen_months         = floor(interval(artstart_date, as.Date(params$max)) / months(1)),

         mortality             = if_else(outcome == "dead", 1, 0, 0),
         plhiv                 = if_else(mortality == 0, 1, 0, 0),
         everonart             = 1,
         everonart_plhiv       = if_else(everonart == 1 & mortality == 0, 1, 0, 0),


         diff                  = floor(interval(latest_nextpickup, params$max) / days(1)),
         outcome               = case_when(
            outcome == "alive on arv" ~ "onart",
            outcome == "lost to follow up" ~ "ltfu",
            outcome == "trans out" ~ "transout",
            TRUE ~ outcome
         ),
         outcome_new           = case_when(
            outcome == "dead" ~ "dead",
            diff <= 30 ~ "onart",
            grepl("stopped", outcome) & diff > 30 ~ outcome,
            grepl("transout", outcome) & diff > 30 ~ outcome,
            diff > 30 ~ "ltfu",
            outcome == "ltfu" ~ "ltfu",
            TRUE ~ "(no data)"
         ),
         onart                 = if_else(onart == 1, 1, 0, 0),
         onart_new             = if_else(onart == 1 & latest_nextpickup >= as.Date(params$min), 1, 0, 0),


         diff                  = floor(interval(previous_next_pickup, as.Date(params$min) - 1) / days(1)),
         previous_outcome      = case_when(
            outcome == "alive on arv" ~ "onart",
            outcome == "lost to follow up" ~ "ltfu",
            outcome == "trans out" ~ "transout",
            TRUE ~ outcome
         ),
         previous_outcome_new  = case_when(
            outcome == "dead" ~ "dead",
            diff <= 30 ~ "onart",
            grepl("stopped", outcome) & diff > 30 ~ outcome,
            grepl("transout", outcome) & diff > 30 ~ outcome,
            diff > 30 ~ "ltfu",
            outcome == "ltfu" ~ "ltfu",
            TRUE ~ "(no data)"
         ),

         diff                  = floor(interval(previous_next_pickup, latest_ffupdate) / days(1)),
         iit                   = case_when(
            outcome == "onart" &
               outcome != previous_outcome &
               diff < 90 ~ "IIT (ART <3 months)",
            outcome == "onart" &
               outcome != previous_outcome &
               diff %in% seq(90, 179) ~ "IIT (ART 3-5 months)",
            outcome == "onart" &
               outcome != previous_outcome &
               diff >= 180 ~ "IIT (ART >=6 months)",
         ),
         iit_new               = case_when(
            outcome_new == "onart" &
               outcome != previous_outcome_new &
               diff < 90 ~ "IIT (ART <3 months)",
            outcome_new == "onart" &
               outcome != previous_outcome_new &
               diff %in% seq(90, 179) ~ "IIT (ART 3-5 months)",
            outcome_new == "onart" &
               outcome != previous_outcome_new &
               diff >= 180 ~ "IIT (ART >=6 months)",
         ),


         baseline_vl           = if_else(
            condition = baseline_vl == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         baseline_vl_new       = baseline_vl,


         artestablish          = if_else(onart == 1 & artlen_days > 92, 1, 0, 0),
         artestablish_new      = if_else(onart_new == 1 & artlen_months >= 6, 1, 0, 0),
         vltested              = if_else(onart == 1 & baseline_vl == 0 & !is.na(vlp12m), 1, 0, 0),
         vltested_new          = if_else(
            onart_new == 1 &
               baseline_vl_new == 0 &
               !is.na(vlp12m),
            1,
            0,
            0
         ),
         vlsuppress            = if_else(onart == 1 &
                                            baseline_vl == 0 &
                                            vlp12m == 1 &
                                            vl_result < 1000, 1, 0, 0),
         vlsuppress            = if_else(
            onart_new == 1 &
               baseline_vl_new == 0 &
               (vlp12m == 1 | vl_result < 1000),
            1,
            0,
            0
         ),
         vlsuppress_50         = if_else(
            onart_new == 1 &
               baseline_vl_new == 0 &
               vlp12m == 1,
            1,
            0,
            0
         ),

         # special tagging
         mmd_months            = floor(interval(latest_ffupdate, latest_nextpickup) / months(1)),
         mmd                   = case_when(
            mmd_months <= 1 ~ "(1) 1 mo. of ARVs",
            mmd_months %in% seq(2, 3) ~ "(2) 2-3 mos. of ARVs",
            mmd_months %in% seq(4, 5) ~ "(3) 4-5 mos. of ARVs",
            mmd_months %in% seq(6, 12) ~ "(4) 6-12 mos. of ARVs",
            mmd_months > 12 ~ "(5) 12+ mos. worth of ARVs",
            TRUE ~ "(no data)"
         ),
         tle                   = if_else(
            condition = stri_detect_fixed(art_reg, "tdf") &
               stri_detect_fixed(art_reg, "3tc") &
               stri_detect_fixed(art_reg, "efv"),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         tld                   = if_else(
            condition = stri_detect_fixed(art_reg, "tdf") &
               stri_detect_fixed(art_reg, "3tc") &
               stri_detect_fixed(art_reg, "dtg"),
            true      = 1,
            false     = 0,
            missing   = 0
         ),

         artlen                = case_when(
            artlen_months <= 1 ~ "(1) 1 mo. on ARVs",
            artlen_months %in% seq(2, 3) ~ "(2) 2-3 mos. on ARVs",
            artlen_months %in% seq(4, 5) ~ "(3) 4-5 mos. on ARVs",
            artlen_months %in% seq(6, 12) ~ "(4) 6-12 mos. on ARVs",
            artlen_months %in% seq(13, 36) ~ "(5) 2-3 yrs. on ARVs",
            artlen_months %in% seq(37, 60) ~ "(6) 4-5 yrs. on ARVs",
            artlen_months %in% seq(61, 120) ~ "(7) 6-10 yrs. on ARVs",
            artlen_months > 120 ~ "(8) 10+ yrs. on ARVs",
            TRUE ~ "(no data)"
         ),

         startpickuplen_months = floor(interval(artstart_date, latest_nextpickup) / months(1)),
         startpickuplen        = case_when(
            startpickuplen_months <= 1 ~ "(1) 1 mo. on ARVs",
            startpickuplen_months %in% seq(2, 3) ~ "(2) 2-3 mos. on ARVs",
            startpickuplen_months %in% seq(4, 5) ~ "(3) 4-5 mos. on ARVs",
            startpickuplen_months %in% seq(6, 12) ~ "(4) 6-12 mos. on ARVs",
            startpickuplen_months %in% seq(13, 36) ~ "(5) 2-3 yrs. on ARVs",
            startpickuplen_months %in% seq(37, 60) ~ "(6) 4-5 yrs. on ARVs",
            startpickuplen_months %in% seq(61, 120) ~ "(7) 6-10 yrs. on ARVs",
            startpickuplen_months > 120 ~ "(8) 10+ yrs. on ARVs",
            TRUE ~ "(no data)"
         ),

         pickup                = if_else(is.na(latest_nextpickup), latest_ffupdate + 30, latest_nextpickup, latest_nextpickup),
         ltfu_months           = floor(interval(pickup, as.Date(params$max)) / months(1)),
         ltfulen               = case_when(
            ltfu_months <= 1 ~ "(1) 1 mo. LTFU",
            ltfu_months %in% seq(2, 3) ~ "(2) 2-3 mos. LTFU",
            ltfu_months %in% seq(4, 5) ~ "(3) 4-5 mos. LTFU",
            ltfu_months %in% seq(6, 12) ~ "(4) 6-12 mos. LTFU",
            ltfu_months %in% seq(13, 36) ~ "(5) 2-3 yrs. LTFU",
            ltfu_months %in% seq(37, 60) ~ "(6) 4-5 yrs. LTFU",
            ltfu_months %in% seq(61, 120) ~ "(7) 6-10 yrs. LTFU",
            ltfu_months > 120 ~ "(8) 10+ yrs. LTFU",
            TRUE ~ "(no data)"
         ),
      )

   return(data)
}

# disaggregations
attach_tx_to_dx <- function(data) {
   data$dx %<>%
      select(-outcome) %>%
      left_join(
         y  = data$tx %>%
            select(
               idnum,
               art_id,
               curr_age_c1,
               curr_age_c2,
               everonart,
               everonart_plhiv,
               onart,
               onart_new,
               outcome,
               outcome_new,
               iit,
               iit_new,
               artestablish,
               artestablish_new,
               baseline_vl,
               baseline_vl_new,
               vltested,
               vltested_new,
               vlsuppress,
               vlsuppress_50,
               vlp12m,
               vl_date,
               vl_result,
               mmd,
               tld,
               tle,
               artlen,
               startpickuplen,
               ltfulen,
               artstart_date,
               latest_regimen,
               latest_ffupdate,
               latest_nextpickup,
               CURR_FACI,
               CURR_TX_HUB  = REAL_HUB,
               CURR_TX_REG  = REAL_REG,
               CURR_TX_PROV = REAL_PROV,
               CURR_TX_MUNC = REAL_MUNC
            ),
         by = join_by(idnum)
      ) %>%
      mutate(
         tat_confirm_art = floor(interval(confirm_date, artstart_date) / days(1)),
         tat_confirm_art = case_when(
            tat_confirm_art < 0 ~ "0) Treatment before confirmatory",
            tat_confirm_art == 0 ~ "1) Same day",
            tat_confirm_art >= 1 & tat_confirm_art <= 14 ~ "2) Within 7 days",
            tat_confirm_art >= 15 & tat_confirm_art <= 30 ~ "3) Within 30 days",
            tat_confirm_art >= 31 ~ "4) More than 30 days",
            is.na(confirm_date) & !is.na(idnum) ~ "4) More than 30 days",
            TRUE ~ "(not yet confirmed)",
         )
      )

   data$tx %<>%
      left_join(
         y  = data$dx %>%
            select(
               idnum,
               dx_age_c1,
               dx_age_c2,
               dx,
               dx_plhiv,
               mot,
               kap_type,
               confirm_type,
               confirm_date,
               PERM_NAME_REG,
               PERM_NAME_PROV,
               PERM_NAME_MUNC,
               DX_LAB,
               DX_REG,
               DX_PROV,
               DX_MUNC,
               CONFIRM_LAB
            ),
         by = join_by(idnum)
      ) %>%
      mutate(
         tat_confirm_art = floor(interval(confirm_date, artstart_date) / days(1)),
         tat_confirm_art = case_when(
            tat_confirm_art < 0 ~ "0) Tx before dx",
            tat_confirm_art == 0 ~ "1) Same day",
            tat_confirm_art >= 1 & tat_confirm_art <= 14 ~ "2) RAI (w/in 14 days)",
            tat_confirm_art >= 15 & tat_confirm_art <= 30 ~ "3) W/in 30days",
            tat_confirm_art >= 31 ~ "4) More than 30 days",
            is.na(confirm_date) & !is.na(idnum) ~ "5) More than 30 days",
            TRUE ~ "(not yet confirmed)",
         )
      )

   return(data)
}

remove_cols <- function(data, oh) {
   data$dx %<>%
      select(
         -ends_with("PSGC_MUNC"),
         -ends_with("PSGC_PROV"),
         -any_of(c(
            "PATIENT_ID",
            "REC_ID",
            "transmit",
            "sexhow",
            "muncity",
            "province",
            "region",
            "confirmlab",
            "rhivda_done",
            "dxlab_standa1rd",
            "dx_muncity",
            "dx_province",
            "dx_region",
            "TEST_FACI",
            "mort",
            "dead",
            "DX_FACI",
            "DX_SUB_FACI",
            "remarks",
            "SUB_FACI_ID",
            "SERVICE_FACI",
            "SERVICE_SUB_FACI",
            "use_record_faci",
            "TEST_SUB_FACI",
            "NHSSS_FACI",
            "NHSSS_SUB_FACI",
            "faci_src",
            "msm",
            "tgw"
         ))
      )

   data$tx %<>%
      select(
         -ends_with("PSGC_MUNC"),
         -ends_with("PSGC_PROV"),
         -any_of(c(
            "PATIENT_ID",
            "REC_ID",
            "hub",
            "branch",
            "realhub",
            "realhub_branch",
            "previous_ffupdate",
            "previous_nextpickup",
            "previous_regimen",
            "art_reg",
            "PATIENT_ID",
            "TX_HUB",
            "TX_REG",
            "TX_PROV",
            "TX_MUNC",
            "artlen_days",
            "artlen_months",
            "diff",
            "startpickuplen_months",
            "ltfulen_month"
         ))
      ) %>%
      left_join(
         y        = oh$tx %>%
            rename(faci_enroll_date = VISIT_DATE) %>%
            bind_rows(
               data$tx %>%
                  select(CENTRAL_ID, FACI_ID = CURR_FACI, faci_enroll_date = artstart_date)
            ) %>%
            mutate(ORIG_FACI = FACI_ID) %>%
            arrange(faci_enroll_date) %>%
            distinct(CENTRAL_ID, FACI_ID, .keep_all = TRUE) %>%
            ohasis$get_faci(
               list(HUB_NAME = c("FACI_ID", "SUB_FACI_ID")),
               "name",
               c("HUB_REG", "HUB_PROV", "HUB_MUNC")
            ) %>%
            arrange(faci_enroll_date) %>%
            distinct(CENTRAL_ID, HUB_NAME, .keep_all = TRUE) %>%
            distinct(CENTRAL_ID, ORIG_FACI, .keep_all = TRUE),
         by       = join_by(CENTRAL_ID),
         multiple = "all"
      ) %>%
      left_join(
         y  = data$tx %>%
            select(CENTRAL_ID, CURR_FACI) %>%
            mutate(
               prime_record = 1,
               ORIG_FACI    = CURR_FACI
            ),
         by = join_by(CENTRAL_ID, CURR_FACI, ORIG_FACI)
      ) %>%
      mutate(
         linkage_facility = case_when(
            prime_record == 1 ~ "same",
            is.na(prime_record) ~ "transfer",
            TRUE ~ "(no data)"
         )
      ) %>%
      rename(
         FACI_ID      = ORIG_FACI,
         CURR_TX_HUB  = REAL_HUB,
         CURR_TX_REG  = REAL_REG,
         CURR_TX_PROV = REAL_PROV,
         CURR_TX_MUNC = REAL_MUNC
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p      <- envir
   p$data <- get_faci_ids(p$harp, p$oh)
   p$data <- add_faci_info(p$data)
   p$data <- add_addr_info(p$data)
   p$data <- gen_disagg(p$data, p$params)
   p$data <- attach_tx_to_dx(p$data)
   p$data <- remove_cols(p$data, p$oh)
}