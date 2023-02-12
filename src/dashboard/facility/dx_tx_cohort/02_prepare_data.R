get_faci_ids <- function(harp, oh) {
   data    <- list()
   data$dx <- harp$dx %>%
      get_cid(oh$id_reg, PATIENT_ID) %>%
      # match facility ids using list consolidated with encoded dxlab_standard &
      # dx_address;
      left_join(
         y  = read_sheet(as_id("1HuZWYG-Y926d8h3zoLKbr0I3qPP4VOZagyNAW3ht97k"), "Facility ID Matching") %>%
            rename(
               DX_FACI     = FACI_ID,
               DX_SUB_FACI = SUB_FACI_ID
            ),
         by = join_by(dx_region, dx_province, dx_muncity, dxlab_standard)
      ) %>%
      left_join(
         y  = oh$dx %>%
            mutate(
               use_record_faci = if_else(
                  condition = is.na(SERVICE_FACI),
                  true      = 1,
                  false     = 0
               ),
               SERVICE_FACI    = if_else(
                  condition = use_record_faci == 1,
                  true      = FACI_ID,
                  false     = SERVICE_FACI
               ),
            ),
         by = join_by(REC_ID)
      ) %>%
      mutate(
         TEST_SUB_FACI = if_else(
            is.na(TEST_FACI),
            SERVICE_SUB_FACI,
            "",
            ""
         ),
         TEST_SUB_FACI = if_else(
            TEST_FACI == SERVICE_FACI,
            SERVICE_SUB_FACI,
            TEST_SUB_FACI,
            TEST_SUB_FACI
         ),
         TEST_FACI     = if_else(
            is.na(TEST_FACI),
            SERVICE_FACI,
            TEST_FACI,
            TEST_FACI
         ),
         confirmlab    = case_when(
            labcode2 == "TLY22-09-01297" ~ "TLY",
            TRUE ~ confirmlab
         )
      ) %>%
      left_join(
         y  = read_sheet(as_id("1yxx1_VhomkBABJ72HgzjG7RNai5sJeZQdDZiiS0SJkU")) %>%
            arrange(desc(SUB_FACI_ID), FACI_ID) %>%
            distinct(FACI_ID, confirmlab, .keep_all = TRUE) %>%
            select(
               confirmlab,
               CONFIRM_FACI     = FACI_ID,
               CONFIRM_SUB_FACI = SUB_FACI_ID
            ),
         by = join_by(confirmlab)
      ) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            filter(!is.na(FACI_NAME_CLEAN)) %>%
            select(
               NHSSS_FACI     = FACI_ID,
               NHSSS_SUB_FACI = SUB_FACI_ID,
               dxlab_standard = FACI_NAME_CLEAN
            ) %>%
            arrange(desc(NHSSS_SUB_FACI), dxlab_standard) %>%
            distinct(NHSSS_FACI, dxlab_standard, .keep_all = TRUE),
         by = join_by(dxlab_standard)
      ) %>%
      mutate(
         faci_src      = case_when(
            !is.na(DX_FACI) ~ "dxlab",
            !is.na(NHSSS_FACI) ~ "nhsss",
            !is.na(TEST_FACI) ~ "test",
         ),
         HARP_FACI     = case_when(
            faci_src == "dxlab" ~ DX_FACI,
            faci_src == "nhsss" ~ NHSSS_FACI,
            faci_src == "test" ~ TEST_FACI,
         ),
         HARP_FACI     = if_else(HARP_FACI == "130000", NA_character_, HARP_FACI, HARP_FACI),
         HARP_SUB_FACI = case_when(
            faci_src == "dxlab" ~ DX_SUB_FACI,
            faci_src == "nhsss" ~ NHSSS_SUB_FACI,
            faci_src == "test" ~ TEST_SUB_FACI,
         )
      ) %>%
      distinct(idnum, .keep_all = TRUE)

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
      )

   return(data)
}

# attach facility names
add_faci_info <- function(data) {
   data$dx %<>%
      ohasis$get_faci(
         list(CONFIRM_LAB = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(DX_LAB = c("HARP_FACI", "HARP_SUB_FACI")),
         "name",
         c("DX_REG", "DX_PROV", "DX_MUNC")
      )

   data$tx %<>%
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

# disaggregations
gen_disagg <- function(data, params) {
   data$dx %<>%
      mutate(
         # dx age
         dx_age       = floor(age),
         dx_age_c     = case_when(
            dx_age >= 0 & dx_age < 15 ~ "<15",
            dx_age >= 15 & dx_age < 25 ~ "15-24",
            dx_age >= 25 & dx_age < 35 ~ "25-34",
            dx_age >= 35 & dx_age < 50 ~ "35-49",
            dx_age >= 50 & dx_age < 1000 ~ "50+",
            TRUE ~ "(no data)"
         ),
         dx_age_c     = case_when(
            dx_age >= 0 & dx_age < 15 ~ "<15",
            dx_age >= 15 & dx_age < 25 ~ "15-24",
            dx_age >= 25 & dx_age < 35 ~ "25-34",
            dx_age >= 35 & dx_age < 50 ~ "35-49",
            dx_age >= 50 & dx_age < 1000 ~ "50+",
            TRUE ~ "(no data)"
         ),

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

         msm          = if_else(sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"), 1, 0, 0),
         tgw          = if_else(sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"), 1, 0, 0),
         kap_type     = case_when(
            transmit == "IVDU" ~ "PWID",
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "MSM-TGW",
            sex == "Male" ~ "Other Males",
            sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
            sex == "Female" ~ "Other Females",
            TRUE ~ "Other"
         ),

         confirm_type = case_when(
            is.na(rhivda_done) ~ "SACCL",
            rhivda_done == 0 ~ "SACCL",
            rhivda_done == 1 ~ "CrCL",
         ),

         mortality    = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 0, 1, 1),
         dx           = if_else(!is.na(idnum), 1, 0, 0),
         dx_plhiv     = if_else(dx == 1 & mortality == 0, 1, 0, 0),
      )

   data$tx %<>%
      mutate(
         # dx age
         tx_age                = floor(curr_age),
         tx_age_c              = case_when(
            tx_age >= 0 & tx_age < 15 ~ "<15",
            tx_age >= 15 & tx_age < 25 ~ "15-24",
            tx_age >= 25 & tx_age < 35 ~ "25-34",
            tx_age >= 35 & tx_age < 50 ~ "35-49",
            tx_age >= 50 & tx_age < 1000 ~ "50+",
            TRUE ~ "(no data)"
         ),
         tx_age_c              = case_when(
            tx_age >= 0 & tx_age < 15 ~ "<15",
            tx_age >= 15 & tx_age < 25 ~ "15-24",
            tx_age >= 25 & tx_age < 35 ~ "25-34",
            tx_age >= 35 & tx_age < 50 ~ "35-49",
            tx_age >= 50 & tx_age < 1000 ~ "50+",
            TRUE ~ "(no data)"
         ),

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
            outcome == "stopped" ~ "stopped",
            diff > 30 ~ "ltfu",
            diff <= 30 ~ "onart",
            outcome == "ltfu" ~ "ltfu",
            TRUE ~ "(no data)"
         ),
         onart                 = if_else(onart == 1, 1, 0, 0),
         onart_new             = if_else(onart == 1 & latest_nextpickup >= as.Date(params$min), 1, 0, 0),


         baseline_vl           = if_else(
            condition = baseline_vl == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         baseline_vl_new       = if_else(
            condition = floor(interval(artstart_date, vl_date) / months(1)) < 6,
            true      = 1,
            false     = 0,
            missing   = 0
         ),


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
         vlsuppress            = if_else(onart == 1 & baseline_vl == 0 & vlp12m == 1, 1, 0, 0),
         vlsuppress_50         = if_else(
            onart_new == 1 &
               baseline_vl_new == 0 &
               vlp12m == 1 &
               vl_result < 50,
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

         ltfuestablish         = if_else(onart == 0 & startpickuplen_months >= 6, 1, 0, 0),
         ltfuestablish_new     = if_else(onart_new == 0 & startpickuplen_months >= 6, 1, 0, 0),
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p      <- envir
   p$data <- get_faci_ids(p$harp, p$oh)
   p$data <- add_faci_info(p$data)
   p$data <- gen_disagg(p$data, p$params)
}