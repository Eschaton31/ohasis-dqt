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

# attach address names
add_addr_info <- function(data) {
   data$dx %<>%
      left_join(
         y  = ohasis$ref_addr %>%
            mutate(
               drop = if_else(
                  condition = StrLeft(PSGC_MUNC, 4) == "1339" & PSGC_MUNC != "133900000",
                  true      = 1,
                  false     = 0,
                  missing   = 0
               )
            ) %>%
            filter(drop == 0) %>%
            select(
               region         = NHSSS_REG,
               province       = NHSSS_PROV,
               muncity        = NHSSS_MUNC,
               PERM_NAME_REG  = NAME_REG,
               PERM_NAME_PROV = NAME_PROV,
               PERM_NAME_MUNC = NAME_MUNC,
               PERM_PSGC_REG  = PSGC_REG,
               PERM_PSGC_PROV = PSGC_PROV,
               PERM_PSGC_MUNC = PSGC_MUNC
            ),
         by = c("region", "province", "muncity")
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
            sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
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
         tx_age_c1             = gen_agegrp(curr_age, "harp"),
         tx_age_c2             = gen_agegrp(curr_age, "5yr"),

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
               tx_age_c1,
               tx_age_c2,
               everonart,
               everonart_plhiv,
               onart,
               onart_new,
               outcome,
               outcome_new,
               artestablish,
               artestablish_new,
               baseline_vl,
               baseline_vl_new,
               vltested,
               vltested_new,
               vlsuppress,
               vlsuppress_50,
               vlp12m,
               mmd,
               tld,
               tle,
               artlen,
               startpickuplen,
               ltfulen,
               latest_regimen,
               latest_ffupdate,
               latest_nextpickup,
               REAL_HUB,
               REAL_REG,
               REAL_MUNC,
               REAL_MUNC,
            ),
         by = join_by(idnum)
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
               PERM_NAME_REG,
               PERM_NAME_PROV,
               PERM_NAME_MUNC,
               PERM_PSGC_REG,
               PERM_PSGC_PROV,
               PERM_PSGC_MUNC,
               DX_LAB,
               DX_REG,
               DX_PROV,
               DX_MUNC,
               CONFIRM_LAB
            ),
         by = join_by(idnum)
      )

   return(data)
}

remove_cols <- function(data) {
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
            "FACI_ID",
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
            "startpickuplen",
            "ltfulen"
         ))
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
   p$data <- remove_cols(p$data)
}