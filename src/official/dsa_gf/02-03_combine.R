##  prepare dataset for HTS_TST ------------------------------------------------

gf$linelist$ohasis_uic <- bind_rows(gf$forms$form_cfbs, gf$forms$form_a, gf$forms$form_hts) %>%
   mutate(
      # tag those without form faci
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
   ) %>%
   select(-FACI_ID) %>%
   distinct_all()

gf$linelist$psfi_matched <- gf$logsheet$psfi %>%
   filter(sheet %in% c("MSMTGW", "PWID")) %>%
   left_join(
      y  = gf$corr$logsheet_psfi$ls_site,
      by = c("ls_subtype", "ls_kind")
   ) %>%
   left_join(
      y  = gf$corr$logsheet_psfi$staff %>%
         rename(
            site_region = 2
         ) %>%
         select(
            site_region,
            site_province,
            site_muncity,
            ls_subtype,
            provider_name,
            PSFI_STAFF_ID,
            USER_ID,
            FACI_ID,
            SUB_FACI_ID
         ) %>%
         filter(if_any(c(FACI_ID, USER_ID), ~!is.na(.))) %>%
         distinct(site_region, site_province, site_muncity, ls_subtype, provider_name, .keep_all = TRUE),
      by = c(
         "site_region",
         "site_province",
         "site_muncity",
         "ls_subtype",
         "provider_name"
      )
   ) %>%
   mutate(
      SERVICE_SUB_FACI = case_when(
         !is.na(LS_SUB_FACI) ~ LS_SUB_FACI,
         !is.na(FACI_ID) ~ SUB_FACI_ID,
      ),
      SERVICE_FACI     = case_when(
         !is.na(LS_FACI) ~ LS_FACI,
         !is.na(FACI_ID) ~ FACI_ID,
         !is.na(USER_ID) ~ StrLeft(USER_ID, 6),
      ),
      with_uic         = if_else(
         condition = !is.na(SERVICE_FACI),
         true      = 1,
         false     = 0
      ),
      sex              = "M"
   ) %>%
   left_join(
      y  = gf$linelist$ohasis_uic %>%
         select(
            uic = UIC,
            SERVICE_FACI,
            PATIENT_ID
         ) %>%
         distinct_all(),
      by = c("uic", "SERVICE_FACI")
   ) %>%
   distinct(ohasis_record, .keep_all = TRUE) %>%
   left_join(
      y  = gf$linelist$ohasis_uic %>%
         select(
            uic     = UIC,
            UIC_PID = PATIENT_ID
         ) %>%
         distinct_all(),
      by = "uic"
   ) %>%
   distinct(ohasis_record, .keep_all = TRUE) %>%
   mutate(
      with_pid   = case_when(
         !is.na(PATIENT_ID) ~ 1,
         !is.na(UIC_PID) ~ 1,
         TRUE ~ 0
      ),
      PATIENT_ID = case_when(
         !is.na(PATIENT_ID) ~ PATIENT_ID,
         TRUE ~ UIC_PID
      )
   ) %>%
   get_cid(gf$forms$id_registry, PATIENT_ID) %>%
   distinct(ohasis_record, .keep_all = TRUE) %>%
   ohasis$get_faci(
      list("site_name" = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "name"
   )

gf$linelist$psfi_matched %<>%
   mutate(
      site_name = case_when(
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATOR" &
            ls_subtype == "LGU" ~ "Global Fund PN (LGU)",
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATOR" &
            ls_subtype == "CBO" ~ "Global Fund PN (CBO)",
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATOR" &
            ls_subtype == "OTHER PARTNERS" ~ "Global Fund PN (Other Partners)",
         is.na(site_name) &
            ls_kind == "PROVINCIAL PEER NAVIGATOR" &
            ls_subtype == "LGU" ~ "Provincial Fund PN (LGU)",
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATION" &
            ls_subtype == "LGU" ~ "Global Fund PN (LGU)",
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATOR" &
            ls_subtype == "RURAL HEALTH UNIT" ~ "Global Fund PN (RHU)",
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATION" &
            ls_subtype == "RURAL HEALTH UNIT" ~ "Global Fund PN (RHU)",
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATOR" &
            ls_subtype == "TREATMENT HUB" ~ "Global Fund PN (HUB)",
         is.na(site_name) &
            ls_kind == "LGU PEER NAVIGATOR" &
            ls_subtype == "LGU" ~ "LGU PN (LGU)",
         is.na(site_name) &
            ls_kind == "LGU PEER NAVIGATOR" &
            ls_subtype == "RURAL HEALTH UNIT" ~ "LGU PN (RHU)",
         is.na(site_name) &
            ls_kind == "PROVINCIAL PEER NAVIGATOR" &
            ls_subtype == "RURAL HEALTH UNIT" ~ "Provincial PN (RHU)",
         is.na(site_name) &
            ls_kind == "REGIONAL PEER NAVIGATOR" &
            ls_subtype == "LGU" ~ "Regional PN (LGU)",
         is.na(site_name) &
            ls_kind == "TREATMENT HUB" &
            ls_subtype == "LGU" ~ "Hub PN (LGU)",
         is.na(site_name) &
            ls_kind == "OTHER CBOS" ~ "Other CBOs",
         is.na(site_name) &
            provider_name == "JUNARDNAMOC" &
            tested == "CBS" ~ "Global Fund PN (LGU)",
         TRUE ~ site_name
      )
   )

gf$logsheet$combined <- bind_rows(
   gf$logsheet$ohasis %>%
      arrange(reach_date) %>%
      distinct(ohasis_id, .keep_all = TRUE) %>%
      anti_join(
         y  = gf$linelist$psfi_matched %>%
            select(ohasis_id = CENTRAL_ID),
         by = "ohasis_id"
      ) %>%
      mutate(data_src = "OHASIS") %>%
      mutate_at(
         .vars = vars(num_sex_partner_m, num_sex_partner_f),
         ~as.character(.)
      ),
   gf$linelist$psfi_matched %>%
      anti_join(
         y  = gf$logsheet$ohasis %>%
            arrange(reach_date) %>%
            distinct(ohasis_id, .keep_all = TRUE) %>%
            select(CENTRAL_ID = ohasis_id),
         by = "CENTRAL_ID"
      ) %>%
      rename(ohasis_id = CENTRAL_ID) %>%
      mutate(
         data_src     = "PSFI",
         site_gf_2022 = 1
      ),
   gf$linelist$psfi_matched %>%
      select(
         -confirmed_positive,
         -confirm_date,
         -artstart_date,
         -tx_hub
      ) %>%
      inner_join(
         y  = gf$logsheet$ohasis %>%
            arrange(reach_date) %>%
            distinct(ohasis_id, .keep_all = TRUE) %>%
            select(
               CENTRAL_ID = ohasis_id,
               everonprep,
               confirmed_positive,
               confirm_date,
               artstart_date,
               tx_hub,
               tx_region
            ),
         by = "CENTRAL_ID"
      ) %>%
      rename(ohasis_id = CENTRAL_ID) %>%
      mutate(
         data_src     = "Both",
         site_gf_2022 = 1
      )
) %>%
   arrange(reach_date) %>%
   # add service address
   left_join(
      y  = ohasis$get_addr(
         bind_rows(gf$forms$form_cfbs, gf$forms$form_hts),
         c(
            "CBS_REG"  = "HIV_SERVICE_PSGC_REG",
            "CBS_PROV" = "HIV_SERVICE_PSGC_PROV",
            "CBS_MUNC" = "HIV_SERVICE_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
         select(
            ohasis_record = REC_ID,
            CBS_MUNC,
            CBS_PROV,
            CBS_REG
         ),
      by = "ohasis_record"
   ) %>%
   mutate(
      site_region   = case_when(
         data_src == "OHASIS" &
            tested == "CBS" &
            CBS_MUNC != "Unknown" &
            !is.na(CBS_MUNC) ~ CBS_REG,
         !is.na(SITE_REG) ~ SITE_REG,
         TRUE ~ site_region
      ),
      site_province = case_when(
         data_src == "OHASIS" &
            tested == "CBS" &
            CBS_MUNC != "Unknown" &
            !is.na(CBS_MUNC) ~ CBS_PROV,
         !is.na(SITE_PROV) ~ SITE_PROV,
         TRUE ~ site_province
      ),
      site_muncity  = case_when(
         data_src == "OHASIS" &
            tested == "CBS" &
            CBS_MUNC != "Unknown" &
            !is.na(CBS_MUNC) ~ CBS_MUNC,
         !is.na(SITE_MUNC) ~ SITE_MUNC,
         TRUE ~ site_muncity
      ),
      ohasis_id     = case_when(
         is.na(ohasis_id) ~ ohasis_record,
         TRUE ~ ohasis_id
      )
   ) %>%
   arrange(ohasis_id, kap_type) %>%
   group_by(ohasis_id) %>%
   mutate(
      count = n(),
      pair  = paste(collapse = "-", kap_type)
   ) %>%
   ungroup() %>%
   mutate(
      kap_type = case_when(
         pair == "GAY-MSM" ~ "MSM",
         pair == "GAY-TGW" ~ "TGW",
         TRUE ~ kap_type
      )
   ) %>%
   arrange(reach_date) %>%
   # distinct(ohasis_id, .keep_all = TRUE)
   distinct_all()

gf$logsheet$duplicates <- gf$logsheet$combined %>%
   get_dupes(ohasis_id) %>%
   relocate(uic, .after = ohasis_id)