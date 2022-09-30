##  prepare dataset for HTS_TST ------------------------------------------------

dr$matched <- dr$for_match %>%
   left_join(
      y  = dr$corr$logsheet_psfi$ls_site %>%
         select(
            ls_kind,
            ls_subtype,
            LS_FACI,
            LS_SUB_FACI
         ),
      by = c("ls_subtype", "ls_kind")
   ) %>%
   left_join(
      y  = dr$corr$logsheet_psfi$staff %>%
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
         ),
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
      y  = dr$ohasis_uic %>%
         mutate(
            SERVICE_FACI = if_else(is.na(SERVICE_FACI), FACI_ID, SERVICE_FACI, SERVICE_FACI)
         ) %>%
         filter(!is.na(UIC)) %>%
         select(
            uic = UIC,
            SERVICE_FACI,
            PATIENT_ID
         ) %>%
         distinct_all(),
      by = c("uic", "SERVICE_FACI")
   ) %>%
   left_join(
      y  = dr$ohasis_uic %>%
         filter(!is.na(UIC)) %>%
         select(
            uic     = UIC,
            UIC_PID = PATIENT_ID
         ) %>%
         distinct_all(),
      by = "uic"
   ) %>%
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
   left_join(
      y  = dr$id_registry,
      by = "PATIENT_ID"
   ) %>%
   distinct(ohasis_record, .keep_all = TRUE) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )

dr$matched <- ohasis$get_faci(
   dr$matched,
   list("site_name" = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
   "name"
)

dr$matched %<>%
   mutate(
      site_name = case_when(
         is.na(site_name) &
            ls_kind == "GF-CHOW" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "CHOW" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "LGU CHOW" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "LGU CHOW" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "DOH REGION CHOW" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "DOH" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "UNAIDS" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "LGU" &
            ls_subtype == "LGU" ~ "Global Fund CHOW (LGU)",
         is.na(site_name) &
            ls_kind == "GLOBAL FUND PEER NAVIGATOR" &
            ls_subtype == "LGU" ~ "Global Fund PN (LGU)",
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
            ls_kind == "LGU PEER EDUCATOR" &
            ls_subtype == "LGU" ~ "LGU PE (LGU)",
         is.na(site_name) &
            ls_kind == "PROVINCIAL PEER NAVIGATOR" &
            ls_subtype == "RURAL HEALTH UNIT" ~ "Provincial PN (RHU)",
         is.na(site_name) &
            ls_kind == "PROVINCIAL PEER NAVIGATOR" &
            ls_subtype == "LGU" ~ "Provincial PN (LGU)",
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
         is.na(site_name) &
            provider_name == "AVES LOVE & LIGHT" &
            ls_kind == "CBO" ~ "Other CBOs",
         TRUE ~ site_name
      )
   )

dr$final <- dr$matched %>%
   arrange(reach_date) %>%
   # distinct(ohasis_id, .keep_all = TRUE)
   distinct_all()