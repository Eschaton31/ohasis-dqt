dr      <- new.env()
dr$icap <- new.env()
local(envir = dr$icap, {
   harp        <- list()
   harp$dx     <- read_dta(ohasis$get_data("harp_full", 2022, 12)) %>%
      mutate(
         msm    = if_else(sex == "MALE" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"), 1, 0, 0),
         pwid   = if_else(transmit == "IVDU", 1, 0, 0),
         others = if_else(msm + pwid == 0, 1, 0, 0),
         MOT    = case_when(
            msm == 1 ~ "MSM",
            pwid == 1 ~ "PWID",
            others == 1 ~ "Others",
         )
      )
   harp$tx2022 <- read_dta(hs_data("harp_tx", "outcome", 2022, 12)) %>%
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
               SUB_FACI_CODE = if_else(is.na(SUB_FACI_CODE), "", SUB_FACI_CODE, SUB_FACI_CODE)
            ) %>%
            select(
               realhub        = FACI_CODE,
               realhub_branch = SUB_FACI_CODE,
               tx_faci        = FACI_NAME,
            ) %>%
            distinct_all(),
         by = c("realhub", "realhub_branch")
      ) %>%
      mutate(
         tld           = if_else(
            stri_detect_fixed(art_reg, "tdf") &
               stri_detect_fixed(art_reg, "3tc") &
               stri_detect_fixed(art_reg, "dtg"),
            1,
            0,
            0
         ),
         ltfu          = if_else((is.na(onart) | onart == 0) & outcome != "dead", 1, 0, 0),
         vl_elig       = if_else(
            condition = (interval(artstart_date, as.Date("2022-06-30")) / months(1)) >= 6,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         vl_tested     = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & !is.na(vlp12m),
            1,
            0,
            0
         ),
         vl_suppressed = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & vlp12m == 1,
            1,
            0,
            0
         )
      )
   harp$tx2021 <- read_dta(hs_data("harp_tx", "outcome", 2021, 12)) %>%
      mutate(hub = toupper(hub)) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            filter(SUB_FACI_ID == "", nchar(FACI_CODE) == 3) %>%
            select(
               hub     = FACI_CODE,
               tx_faci = FACI_NAME,
               tx_reg  = FACI_NHSSS_REG,
               tx_prov = FACI_NHSSS_PROV,
               tx_munc = FACI_NHSSS_MUNC,
            ) %>%
            mutate(
               hub = case_when(
                  stri_detect_regex(hub, "^SAIL") ~ "SHP",
                  stri_detect_regex(hub, "^TLY") ~ "TLY",
                  TRUE ~ hub
               ),
            ) %>%
            distinct(hub, .keep_all = TRUE),
         by = "hub"
      ) %>%
      mutate(
         tld           = if_else(
            stri_detect_fixed(art_reg1, "tdf") &
               stri_detect_fixed(art_reg1, "3tc") &
               stri_detect_fixed(art_reg1, "dtg"),
            1,
            0,
            0
         ),
         ltfu          = if_else((is.na(onart) | onart == 0) & outcome != "dead", 1, 0, 0),
         vl_elig       = if_else(
            condition = (interval(artstart_date, as.Date("2021-12-31")) / months(1)) >= 6,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         vl_tested     = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & !is.na(vlp12m),
            1,
            0,
            0
         ),
         vl_suppressed = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & vlp12m == 1,
            1,
            0,
            0
         )
      )
   harp$tx2020 <- read_dta(hs_data("harp_tx", "outcome", 2020, 12)) %>%
      mutate(hub = toupper(hub)) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            filter(SUB_FACI_ID == "", nchar(FACI_CODE) == 3) %>%
            select(
               hub     = FACI_CODE,
               tx_faci = FACI_NAME,
               tx_reg  = FACI_NHSSS_REG,
               tx_prov = FACI_NHSSS_PROV,
               tx_munc = FACI_NHSSS_MUNC,
            ) %>%
            mutate(
               hub = case_when(
                  stri_detect_regex(hub, "^SAIL") ~ "SHP",
                  stri_detect_regex(hub, "^TLY") ~ "TLY",
                  TRUE ~ hub
               ),
            ) %>%
            distinct(hub, .keep_all = TRUE),
         by = "hub"
      ) %>%
      mutate(
         tld           = if_else(
            stri_detect_fixed(art_reg1, "tdf") &
               stri_detect_fixed(art_reg1, "3tc") &
               stri_detect_fixed(art_reg1, "dtg"),
            1,
            0,
            0
         ),
         ltfu          = if_else((is.na(onart) | onart == 0) & outcome != "dead", 1, 0, 0),
         vl_elig       = if_else(
            condition = (interval(artstart_date, as.Date("2020-12-31")) / months(1)) >= 6,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         vl_tested     = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & !is.na(vlp12m),
            1,
            0,
            0
         ),
         vl_suppressed = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & vlp12m == 1,
            1,
            0,
            0
         )
      )
   harp$tx2019 <- read_dta(hs_data("harp_tx", "outcome", 2019, 12)) %>%
      mutate(hub = toupper(hub)) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            filter(SUB_FACI_ID == "", nchar(FACI_CODE) == 3) %>%
            select(
               hub     = FACI_CODE,
               tx_faci = FACI_NAME,
               tx_reg  = FACI_NHSSS_REG,
               tx_prov = FACI_NHSSS_PROV,
               tx_munc = FACI_NHSSS_MUNC,
            ) %>%
            mutate(
               hub = case_when(
                  stri_detect_regex(hub, "^SAIL") ~ "SHP",
                  stri_detect_regex(hub, "^TLY") ~ "TLY",
                  TRUE ~ hub
               ),
            ) %>%
            distinct(hub, .keep_all = TRUE),
         by = "hub"
      ) %>%
      mutate(
         tld           = if_else(
            stri_detect_fixed(art_reg1, "tdf") &
               stri_detect_fixed(art_reg1, "3tc") &
               stri_detect_fixed(art_reg1, "dtg"),
            1,
            0,
            0
         ),
         ltfu          = if_else((is.na(onart) | onart == 0) & outcome != "dead", 1, 0, 0),
         vl_elig       = if_else(
            condition = (interval(artstart_date, as.Date("2019-12-31")) / months(1)) >= 6,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         vl_tested     = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & !is.na(vlp12m),
            1,
            0,
            0
         ),
         vl_suppressed = if_else(
            (is.na(baseline_vl) | baseline_vl == 0) & vlp12m == 1,
            1,
            0,
            0
         )
      )
})

dr$icap$ss    <- "1BTLzGcrBvwV6GjJRb-"
dr$icap$r6$dx <- dr$icap$harp$dx %>%
   filter(dx_region == "6", year >= 2019) %>%
   mutate(dx = 1) %>%
   group_by(dx_province, year) %>%
   summarise(
      CrCL  = sum(if_else(rhivda_done == 1, 1, 0, 0)),
      SACCL = sum(if_else(is.na(rhivda_done), 1, 0, 0)),
      Total = sum(dx),
   ) %>%
   ungroup() %>%
   mutate(DFojq0JFtMyyUL8QX21_KA1hs
      `% CrCL-confirmed` = (CrCL / Total) * 100
   ) %>%
   bind_rows(
      dr$icap$harp$dx %>%
         filter(dx_region == "6", year >= 2019) %>%
         mutate(dx = 1) %>%
         group_by(year) %>%
         summarise(
            CrCL  = sum(if_else(rhivda_done == 1, 1, 0, 0)),
            SACCL = sum(if_else(is.na(rhivda_done), 1, 0, 0)),
            Total = sum(dx),
         ) %>%
         ungroup() %>%
         mutate(
            dx_province        = "Region 6",
            `% CrCL-confirmed` = (CrCL / Total) * 100
         )
   )
dr$icap$r7$dx <- dr$icap$harp$dx %>%
   filter(dx_region == "7", year >= 2019) %>%
   mutate(dx = 1) %>%
   group_by(dx_province, year, MOT) %>%
   summarise(
      CrCL  = sum(if_else(rhivda_done == 1, 1, 0, 0)),
      SACCL = sum(if_else(is.na(rhivda_done), 1, 0, 0)),
      Total = sum(dx),
   ) %>%
   ungroup() %>%
   mutate(
      `% CrCL-confirmed` = (CrCL / Total) * 100
   ) %>%
   bind_rows(
      dr$icap$harp$dx %>%
         filter(dx_region == "7", year >= 2019) %>%
         mutate(dx = 1) %>%
         group_by(year) %>%
         summarise(
            CrCL  = sum(if_else(rhivda_done == 1, 1, 0, 0)),
            SACCL = sum(if_else(is.na(rhivda_done), 1, 0, 0)),
            Total = sum(dx),
         ) %>%
         ungroup() %>%
         mutate(
            dx_province        = "Region 7",
            `% CrCL-confirmed` = (CrCL / Total) * 100
         )
   )

dr$icap$r6$crcl <- dr$icap$harp$dx %>%
   filter(dx_region == "6", year >= 2019) %>%
   mutate(dx = 1) %>%
   group_by(dx_province, year, confirmlab) %>%
   summarise(
      Total = sum(dx),
   )

dr$icap$r7$crcl <- dr$icap$harp$dx %>%
   filter(dx_region == "7", year >= 2019) %>%
   mutate(dx = 1) %>%
   group_by(dx_province, year, confirmlab, MOT) %>%
   summarise(
      Total = sum(dx),
   )


dr$icap$r6$dx_enroll <- dr$icap$harp$dx %>%
   filter(dx_region == "6", year >= 2019, everonart == 1) %>%
   mutate(
      dx          = 1,
      tat_test    = case_when(
         !is.na(specimen_receipt_date) ~ interval(specimen_receipt_date, artstart_date) / days(1),
         is.na(specimen_receipt_date) ~ interval(visit_date, artstart_date) / days(1),
      ),
      tat_confirm = interval(confirm_date, artstart_date) / days(1)
   ) %>%
   group_by(dx_province, year) %>%
   summarise(
      `Started on ART` = sum(dx),
      test_min         = min(tat_test),
      test_max         = max(tat_test),
      test_p50         = median(tat_test),
      confirm_min      = min(tat_confirm),
      confirm_max      = max(tat_confirm),
      confirm_p50      = median(tat_confirm),
   ) %>%
   ungroup() %>%
   mutate(
      `HIV Testing to Enrollment in days = min to max (median)`  = glue("{test_min} to {test_max} ({test_p50})"),
      `Confirmatory to Enrollment in days = min to max (median)` = glue("{confirm_min} to {confirm_max} ({confirm_p50})"),
   ) %>%
   bind_rows(
      dr$icap$harp$dx %>%
         filter(dx_region == "6", year >= 2019, everonart == 1) %>%
         mutate(
            dx          = 1,
            tat_test    = case_when(
               !is.na(specimen_receipt_date) ~ interval(specimen_receipt_date, artstart_date) / days(1),
               is.na(specimen_receipt_date) ~ interval(visit_date, artstart_date) / days(1),
            ),
            tat_confirm = interval(confirm_date, artstart_date) / days(1)
         ) %>%
         group_by(year) %>%
         summarise(
            `Started on ART` = sum(dx),
            test_min         = min(tat_test),
            test_max         = max(tat_test),
            test_p50         = median(tat_test),
            confirm_min      = min(tat_confirm),
            confirm_max      = max(tat_confirm),
            confirm_p50      = median(tat_confirm),
         ) %>%
         ungroup() %>%
         mutate(
            dx_province                                                = "Region 6",
            `HIV Testing to Enrollment in days = min to max (median)`  = glue("{test_min} to {test_max} ({test_p50})"),
            `Confirmatory to Enrollment in days = min to max (median)` = glue("{confirm_min} to {confirm_max} ({confirm_p50})"),
         )
   )
dr$icap$r7$dx_enroll <- dr$icap$harp$dx %>%
   filter(dx_region == "7", year >= 2019, everonart == 1) %>%
   mutate(
      dx          = 1,
      tat_test    = case_when(
         !is.na(specimen_receipt_date) ~ interval(specimen_receipt_date, artstart_date) / days(1),
         is.na(specimen_receipt_date) ~ interval(visit_date, artstart_date) / days(1),
      ),
      tat_confirm = interval(confirm_date, artstart_date) / days(1)
   ) %>%
   group_by(dx_province, year, MOT) %>%
   summarise(
      `Started on ART` = sum(dx),
      test_min         = min(tat_test),
      test_max         = max(tat_test),
      test_p50         = median(tat_test),
      confirm_min      = min(tat_confirm),
      confirm_max      = max(tat_confirm),
      confirm_p50      = median(tat_confirm),
   ) %>%
   ungroup() %>%
   mutate(
      `HIV Testing to Enrollment in days = min to max (median)`  = glue("{test_min} to {test_max} ({test_p50})"),
      `Confirmatory to Enrollment in days = min to max (median)` = glue("{confirm_min} to {confirm_max} ({confirm_p50})"),
   ) %>%
   bind_rows(
      dr$icap$harp$dx %>%
         filter(dx_region == "7", year >= 2019, everonart == 1) %>%
         mutate(
            dx          = 1,
            tat_test    = case_when(
               !is.na(specimen_receipt_date) ~ interval(specimen_receipt_date, artstart_date) / days(1),
               is.na(specimen_receipt_date) ~ interval(visit_date, artstart_date) / days(1),
            ),
            tat_confirm = interval(confirm_date, artstart_date) / days(1)
         ) %>%
         group_by(year, MOT) %>%
         summarise(
            `Started on ART` = sum(dx),
            test_min         = min(tat_test),
            test_max         = max(tat_test),
            test_p50         = median(tat_test),
            confirm_min      = min(tat_confirm),
            confirm_max      = max(tat_confirm),
            confirm_p50      = median(tat_confirm),
         ) %>%
         ungroup() %>%
         mutate(
            dx_province                                                = "Region 6",
            `HIV Testing to Enrollment in days = min to max (median)`  = glue("{test_min} to {test_max} ({test_p50})"),
            `Confirmatory to Enrollment in days = min to max (median)` = glue("{confirm_min} to {confirm_max} ({confirm_p50})"),
         )
   )

dr$icap$r6$tx <- bind_rows(
   # 2019
   dr$icap$harp$tx2019 %>%
      filter(tx_reg == "6") %>%
      select(
         tx_faci,
         tx_reg,
         tx_prov,
         tx_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2019),
   dr$icap$harp$tx2020 %>%
      filter(tx_reg == "6") %>%
      select(
         tx_faci,
         tx_reg,
         tx_prov,
         tx_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2020),
   dr$icap$harp$tx2021 %>%
      filter(tx_reg == "6") %>%
      select(
         tx_faci,
         tx_reg,
         tx_prov,
         tx_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2021),
   dr$icap$harp$tx2022 %>%
      filter(real_reg == "6") %>%
      select(
         tx_faci,
         tx_reg  = real_reg,
         tx_prov = real_prov,
         tx_munc = real_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2022),
) %>%
   group_by(tx_prov, tx_faci, harpyr) %>%
   summarise(
      `Total`         = n(),
      `Alive on ART`  = sum(if_else(onart == 1, 1, 0, 0)),
      `On TLD`        = sum(if_else(onart == 1 & tld == 1, 1, 0, 0)),
      `LTFU`          = sum(if_else(ltfu == 1, 1, 0, 0)),
      `VL Eligible`   = sum(if_else(onart == 1 & vl_elig == 1, 1, 0, 0)),
      `VL Tested`     = sum(if_else(onart == 1 & vl_elig == 1 & vl_tested == 1, 1, 0, 0)),
      `VL Suppressed` = sum(if_else(onart == 1 &
                                       vl_elig == 1 &
                                       vl_tested == 1 &
                                       vl_suppressed == 1, 1, 0, 0)),
   ) %>%
   mutate(`On TLD %` = format((`On TLD` / `Alive on ART`) * 100, 2), .after = `On TLD`) %>%
   mutate(`LTFU %` = format((`LTFU` / `Total`) * 100, 2), .after = `LTFU`) %>%
   mutate(`VL Tested %` = format((`VL Tested` / `VL Eligible`) * 100, 2), .after = `VL Tested`) %>%
   mutate(`VL Suppressed %` = format((`VL Suppressed` / `VL Tested`) * 100, 2), .after = `VL Suppressed`)

dr$icap$r7$tx <- bind_rows(
   # 2019
   dr$icap$harp$tx2019 %>%
      filter(tx_reg == "7") %>%
      select(
         tx_faci,
         tx_reg,
         tx_prov,
         tx_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2019),
   dr$icap$harp$tx2020 %>%
      filter(tx_reg == "7") %>%
      select(
         tx_faci,
         tx_reg,
         tx_prov,
         tx_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2020),
   dr$icap$harp$tx2021 %>%
      filter(tx_reg == "7") %>%
      select(
         tx_faci,
         tx_reg,
         tx_prov,
         tx_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2021),
   dr$icap$harp$tx2022 %>%
      filter(real_reg == "7") %>%
      select(
         tx_faci,
         tx_reg  = real_reg,
         tx_prov = real_prov,
         tx_munc = real_munc,
         tld,
         ltfu,
         vl_elig,
         vl_tested,
         vl_suppressed,
         onart
      ) %>%
      mutate(harpyr = 2022),
) %>%
   group_by(tx_prov, tx_faci, harpyr) %>%
   summarise(
      `Total`         = n(),
      `Alive on ART`  = sum(if_else(onart == 1, 1, 0, 0)),
      `On TLD`        = sum(if_else(onart == 1 & tld == 1, 1, 0, 0)),
      `LTFU`          = sum(if_else(ltfu == 1, 1, 0, 0)),
      `VL Eligible`   = sum(if_else(onart == 1 & vl_elig == 1, 1, 0, 0)),
      `VL Tested`     = sum(if_else(onart == 1 & vl_elig == 1 & vl_tested == 1, 1, 0, 0)),
      `VL Suppressed` = sum(if_else(onart == 1 &
                                       vl_elig == 1 &
                                       vl_tested == 1 &
                                       vl_suppressed == 1, 1, 0, 0)),
   ) %>%
   mutate(`On TLD %` = format((`On TLD` / `Alive on ART`) * 100, 2), .after = `On TLD`) %>%
   mutate(`LTFU %` = format((`LTFU` / `Total`) * 100, 2), .after = `LTFU`) %>%
   mutate(`VL Tested %` = format((`VL Tested` / `VL Eligible`) * 100, 2), .after = `VL Tested`) %>%
   mutate(`VL Suppressed %` = format((`VL Suppressed` / `VL Tested`) * 100, 2), .after = `VL Suppressed`)

write_sheet(dr$icap$r6$dx, dr$icap$ss, "Diagnosis (R6)")
write_sheet(dr$icap$r7$dx, dr$icap$ss, "Diagnosis (R7)")
write_sheet(dr$icap$r6$dx_enroll, dr$icap$ss, "TAT (R6)")
write_sheet(dr$icap$r7$dx_enroll, dr$icap$ss, "TAT (R7)")
write_sheet(dr$icap$r6$crcl, dr$icap$ss, "CrCL (R6)")
write_sheet(dr$icap$r7$crcl, dr$icap$ss, "CrCL (R7)")
write_sheet(dr$icap$r6$tx, dr$icap$ss, "Treatment (R6)")
write_sheet(dr$icap$r7$tx, dr$icap$ss, "Treatment (R7)")