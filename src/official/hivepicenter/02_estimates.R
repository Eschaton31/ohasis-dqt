##  Load Estimates
local(envir = epictr, {
   estimates <- list()

   estimates$class <- read_sheet(
      as_id("16_ytFIRiAgmo6cqtoy0JkZoI62S5IuWxKyNKHO_R1fs"),
      "v2022"
   )

   # download current file
   tmpfile <- tempfile("aem_cata_rotp", fileext = ".xlsx")
   drive_download("https://docs.google.com/spreadsheets/d/1Di8T9qXHhmTHZFnJ5aBAwltlb1C5ZFUE", tmpfile, overwrite = TRUE)

   # process data
   estimates$cata_rotp <- read_xlsx(tmpfile, "Prov and City Level") %>%
      select(1:14) %>%
      slice(3:nrow(.)) %>%
      rename(
         province = prov,
         est2020  = 10,
         est2021  = 11,
         est2022  = 12,
         est2023  = 13,
         est2024  = 14,
      ) %>%
      mutate(
         est_type  = case_when(
            is.na(region) ~ "adjust",
            !is.na(region) & is.na(province) & is.na(muncity) ~ "region",
            TRUE ~ "estimate"
         ),
         region    = stri_replace_last_regex(region, "\\.0$", ""),
         region    = case_when(
            stri_detect_regex(region, "^Discrepancy") ~ "UNKNOWN",
            TRUE ~ region
         ),
         muncity  = case_when(
            region == "9" & province == "BASILAN" ~ "ISABELA",
            TRUE ~ muncity
         ),
         province  = case_when(
            region == "9" & province == "BASILAN" ~ "BASILAN-RO9",
            TRUE ~ province
         ),
         unknown   = if_else(is.na(muncity), 1, 0, 0),
         aem_class = if_else(unknown == 1, "non a", aem_class, aem_class),
         munc_alt  = if_else(muncity == "ROTP", "UNKNOWN", muncity, muncity)
      ) %>%
      mutate_at(
         .vars = vars(province, muncity, munc_alt),
         ~if_else(unknown == 1, "UNKNOWN", ., .)
      )

   estimates$adjust_reg <- estimates$cata_rotp %>%
      filter(est_type == "region") %>%
      select(
         region,
         starts_with("est2"),
      ) %>%
      left_join(
         y  = estimates$cata_rotp %>%
            filter(est_type == "estimate") %>%
            group_by(region) %>%
            summarise_at(
               .vars = vars(starts_with("est2")),
               ~sum(., na.rm = TRUE)
            ) %>%
            rename_all(
               ~case_when(
                  stri_detect_regex(., "^est2") ~ paste0("sub_", .),
                  TRUE ~ .
               )
            ),
         by = "region"
      ) %>%
      mutate(
         est2020   = est2020 - if_else(is.na(sub_est2020), 0, sub_est2020, sub_est2020),
         est2021   = est2021 - if_else(is.na(sub_est2021), 0, sub_est2021, sub_est2021),
         est2022   = est2022 - if_else(is.na(sub_est2022), 0, sub_est2022, sub_est2022),
         est2023   = est2023 - if_else(is.na(sub_est2023), 0, sub_est2023, sub_est2023),
         est2024   = est2024 - if_else(is.na(sub_est2024), 0, sub_est2024, sub_est2024),
         province  = "UNKNOWN",
         muncity   = "UNKNOWN",
         munc_alt  = "UNKNOWN",
         aem_class = "non a",
         est_type  = "adjust",
         unknown   = 1
      ) %>%
      select(
         region,
         province,
         muncity,
         munc_alt,
         aem_class,
         est_type,
         unknown,
         starts_with("est2"),
      )

   estimates$cata_rotp %<>%
      filter(est_type == "estimate") %>%
      bind_rows(estimates$adjust_reg) %>%
      left_join(
         y  = ref_addr %>%
            select(
               region   = NHSSS_REG,
               province = NHSSS_PROV,
               muncity  = NHSSS_MUNC,
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               PSGC_AEM
            ),
         by = c(
            "region",
            "province",
            "munc_alt" = "muncity"
         )
      ) %>%
      mutate_at(
         .vars = vars(starts_with("PSGC")),
         ~if_else(. != "", paste0("PH", .), "")
      ) %>%
      # mutate(
      #    PSGC_AEM = if_else(aem_class %in% c("a", "ncr", "cebu city", "cebu province"), PSGC_MUNC, PSGC_PROV, PSGC_PROV),
      # ) %>%
      select(
         region,
         province,
         muncity,
         aem_class,
         starts_with("est2"),
         starts_with("PSGC"),
      ) %>%
      pivot_longer(
         cols      = starts_with("est2"),
         names_to  = "report_yr",
         values_to = "est"
      ) %>%
      mutate(
         report_yr = as.numeric(stri_replace_first_fixed(report_yr, "est", ""))
      )

   unlist(tmpfile)
   rm(tmpfile)
})

