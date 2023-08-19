psgc_aem <- function(ref_addr) {
   local_drive_quiet()

   aem_xl  <- "https://docs.google.com/spreadsheets/d/1Di8T9qXHhmTHZFnJ5aBAwltlb1C5ZFUE"
   tmpfile <- tempfile("aem_cata_rotp", fileext = ".xlsx")
   drive_download(aem_xl, tmpfile, overwrite = TRUE)

   # process data
   aem <- read_xlsx(tmpfile, "Prov and City Level", col_types = "text", .name_repair = "unique_quiet") %>%
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
      mutate_at(
         .vars = vars(region),
         ~str_replace(., "\\.0$", "")
      ) %>%
      mutate_at(
         .vars = vars(starts_with("est2")),
         ~as.integer(.)
      ) %>%
      mutate(
         est_type  = case_when(
            is.na(region) ~ "adjust",
            !is.na(region) & is.na(province) & is.na(muncity) ~ "region",
            TRUE ~ "estimate"
         ),
         region    = case_when(
            stri_detect_regex(region, "^Discrepancy") ~ "UNKNOWN",
            TRUE ~ region
         ),
         muncity   = case_when(
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

   est_reg_adjust <- aem %>%
      filter(est_type == "region") %>%
      select(
         region,
         starts_with("est2"),
      ) %>%
      left_join(
         y  = aem %>%
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
      )

   est_reg_adjust %<>%
      mutate(
         across(
            names(select(., starts_with("est2", ignore.case = FALSE))),
            ~as.integer(. - coalesce(pull(est_reg_adjust, str_c("sub_", cur_column())), 0))
         )
      ) %>%
      mutate(
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

   new_ref <- ref_addr %>%
      mutate(
         drop = case_when(
            StrLeft(PSGC_PROV, 4) == "1339" & (PSGC_MUNC != "133900000" | is.na(PSGC_MUNC)) ~ 1,
            StrLeft(PSGC_REG, 4) == "1300" & PSGC_MUNC == "" ~ 1,
            stri_detect_fixed(NAME_PROV, "City") & NHSSS_MUNC == "UNKNOWN" ~ 1,
            TRUE ~ 0
         ),
      ) %>%
      filter(drop == 0) %>%
      add_row(
         PSGC_REG   = "130000000",
         PSGC_PROV  = "",
         PSGC_MUNC  = "",
         NAME_REG   = "National Capital Region (NCR)",
         NAME_PROV  = "Unknown",
         NAME_MUNC  = "Unknown",
         NHSSS_REG  = "NCR",
         NHSSS_PROV = "UNKNOWN",
         NHSSS_MUNC = "UNKNOWN",
      )

   ref_aem <- aem %>%
      filter(est_type == "estimate") %>%
      bind_rows(est_reg_adjust) %>%
      left_join(
         y  = new_ref %>%
            select(
               region   = NHSSS_REG,
               province = NHSSS_PROV,
               muncity  = NHSSS_MUNC,
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC
            ) %>%
            mutate(
               muncity == if_else(province != "UNKNOWN" & muncity == "UNKNOWN", "ROTP", muncity, muncity)
            ),
         by = join_by(region, province, muncity)
      ) %>%
      left_join(
         y  = new_ref %>%
            select(
               region         = NHSSS_REG,
               province       = NHSSS_PROV,
               ROTP_PSGC_REG  = PSGC_REG,
               ROTP_PSGC_PROV = PSGC_PROV,
            ) %>%
            mutate(
               drop = case_when(
                  region == "NCR" & province == "NCR" ~ 1,
                  ROTP_PSGC_PROV == "129800000" ~ 1,
                  TRUE ~ 0
               )
            ) %>%
            filter(drop == 0) %>%
            distinct_all(),
         by = join_by(region, province)
      ) %>%
      mutate(
         PSGC_REG  = coalesce(PSGC_REG, ROTP_PSGC_REG),
         PSGC_PROV = coalesce(PSGC_PROV, ROTP_PSGC_PROV),
         PSGC_AEM  = coalesce(PSGC_MUNC, PSGC_PROV)
      ) %>%
      select(-ROTP_PSGC_REG, ROTP_PSGC_PROV)

   aem_sites <- ref_aem %>%
      filter(muncity != "ROTP", province != "BASILAN-RO9") %>%
      select(
         aem_class_sites = aem_class,
         PSGC_REG,
         PSGC_PROV,
         PSGC_MUNC
      )

   aem_rotp <- ref_aem %>%
      filter(muncity == "ROTP" | province == "BASILAN-RO9") %>%
      mutate(
         aem_class = "non a"
      ) %>%
      select(
         aem_class_rotp = aem_class,
         PSGC_REG,
         PSGC_PROV
      )

   # rename columns
   final_ref <- new_ref %>%
      left_join(aem_sites, join_by(PSGC_REG, PSGC_PROV, PSGC_MUNC), na_matches = "never") %>%
      left_join(aem_rotp, join_by(PSGC_REG, PSGC_PROV), na_matches = "never") %>%
      mutate(
         aem_class = coalesce(aem_class_sites, aem_class_rotp),
         rotp      = if_else(is.na(aem_class_sites) & !is.na(aem_class_rotp), 1, 0, 0),
      ) %>%
      mutate(
         NAME_PROV = case_when(
            stri_detect_fixed(NAME_PROV, "NCR") ~ stri_replace_all_fixed(NAME_PROV, " (Not a Province)", ""),
            TRUE ~ NAME_PROV
         ),
         NAME_MUNC = case_when(
            PSGC_MUNC == "031405000" ~ "Bulacan City",
            TRUE ~ NAME_MUNC
         ),
         PSGC_PROV = case_when(
            PSGC_MUNC == "129804000" ~ "124700000",
            TRUE ~ PSGC_PROV
         ),

         # aem tagging
         PSGC_AEM  = case_when(
            PSGC_MUNC == "099701000" ~ "099701000",
            PSGC_MUNC == "129804000" ~ "124700000",
            aem_class %in% c("a", "ncr", "cebu city", "cebu province") ~ PSGC_MUNC,
            TRUE ~ PSGC_PROV
         ),
         NAME_AEM  = case_when(
            PSGC_MUNC == "031405000" ~ "Bulacan City",
            PSGC_MUNC == "129804000" ~ "Cotabato Province",
            aem_class %in% c("a", "ncr", "cebu city", "cebu province") ~ NAME_MUNC,
            rotp == 1 & !grepl("Province", NAME_PROV) ~ str_c(NAME_PROV, " Province"),
            TRUE ~ NAME_PROV
         ),
         NHSSS_AEM = case_when(
            PSGC_MUNC == "031405000" ~ "BULACAN",
            PSGC_MUNC == "129804000" ~ "ROTP",
            aem_class %in% c("a", "ncr", "cebu city", "cebu province") ~ NHSSS_MUNC,
            rotp == 1 & !grepl("Province", NHSSS_PROV) ~ "ROTP",
            TRUE ~ "ROTP"
         )
      ) %>%
      select(-aem_class_sites, -aem_class_rotp, -rotp, -drop) %>%
      mutate_at(
         .vars = vars(starts_with("PSGC")),
         ~if_else(. != "", str_c("PH", .), "")
      )

   final_aem <- ref_aem %>%
      left_join(
         y  = final_ref %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_AEM,
               NAME_REG,
               NAME_PROV,
               NAME_AEM
            ) %>%
            distinct_all(),
         by = join_by(PSGC_REG, PSGC_PROV, PSGC_AEM)
      ) %>%
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
   refs <- list(
      addr = final_ref,
      aem  = final_aem
   )

   return(refs)
}

harp_addr_to_id <- function(data, ref_addr, harp_addr, aem_sub_ntl = FALSE) {
   psgc_reg  <- names(harp_addr)[1]
   psgc_prov <- names(harp_addr)[2]
   psgc_munc <- names(harp_addr)[3]

   col_reg  <- as.name(harp_addr[[1]])
   col_prov <- as.name(harp_addr[[2]])
   col_munc <- as.name(harp_addr[[3]])

   data %<>%
      mutate(
         overseas_addr = case_when(
            {{col_munc}} == "OUT OF COUNTRY" ~ 1,
            TRUE ~ 0
         ),

         {{col_reg}}   := case_when(
            {{col_reg}} == "" & {{col_prov}} == "LANAO DEL NORTE" ~ "10",
            {{col_reg}} == "" & {{col_prov}} == "MISAMIS ORIENTAL" ~ "10",
            {{col_prov}} == "ILOILO" ~ "6",
            {{col_prov}} == "ABRA" ~ "CAR",
            {{col_prov}} == "1" ~ "ILOCOS SUR",
            {{col_prov}} == "3" ~ "BULACAN",
            {{col_prov}} == "4A" ~ "RIZAL",
            overseas_addr == 1 ~ "OVERSEAS",
            TRUE ~ {{col_reg}}
         ),
         {{col_prov}}  := case_when(
            {{col_prov}} == "METRO MANILA" ~ "NCR",
            {{col_prov}} == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
            {{col_prov}} == "MT. PROVINCE" ~ "MOUNTAIN PROVINCE",
            {{col_prov}} == "NCR" & {{col_munc}} == "UNKNOWN" ~ "UNKNOWN",
            {{col_prov}} == "DAVAO DEL SUR" & {{col_munc}} == "KAPALONG" ~ "DAVAO DEL NORTE",
            {{col_prov}} == "SOUTH COTABATO" & {{col_munc}} == "KIDAPAWAN" ~ "COTABATO",
            overseas_addr == 1 ~ "OVERSEAS",
            TRUE ~ {{col_prov}}
         ),
         {{col_munc}}  := str_replace_all({{col_munc}}, "\\bGEN\\.\\b", "GENERAL"),
         {{col_munc}}  := case_when(
            {{col_munc}} == "LAPU LAPU" ~ "LAPU-LAPU",
            {{col_munc}} == "CEBU CITY" ~ "CEBU",
            {{col_munc}} == "QUEZON CITY" ~ "QUEZON",
            {{col_munc}} == "ISLAND GARDEN SAMAL" ~ "SAMAL",
            {{col_prov}} == "CEBU" & {{col_munc}} == "PINAMUNGAHAN" ~ "PINAMUNGAJAN",
            {{col_prov}} == "BULACAN" & {{col_munc}} == "SAN JUAN" ~ "MALOLOS",
            {{col_prov}} == "MISAMIS OCCIDENTAL" & {{col_munc}} == "OZAMIS" ~ "OZAMIZ",
            {{col_prov}} == "OCCIDENTAL MINDORO" & {{col_munc}} == "MABURAO" ~ "MAMBURAO",
            {{col_prov}} == "BULACAN" & {{col_munc}} == "SJDM" ~ "SAN JOSE DEL MONTE",
            {{col_prov}} == "AURORA" & {{col_munc}} == "AURORA" ~ "MARIA AURORA",
            {{col_prov}} == "NUEVA ECIJA" & {{col_munc}} == "SAN JUAN" ~ "SAN JOSE",
            stri_detect_fixed({{col_munc}}, "(") ~ substr({{col_munc}}, 1, stri_locate_first_fixed({{col_munc}}, " (") - 1),
            overseas_addr == 1 ~ "OVERSEAS",
            TRUE ~ {{col_munc}}
         )
      ) %>%
      left_join(
         y  = ref_addr %>%
            mutate(
               drop = case_when(
                  StrLeft(PSGC_PROV, 4) == "1339" & (PSGC_MUNC != "133900000" | is.na(PSGC_MUNC)) ~ 1,
                  StrLeft(PSGC_REG, 4) == "1300" & PSGC_MUNC == "" ~ 1,
                  stri_detect_fixed(NAME_PROV, "City") & NHSSS_MUNC == "UNKNOWN" ~ 1,
                  TRUE ~ 0
               ),
            ) %>%
            filter(drop == 0) %>%
            add_row(
               PSGC_REG   = "130000000",
               PSGC_PROV  = "",
               PSGC_MUNC  = "",
               NAME_REG   = "National Capital Region (NCR)",
               NAME_PROV  = "Unknown",
               NAME_MUNC  = "Unknown",
               NHSSS_REG  = "NCR",
               NHSSS_PROV = "UNKNOWN",
               NHSSS_MUNC = "UNKNOWN",
            ) %>%
            mutate(
               PSGC_PROV = if (aem_sub_ntl) {
                  case_when(
                     PSGC_MUNC == "129804000" ~ "124700000",
                     TRUE ~ PSGC_PROV
                  )
               } else {
                  PSGC_PROV
               }
            ) %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               {{col_reg}}  := NHSSS_REG,
               {{col_prov}} := NHSSS_PROV,
               {{col_munc}} := NHSSS_MUNC,
            ),
         by = join_by({{col_reg}}, {{col_prov}}, {{col_munc}})
      ) %>%
      relocate(PSGC_REG, PSGC_PROV, PSGC_MUNC, .after = {{col_munc}}) %>%
      rename_all(
         ~case_when(
            . == "PSGC_REG" ~ psgc_reg,
            . == "PSGC_PROV" ~ psgc_prov,
            . == "PSGC_MUNC" ~ psgc_munc,
            TRUE ~ .
         )
      )

   return(data)
}
