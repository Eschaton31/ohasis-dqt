##  Load HARP Datasets
local(envir = epictr, {
   harp    <- list()
   harp$dx <- list()
   harp$tx <- list()

   for (yr in c(2020, 2021, 2022)) {
      ref_yr            <- as.character(yr)
      harp$dx[[ref_yr]] <- ohasis$get_data("harp_full", yr, 12) %>%
         read_dta() %>%
         mutate_if(
            .predicate = is.character,
            ~if_else(str_squish(.) == "", NA_character_, ., .)
         ) %>%
         mutate(
            overseas_addr = case_when(
               muncity == "OUT OF COUNTRY" ~ 1,
               TRUE ~ 0
            ),

            # address
            muncity       = case_when(
               muncity == "PINAMUNGAHAN" & province == "CEBU" ~ "PINAMUNGAJAN",
               muncity == "SAN JUAN" & province == "BULACAN" ~ "MALOLOS",
               overseas_addr == 1 ~ "OVERSEAS",
               TRUE ~ muncity
            ),
            province      = case_when(
               province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
               overseas_addr == 1 ~ "OVERSEAS",
               TRUE ~ province
            ),
            region        = case_when(
               overseas_addr == 1 ~ "OVERSEAS",
               TRUE ~ region
            ),

            # dx address
            dx_region     = case_when(
               idnum == 4161 ~ "NCR",
               idnum == 6775 ~ "CAR",
               idnum %in% c(26083, 27030) ~ "4B",
               dx_province == "ILOILO" ~ "6",
               dx_province == "ABRA" ~ "CAR",
               dx_province == "1" ~ "ILOCOS SUR",
               dx_province == "3" ~ "BULACAN",
               dx_province == "4A" ~ "RIZAL",
               TRUE ~ dx_region
            ),
            dx_province   = case_when(
               idnum == 4161 ~ "NCR",
               idnum == 6775 ~ "BENGUET",
               idnum %in% c(26083, 27030) ~ "PALAWAN",
               dx_province == "DAVAO DEL SUR" & dx_muncity == "KAPALONG" ~ "DAVAO DEL NORTE",
               dx_province == "SOUTH COTABATO" & dx_muncity == "KIDAPAWAN" ~ "COTABATO",
               dx_province == "METRO MANILA" ~ "NCR",
               dx_province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
               dx_province == "MT. PROVINCE" ~ "MOUNTAIN PROVINCE",
               TRUE ~ dx_province
            ),
            dx_muncity    = case_when(
               idnum == 4161 ~ "QUEZON",
               idnum == 6775 ~ "BAGUIO",
               idnum %in% c(4887, 4945, 5035) ~ "PASIG",
               idnum %in% c(26083, 27030) ~ "PUERTO PRINCESA",
               dx_province == "SOUTH COTABATO" & dx_muncity == "GEN. SANTOS" ~ "GENERAL SANTOS",
               dx_province == "MISAMIS OCCIDENTAL" & dx_muncity == "OZAMIS" ~ "OZAMIZ",
               dx_province == "OCCIDENTAL MINDORO" & dx_muncity == "MABURAO" ~ "MAMBURAO",
               dx_province == "BULACAN" & dx_muncity == "SJDM" ~ "SAN JOSE DEL MONTE",
               dx_province == "AURORA" & dx_muncity == "AURORA" ~ "MARIA AURORA",
               dx_province == "NUEVA ECIJA" & dx_muncity == "SAN JUAN" ~ "SAN JOSE",
               dx_province == "CAVITE" & dx_muncity == "GEN. TRIAS" ~ "GENERAL TRIAS",
               dx_province == "CAVITE" & dx_muncity == "GMA" ~ "GEN. MARIANO ALVAREZ",
               TRUE ~ dx_muncity
            ),

            final_hub     = if (yr == 2022) realhub else toupper(hub),
            final_branch  = if (yr == 2022) realhub_branch else NA_character_,
            baseline_vl   = if (yr == 2020) if_else(baseline_vl == 0, as.numeric(NA), baseline_vl, baseline_vl) else baseline_vl
         )

      harp$tx[[ref_yr]] <- hs_data("harp_tx", "outcome", yr, 12) %>%
         read_dta() %>%
         mutate_if(
            .predicate = is.character,
            ~if_else(str_squish(.) == "", NA_character_, ., .)
         ) %>%
         mutate(
            final_hub    = if (yr == 2022) realhub else toupper(hub),
            final_branch = if (yr == 2022) realhub_branch else NA_character_,
            baseline_vl  = if (yr == 2020) if_else(baseline_vl == 0, as.numeric(NA), baseline_vl, baseline_vl) else baseline_vl
         )

      if (yr == 2022) {
         harp$tx[[ref_yr]] %<>%
            left_join(
               y  = hs_data("harp_tx", "reg", yr, 12) %>%
                  read_dta() %>%
                  select(
                     art_id,
                     confirmatory_code
                  ),
               by = "art_id"
            )
      }
   }
})

epictr$harp$dx$`2020` %<>%
   left_join(
      y  = epictr$harp$dx$`2022` %>%
         select(
            idnum,
            gender_identity,
            new_lab  = dxlab_standard,
            new_reg  = dx_region,
            new_prov = dx_province,
            new_munc = dx_muncity
         ),
      by = "idnum"
   ) %>%
   mutate(
      use_new     = case_when(
         dx_region == "UNKNOWN" & !is.na(new_reg) ~ 1,
         is.na(dx_region) & !is.na(new_reg) ~ 1,
         is.na(dx_province) & !is.na(new_prov) ~ 1,
         is.na(dx_muncity) & !is.na(new_munc) ~ 1,
         TRUE ~ 0
      ),
      dx_region   = if_else(use_new == 1, new_reg, dx_region, dx_region),
      dx_province = if_else(use_new == 1, new_prov, dx_province, dx_province),
      dx_muncity  = if_else(use_new == 1, new_munc, dx_muncity, dx_muncity),
   ) %>%
   select(
      -new_reg,
      -new_prov,
      -new_munc,
   )

epictr$harp$dx$`2021` %<>%
   left_join(
      y  = epictr$harp$dx$`2022` %>%
         select(
            idnum,
            gender_identity,
         ),
      by = "idnum"
   )