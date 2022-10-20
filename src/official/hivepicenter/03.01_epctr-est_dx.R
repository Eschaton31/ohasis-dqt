##
local(envir = epictr, {
   data[["est_dx"]] <- list()
   for (yr in c(2020, 2021, 2022)) {
      ref_yr                     <- as.character(yr)
      data[["est_dx"]][[ref_yr]] <- harp$dx[[ref_yr]] %>%
         select(-starts_with("PSGC")) %>%
         left_join(
            y = ref_addr %>%
               select(
                  region   = NHSSS_REG,
                  province = NHSSS_PROV,
                  muncity  = NHSSS_MUNC,
                  PSGC_REG,
                  PSGC_PROV,
                  PSGC_MUNC
               ),
         ) %>%
         left_join(
            y = estimates$class %>%
               select(
                  aem_class,
                  PSGC_REG,
                  PSGC_PROV,
                  PSGC_MUNC
               )
         ) %>%
         mutate(
            PSGC_AEM = if_else(aem_class %in% c("a", "ncr", "cebu city", "cebu province"), PSGC_MUNC, PSGC_PROV, PSGC_PROV),
         ) %>%
         group_by(
            # region, province, muncity,
            PSGC_REG,
            PSGC_PROV,
            PSGC_AEM,
         ) %>%
         summarise(
            dx = sum(if_else(dead != 1 | is.na(dead), 1, 0, 0))
         ) %>%
         ungroup() %>%
         mutate_at(
            .vars = vars(starts_with("PSGC")),
            ~if_else(. != "", paste0("PH", .), "")
         ) %>%
         mutate(
            report_yr = yr
         )
   }
   data[["est_dx"]] <- bind_rows(data[["est_dx"]])

   rm(ref_yr, yr)
})
