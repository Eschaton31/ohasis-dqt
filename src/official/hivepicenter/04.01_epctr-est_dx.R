##
local(envir = epictr, {
   data[["est_dx"]] <- list()
   for (yr in c(2020, 2021, 2022)) {
      ref_yr                     <- as.character(yr)
      data[["est_dx"]][[ref_yr]] <- data$resreg %>%
         filter(year(report_yr) == yr, dx == 1) %>%
         group_by(
            PSGC_REG,
            PSGC_PROV,
            PSGC_AEM,
         ) %>%
         summarise_at(
            .vars = vars(
               dx,
               plhiv,
            ),
            ~sum(., na.rm = TRUE)
         ) %>%
         mutate_at(
            .vars = vars(contains("PSGC")),
            ~if_else(. != "" & nchar(.) == 9, paste0("PH", .), ., "")
         ) %>%
         mutate(
            report_yr = yr
         )

      ntl_plhiv <- data$resreg %>%
         filter(year(report_yr) == yr, dx == 1, is.na(mort)) %>%
         summarise(plhiv = n()) %>%
         as.numeric()

      reg_plhiv <- data$resreg %>%
         filter(year(report_yr) == yr, plhiv == 1) %>%
         summarise(plhiv = n()) %>%
         as.numeric()

      adjust_plhiv <- reg_plhiv - ntl_plhiv

      data[["est_dx"]][[ref_yr]] %<>%
         ungroup() %>%
         mutate(
            plhiv = if_else(
               PSGC_REG == "" &
                  PSGC_PROV == "" &
                  PSGC_AEM == "",
               plhiv - adjust_plhiv,
               plhiv,
               plhiv
            )
         )
   }
   data[["est_dx"]] <- bind_rows(data[["est_dx"]])

   rm(ref_yr, yr)
})
