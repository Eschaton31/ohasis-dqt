##
local(envir = epictr, {
   data[["est_tx"]] <- list()
   for (yr in seq(2020, params$yr)) {
      ref_yr                     <- as.character(yr)
      data[["est_tx"]][[ref_yr]] <- data$resreg %>%
         filter(year(report_yr) == yr) %>%
         group_by(
            PSGC_REG,
            PSGC_PROV,
            PSGC_AEM,
         ) %>%
         summarise_at(
            .vars = vars(
               evertx,
               evertx_plhiv,
               ontx,
               ontx_1mo,
               txestablish,
               txestablish_1mo,
               vltested,
               vltested_1mo,
               vlsuppress,
               vlsuppress_50,
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
   }
   data[["est_tx"]] <- bind_rows(data[["est_tx"]])

   rm(ref_yr, yr)
})
