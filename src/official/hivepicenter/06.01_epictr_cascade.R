local(envir = epictr, {
   inds <- c(
      "dx",
      "dx_plhiv",
      "plhiv",
      "evertx",
      "evertx_plhiv",
      "ontx",
      "ontx_1mo",
      "txestablish",
      "txestablish_1mo",
      "vltested",
      "vltested_1mo",
      "vlsuppress",
      "vlsuppress_50"
   )

   data$cascade <- list()
   for (yr in c(2020, 2021, 2022)) {
      ref_yr <- as.character(yr)
      long   <- epictr$data$resreg %>%
         filter(year(report_yr) == yr) %>%
         mutate(dx = 1) %>%
         pivot_longer(
            cols      = inds,
            names_to  = "indicator",
            values_to = "data"
         )

      group_vars             <- setdiff(names(long), c("data", "dx_age", "cur_age_tx", inds, "row_id", "harp_report_date", "tat_test_confirm", "tat_confirm_art", "confirm_date", "artstart_date", "startpickuplen_months", "ltfu_months", "mmd_months", "txlen_months", "ltfu_months", "txlen_m", "dxlab_standard"))
      min_date               <- max(long$harp_report_date, na.rm = TRUE) %m-% months(11)
      data$cascade[[ref_yr]] <- long %>%
         mutate(
            new_yr = case_when(
               harp_report_date >= min_date ~ 1,
               TRUE ~ 0
            )
         ) %>%
         filter(new_yr == 1) %>%
         select(
            -harp_report_date
         ) %>%
         group_by(across(all_of(group_vars))) %>%
         summarise(
            data = sum(data, na.rm = TRUE)
         )
   }
   data$cascade <- bind_rows(data$cascade)
})

lw_conn     <- ohasis$conn("lw")
table_space <- Id(schema = "harp", table = "epictr_casacde_yr")
if (dbExistsTable(lw_conn, table_space))
   dbRemoveTable(lw_conn, table_space)

dbCreateTable(lw_conn, table_space, epictr$data$cascade)

ohasis$upsert(lw_conn, "harp", "epictr_casacde_yr", epictr$data$cascade, c("PSGC_REG", "PSGC_PROV", "PSGC_AEM", "report_yr", "indicator"))

dbDisconnect(lw_conn)

