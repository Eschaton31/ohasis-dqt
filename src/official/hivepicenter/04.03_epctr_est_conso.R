epictr$data$est <- epictr$data$est_dx %>%
   full_join(
      y  = epictr$data$est_tx,
      by = c("PSGC_REG", "PSGC_PROV", "PSGC_AEM", "report_yr")
   ) %>%
   full_join(
      y  = epictr$estimates$cata_rotp,
      by = c("PSGC_REG", "PSGC_PROV", "PSGC_AEM", "report_yr")
   ) %>%
   left_join(
      y = epictr$ref_addr %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            NAME_REG,
            NAME_PROV,
            NAME_MUNC,
            NAME_AEM
         ) %>%
         mutate_at(
            .vars = vars(contains("PSGC")),
            ~if_else(. != "" & nchar(.) == 9, paste0("PH", .), ., "")
         )
   ) %>%
   distinct_all() %>%
   mutate(
      report_yr = as.Date(paste(sep = "-", report_yr, "01-01")),
      # NAME_AEM  = if_else(aem_class %in% c("a", "ncr", "cebu city", "cebu province"), NAME_MUNC, NAME_PROV, NAME_PROV),
   )

lw_conn     <- ohasis$conn("lw")
table_space <- Id(schema = "harp", table = "epictr_est")
if (dbExistsTable(lw_conn, table_space))
   dbRemoveTable(lw_conn, table_space)

dbWriteTable(lw_conn, table_space, epictr$data$est)
# ohasis$upsert(lw_conn, "harp", "epictr_est", epictr$data$est, c("PSGC_REG", "PSGC_PROV", "PSGC_AEM", "report_yr"))

dbDisconnect(lw_conn)
