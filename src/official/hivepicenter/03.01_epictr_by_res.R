##
local(envir = epictr, {
   data$resreg <- list()
   for (yr in c(2020, 2021, 2022)) {
      ref_yr                <- as.character(yr)
      data$resreg[[ref_yr]] <- data$linelist[[ref_yr]] %>%
         rename_at(
            .vars = vars(starts_with("RES_", ignore.case = FALSE)),
            ~stri_replace_first_regex(., "^RES_", "")
         )
   }
   data$resreg <- bind_rows(data$resreg)
})

lw_conn     <- ohasis$conn("lw")
table_space <- Id(schema = "harp", table = "epictr_linelist_res")
if (dbExistsTable(lw_conn, table_space))
   dbRemoveTable(lw_conn, table_space)

dbCreateTable(lw_conn, table_space, epictr$data$resreg)
dbExecute(
   lw_conn,
   r"(
   alter table harp.epictr_linelist_res modify row_id varchar(355) not null;
)"
)

ohasis$upsert(lw_conn, "harp", "epictr_linelist_res", epictr$data$resreg, c("PSGC_REG", "PSGC_PROV", "PSGC_AEM", "report_yr", "row_id"))

dbDisconnect(lw_conn)

