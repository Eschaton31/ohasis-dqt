icap$final <- bind_rows(icap$flat) %>%
   mutate_at(
      .vars = vars(`Site City`, `Site Province`, `Site Region`, `Age_Band`),
      ~remove_code(.)
   ) %>%
   arrange(
      Indicator,
      `Site/Organization`,
      `DISAG 2`,
      `DISAG 3`,
      `KP Population`,
      Age_Band
   )

# icap$xlsx$dir      <- glue("H:/Data Sharing/ICAP/{icap$coverage$type}/{icap$coverage$ym}")
icap$xlsx$dir      <- "H:/Data Sharing/ICAP/QR/2022.Q3"
icap$xlsx$file     <- glue("ICAP {icap$coverage$type} Indicators ({icap$coverage$ym}).xlsx")
check_dir(icap$xlsx$dir)

icap$xlsx$wb       <- createWorkbook()
icap$xlsx$hs       <- createStyle(
   fontName       = "Calibri",
   fontSize       = 10,
   halign         = "center",
   valign         = "center",
   textDecoration = "bold",
   fgFill         = "#ffe699"
)
icap$xlsx$hs_disag <- createStyle(
   fontName       = "Calibri",
   fontSize       = 10,
   halign         = "center",
   valign         = "center",
   textDecoration = "bold",
   fgFill         = "#92d050"
)
icap$xlsx$cs       <- createStyle(
   fontName = "Calibri",
   fontSize = 10,
   numFmt   = openxlsx_getOp("numFmt", "COMMA")
)

## Sheet 1
addWorksheet(icap$xlsx$wb, "FLAT_PHL")
writeData(icap$xlsx$wb, sheet = 1, x = icap$final)
addStyle(icap$xlsx$wb, sheet = 1, icap$xlsx$hs, rows = 1, cols = seq_len(ncol(icap$final)), gridExpand = TRUE)
addStyle(icap$xlsx$wb, sheet = 1, icap$xlsx$hs_disag, rows = 1, cols = 10:11, gridExpand = TRUE)
addStyle(icap$xlsx$wb, sheet = 1, icap$xlsx$cs, rows = 2:(nrow(icap$final) + 1), cols = seq_len(ncol(icap$final)), gridExpand = TRUE)
setColWidths(icap$xlsx$wb, 1, cols = seq_len(ncol(icap$final)), widths = 'auto')
setRowHeights(icap$xlsx$wb, 1, rows = seq_len(nrow(icap$final) + 1), heights = 14)
freezePane(icap$xlsx$wb, 1, firstRow = TRUE)

saveWorkbook(
   icap$xlsx$wb,
   file.path(icap$xlsx$dir, icap$xlsx$file),
   overwrite = TRUE
)

check_dir(file.path(icap$xlsx$dir, "prev"))
saveRDS(
   icap,
   file.path(icap$xlsx$dir, "prev", "icap.RDS")
)

for (ind in names(icap$linelist)) {
   write_dta(
      icap$linelist[[ind]] %>%
         rename_all(
            ~stri_replace_all_fixed(., " ", "_") %>%
               stri_replace_all_fixed("/", "_") %>%
               stri_replace_all_fixed(".", "_")
         ),
      file.path(icap$xlsx$dir, "prev", glue("{ind}.dta"))
   )

   stata(glue(r"(
u "{icap$xlsx$dir}/prev/{ind}.dta", clear
format_compress
sa "{icap$xlsx$dir}/prev/{ind}.dta", replace
   )"))
}
rm(ind)