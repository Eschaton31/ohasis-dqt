gf$final <- bind_rows(gf$flat) %>%
   mutate(
      `Logsheet Subtype` = toupper(`Logsheet Subtype`)
   ) %>%
   arrange(
      Indicator,
      `Data Source`,
      `Logsheet Subtype`,
      Date_Start,
      Date_End,
      `Site Region`,
      `Site Province`,
      `Site City`,
      `Site/Organization`,
      Sex,
      `Key Population`,
      `Age Group`,
      `DISAG 1`,
      `DISAG 2`,
      `DISAG 3`,
      `DISAG 3.1`,
      `DISAG 4`,
      `DISAG 5`,
      `Key Population`,
      `Age Group`
   ) %>%
   mutate_at(
      .vars = vars(`Age Group`),
      ~remove_code(.)
   )

gf$xlsx$dir  <- glue("H:/Data Sharing/PSFI/{ohasis$ym}")
gf$xlsx$file <- glue("PSFI Indicators ({gf$coverage$curr_yr}.{gf$coverage$curr_mo}).xlsx")
check_dir(gf$xlsx$dir)

gf$xlsx$wb       <- createWorkbook()
gf$xlsx$hs       <- createStyle(
   fontName       = "Calibri",
   fontSize       = 10,
   halign         = "center",
   valign         = "center",
   textDecoration = "bold",
   fgFill         = "#ffe699"
)
gf$xlsx$hs_disag <- createStyle(
   fontName       = "Calibri",
   fontSize       = 10,
   halign         = "center",
   valign         = "center",
   textDecoration = "bold",
   fgFill         = "#92d050"
)
gf$xlsx$cs       <- createStyle(
   fontName = "Calibri",
   fontSize = 10,
   numFmt   = openxlsx_getOp("numFmt", "COMMA")
)

## Sheet 1
addWorksheet(gf$xlsx$wb, "FLAT_PROTECTS")
writeData(gf$xlsx$wb, sheet = 1, x = gf$final)
addStyle(gf$xlsx$wb, sheet = 1, gf$xlsx$hs, rows = 1, cols = seq_len(ncol(gf$final)), gridExpand = TRUE)
addStyle(gf$xlsx$wb, sheet = 1, gf$xlsx$hs_disag, rows = 1, cols = 14:19, gridExpand = TRUE)
addStyle(gf$xlsx$wb, sheet = 1, gf$xlsx$cs, rows = 2:(nrow(gf$final) + 1), cols = seq_len(ncol(gf$final)), gridExpand = TRUE)
setColWidths(gf$xlsx$wb, 1, cols = seq_len(ncol(gf$final)), widths = 'auto')
setRowHeights(gf$xlsx$wb, 1, rows = seq_len(nrow(gf$final) + 1), heights = 14)
freezePane(gf$xlsx$wb, 1, firstRow = TRUE)

saveWorkbook(
   gf$xlsx$wb,
   file.path(gf$xlsx$dir, gf$xlsx$file),
   overwrite = TRUE
)

# check_dir(file.path(gf$xlsx$dir, "prev"))
# saveRDS(
#    gf,
#    file.path(gf$xlsx$dir, "prev", "gf.RDS")
# )
#
# for (ind in names(gf$linelist)) {
#    write_dta(
#       gf$linelist[[ind]] %>%
#          rename_all(
#             ~stri_replace_all_fixed(., " ", "_") %>%
#                stri_replace_all_fixed("/", "_") %>%
#                stri_replace_all_fixed(".", "_")
#          ),
#       file.path(gf$xlsx$dir, "prev", glue("{ind}.dta"))
#    )
#
#    stata(glue(r"(
# u "{gf$xlsx$dir}/prev/{ind}.dta", clear
# format_compress
# sa "{gf$xlsx$dir}/prev/{ind}.dta.dta", replace
#    )"))
# }