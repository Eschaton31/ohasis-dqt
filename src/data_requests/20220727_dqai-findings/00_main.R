dqai <- list()

format_dqai <- function(ss, sheet, col_start, col_end, row_start, row_end) {
   # get sheet properties
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.get",
      params   = list(spreadsheetId = ss)
   )
   val <- googlesheets4::request_make(req)
   res <- httr::content(val)
   for (i in seq_len(length(res$sheets))) {
      sheet_name <- res$sheets[[i]]$properties$title

      if (sheet_name == sheet)
         sheet_id <- res$sheets[[i]]$properties$sheetId
   }

   # conditional formatting
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            deleteConditionalFormatRule = list(
               index   = 0,
               sheetId = sheet_id
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            deleteConditionalFormatRule = list(
               index   = 0,
               sheetId = sheet_id
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            addConditionalFormatRule = list(
               rule = list(
                  ranges      = list(
                     sheetId          = sheet_id,
                     startRowIndex    = row_start,
                     endRowIndex      = row_end,
                     startColumnIndex = col_start,
                     endColumnIndex   = col_end
                  ),
                  booleanRule = list(
                     condition = list(
                        type   = "CUSTOM_FORMULA",
                        values = list(userEnteredValue = r"(=AND($P1=$W1,$P1<>""))")
                     ),
                     format    = list(
                        backgroundColorStyle = list(
                           rgbColor = list(
                              red   = 207 / 255,
                              green = 255 / 255,
                              blue  = 174 / 255
                           )
                        )
                     )
                  )
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            addConditionalFormatRule = list(
               rule = list(
                  ranges      = list(
                     sheetId          = sheet_id,
                     startRowIndex    = row_start,
                     endRowIndex      = row_end,
                     startColumnIndex = col_start,
                     endColumnIndex   = col_end
                  ),
                  booleanRule = list(
                     condition = list(
                        type   = "CUSTOM_FORMULA",
                        values = list(userEnteredValue = r"(=AND($P1<>$W1,$P1<>""))")
                     ),
                     format    = list(
                        backgroundColorStyle = list(
                           rgbColor = list(
                              red   = 244 / 255,
                              green = 204 / 255,
                              blue  = 204 / 255
                           )
                        )
                     )
                  )
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)
}

# dqai$reg_art  <- read_dta("H:/_R/library/hiv_tx/data/20220628_reg-art_2022-05.dta")
dqai$gsheet     <- "1nUTSi-aEFyuNF1McL0i5jV0QVHlsuvy6g_4Bvktv7gU"
dqai$reg_art  <- ohasis$get_data("harp_tx-reg", "2022", "06") %>% read_dta()
dqai$dir      <- "C:/Users/Administrator/Documents/DQAI 2022/VSMMC"
dqai$dta_list <- list()
for (file in list.files(dqai$dir, "*.csv", full.names = TRUE)) {
   fname                  <- tools::file_path_sans_ext(basename(file))
   dqai$dta_list[[fname]] <- read_csv(file, col_types = "c") %>%
	  mutate(`ART ID #` = as.numeric(`ART ID #`)) %>%
	  left_join(
		 y  = dqai$reg_art %>%
			select(
			   `ART ID #`          = art_id,
			   `OHASIS ID`         = CENTRAL_ID,
			   `Birth Date`        = birthdate,
			   `Confirmatory Code` = confirmatory_code
			),
		 by = "ART ID #"
	  ) %>%
	  relocate(`OHASIS ID`, .before = 1) %>%
	  relocate(`Confirmatory Code`, .before = `ART ID #`) %>%
	  mutate(
		 `VSMMC Patient Code`           = NA_character_,
		 `VSMMC UIC`           = NA_character_,
		 `VSMMC Outcome`                = NA_character_,
		 `VSMMC Facility (referred to)` = NA_character_,
		 `VSMMC Regimen`                = NA_character_,
		 `VSMMC Latest Visit`           = NA_character_,
		 `VSMMC Latest Next Pick-up`    = NA_character_,
		 `VSMMC No. of Pills Dispensed` = NA_character_,
		 outcome = fname,
		 `final_px` = gs4_formula(glue(r"(
		 =IF($N{row_number()+1}<>"", $N{row_number()+1}, $H{row_number()+1})
		 )")),
		 `final_uic` = gs4_formula(glue(r"(
		 =IF($O{row_number()+1}<>"", $O{row_number()+1}, $G{row_number()+1})
		 )")),
	  )

   write_sheet(dqai$dta_list[[fname]], dqai$gsheet, fname)
   range_autofit(dqai$gsheet, fname)
   format_dqai(dqai$gsheet, fname, 0, ncol(dqai$dta_list[[fname]]), 0, nrow(dqai$dta_list[[fname]]))
}

dqai$sheet_list <- list()
for (file in c("onart", "ltfu", "dead", "transout")) {
   dqai$sheet_list[[file]] <- read_sheet(dqai$gsheet, file, col_types = "c") %>%
	  mutate(
		 outcome = file
	  ) %>%
	  as.data.frame()
}
rm(fname, file)

# write_xlsx(dqai$dta_list, "C:/Users/Administrator/Documents/DQAI 2022/VSMMC/zcm_masterlist_2022-05.xlsx")

dqai$eb <- bind_rows(dqai$dta_list)

dqai$faci <- bind_rows(dqai$sheet_list) %>%
   rename_all(~stri_replace_all_fixed(., "VSMMC", "HUB")) %>%
   filter(
	  nchar(`OHASIS ID`) <= 18,
	  !stri_detect_fixed(`OHASIS ID`, "Remarks")
   ) %>%
   mutate(
	  `ART ID #`      = case_when(
		 is.na(`ART ID #`) ~ paste("new", row_number()),
		 TRUE ~ as.character(`ART ID #`)
	  ),
	  hub_outcome     = case_when(
		 stri_detect_fixed(toupper(`HUB Outcome`), "PHYSICAL RECORD") ~ outcome,
		 stri_detect_fixed(toupper(`HUB Outcome`), "EXPIRED") ~ "dead",
		 stri_detect_fixed(toupper(`HUB Outcome`), "LTFU") ~ "ltfu",
		 stri_detect_fixed(toupper(`HUB Outcome`), "PEP") ~ "pep",
		 stri_detect_fixed(toupper(`HUB Outcome`), "STOPPED") ~ "stopped",
		 stri_detect_fixed(toupper(`HUB Outcome`), "NOT THEIR CLIENT") ~ "never client",
		 stri_detect_fixed(toupper(`HUB Outcome`), "ENCODE JAN AND APR 2022 VISITS") ~ "onart - 2022 not reported",
		 stri_detect_fixed(toupper(`HUB Outcome`), "RETURN TO TREATMENT") ~ "rtt - jun2022",
		 stri_detect_fixed(toupper(`HUB Outcome`), "SHOPPING") ~ "shopping",
		 stri_detect_fixed(toupper(`HUB Outcome`), "RTT") ~ "rtt - jun2022",
		 TRUE ~ outcome
	  ),
	  new_outcome_may = case_when(
		 hub_outcome == "shopping" ~ "onart",
		 hub_outcome == "stopped" ~ "stopped",
		 hub_outcome == "pep" ~ "remove",
		 hub_outcome == "never client" ~ "remove",
		 hub_outcome == "onart - 2022 not reported" ~ "onart",
		 outcome == "transout" & hub_outcome == "rtt - jun2022" ~ "transout",
		 outcome == "ltfu" & hub_outcome == "rtt - jun2022" ~ "ltfu",
		 TRUE ~ hub_outcome
	  )
   )

dqai$new_ml <- dqai$faci %>%
   filter(new_outcome_may != "remove") %>%
   mutate(
	  `Patient Code`      = case_when(
		 !is.na(`HUB Patient Code`) ~ `Patient Code`,
		 TRUE ~ `Patient Code`
	  ),
	  `VSMMC Patient Code` = NA_character_
   ) %>%
   left_join(
	  y  = dqai$reg_art %>%
		 mutate(art_id = as.character(art_id)) %>%
		 select(
			`ART ID #` = art_id,
			artstart_date,
			birthdate,
			initials,
			confirmatory_code,
			sex
		 ),
	  by = "ART ID #"
   ) %>%
   select(
	  confirmatory_code,
	  `OHASIS ID`,
	  `ART ID #`,
	  `Patient Code`,
	  `VSMMC Patient Code`,
	  `UIC`,
	  `Birth Date`          = birthdate,
	  Initials              = initials,
	  `ART Enrollment Date` = artstart_date,
	  `Latest Follow-up`,
	  `Expected Next Pick-up`,
	  sex,
	  `EB Outcome`          = new_outcome_may
   )

write_sheet(dqai$new_ml, "1JDdnhmQLTVGIEiWmhn3tck0iKAS3VJk5A6gH4t4230A", "Sheet1")