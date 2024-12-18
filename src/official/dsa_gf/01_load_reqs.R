##  Generate pre-requisites and endpoints --------------------------------------

local(envir = gf, {
   coverage         <- list()
   coverage$fy      <- input(prompt = "What is the semestral coverage of the reports? (S2 FY22)")
   coverage$curr_mo <- input(prompt = "What is the reporting month for the reports?", max.char = 2)
   coverage$curr_mo <- stri_pad_left(coverage$curr_mo, 2, "0")

   if (str_left(coverage$fy, 2) == "S1") {
	  # coverage dates
	  coverage$min <- paste(
		 sep = "-",
		 paste0("20", StrRight(coverage$fy, 2)),
		 "01",
		 "01"
	  )
   } else if (str_left(coverage$fy, 2) == "S2") {
	  coverage$min <- paste(
		 sep = "-",
		 paste0("20", StrRight(coverage$fy, 2)),
		 "07",
		 "01"
	  )
   }

   coverage$max <- as.character((as.Date(paste(
	  sep = "-",
	  paste0("20", StrRight(coverage$fy, 2)),
	  coverage$curr_mo,
	  "01"
   )) %m+% months(1)) - 1)

   # reference months
   coverage$prev_mo <- stri_pad_left(month(as.Date(coverage$min) %m-% months(1)), 2, "0")
   coverage$prev_yr <- stri_pad_left(year(as.Date(coverage$min) %m-% months(1)), 2, "0")
   coverage$curr_mo <- stri_pad_left(month(as.Date(coverage$max)), 2, "0")
   coverage$curr_yr <- stri_pad_left(year(as.Date(coverage$max)), 2, "0")
	coverage$ym <- paste(sep = ".", coverage$curr_yr, coverage$curr_mo)
})

# check <- input(
#    prompt  = glue("Check the {green('GDrive Endpoints')}?"),
#    options = c("1" = "yes", "2" = "no"),
#    default = "2"
# )
# if (check == "1") {
#    .log_info("Checking endpoints.")
#    local(envir = gf, {
# 	  gdrive      <- list()
# 	  gdrive$path <- gdrive_endpoint("DSA - GF", coverage$ym)
#    })
# }

check <- input(
   prompt  = glue("Re-download the {green('data corrections')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading corrections list.")
   local(envir = gf, {
	  .log_info("Getting corrections.")
	  # corr <- gdrive_correct(gdrive$path, coverage$ym)
	   sheets <- c("ls_site", "site_addr", "ohasis_faci", "staff")
   		corr <- lapply(sheets, read_sheet, ss = "1qR9sp9VjwGO23vVEr4rMukX6jhDilCa3dbAxcsm2eFI", col_types = "c", .name_repair = "unique_quiet")
	   names(corr) <- sheets
	   rm(sheets)
   })
}

check <- input(
   prompt  = glue("Re-download the {green('GF-supported sites')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading GF-PSFI List of sites.")
   gf$sites <- read_sheet("1y0i8l-HNieOIQ1QGIezVLltwut3y1FxHryvhno9GNb4") %>%
	  distinct(FACI_ID, .keep_all = TRUE)
}

# run through all tables
check <- input(
   prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Updating data lake and data warehouse.")
   local(envir = gf, invisible({
	  tables           <- list()
	  tables$lake      <- c("lab_wide", "disp_meds")
	  tables$warehouse <- c("form_art_bc", "form_prep", "form_a", "form_hts", "form_cfbs", "id_registry", "rec_link")

	  lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
	  lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }))
}

# download forms
check <- input(
   prompt  = glue("Download relevant {green('data/forms')} to be used for consolidation?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   local(envir = gf, invisible({
	  lw_conn <- ohasis$conn("lw")
	  db_conn <- ohasis$conn("db")

	  min <- coverage$min
	  max <- coverage$max

	  forms             <- list()
	  forms$service_art <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "facility_service")) %>%
		 filter(SERVICE == "101201") %>%
		 collect()

	  .log_info("Downloading {green('Record Links')}.")
	  forms$rec_link <- dbTable(
		 db_conn,
		 "ohasis_interim",
		 "rec_link"
	  )

	  .log_info("Downloading {green('Central IDs')}.")
	  forms$id_registry <- dbTable(
		 lw_conn,
		 "ohasis_warehouse",
		 "id_registry",
		 cols = c("CENTRAL_ID", "PATIENT_ID")
	  )

	  .log_info("Downloading {green('Form A')}.")
	  forms$form_a <- dbTable(
		 lw_conn,
		 "ohasis_warehouse",
		 "form_a",
		 where     = glue("((RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(DATE_CONFIRM) >= '{min}' AND DATE(DATE_CONFIRM) <= '{max}') OR (DATE(T3_DATE) >= '{min}' AND DATE(T3_DATE) <= '{max}') OR (DATE(T2_DATE) >= '{min}' AND DATE(T2_DATE) <= '{max}') OR (DATE(T1_DATE) >= '{min}' AND DATE(T1_DATE) <= '{max}') OR (DATE(T0_DATE) >= '{min}' AND DATE(T0_DATE) <= '{max}'))"),
		 raw_where = TRUE
	  ) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
		 )

	  .log_info("Downloading {green('HTS Form')}.")
	  forms$form_hts <- dbTable(
		 lw_conn,
		 "ohasis_warehouse",
		 "form_hts",
		 where     = glue("((RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(DATE_CONFIRM) >= '{min}' AND DATE(DATE_CONFIRM) <= '{max}') OR (DATE(T3_DATE) >= '{min}' AND DATE(T3_DATE) <= '{max}') OR (DATE(T2_DATE) >= '{min}' AND DATE(T2_DATE) <= '{max}') OR (DATE(T1_DATE) >= '{min}' AND DATE(T1_DATE) <= '{max}') OR (DATE(T0_DATE) >= '{min}' AND DATE(T0_DATE) <= '{max}'))"),
		 raw_where = TRUE
	  ) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
		 )

	  .log_info("Downloading {green('CFBS Form')}.")
	  forms$form_cfbs <- dbTable(
		 lw_conn,
		 "ohasis_warehouse",
		 "form_cfbs",
		 where     = glue("(RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(TEST_DATE) >= '{min}' AND DATE(TEST_DATE) <= '{max}')"),
		 raw_where = TRUE
	  ) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
		 )

	  .log_info("Downloading {green('PrEP Form')}.")
	  forms$form_prep <- dbTable(
		 lw_conn,
		 "ohasis_warehouse",
		 "form_prep",
		 where     = glue("(VISIT_DATE >= '{min}' AND VISIT_DATE <= '{max}')"),
		 raw_where = TRUE
	  ) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
		 )

	  dbDisconnect(db_conn)
	  dbDisconnect(lw_conn)
	  rm(min, max, lw_conn, db_conn)
   }))
}

##  Get the previous report's HARP Registry ------------------------------------

check <- input(
   prompt  = "Reload HARP Datasets?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   local(envir = gf, {
	  harp <- list()

	  .log_info("Getting HARP Dx Dataset.")
	  harp$dx <- ohasis$get_data("harp_dx", coverage$curr_yr, coverage$curr_mo) %>%
		 read_dta() %>%
		 # convert Stata string missing data to NAs
		 mutate_if(
			.predicate = is.character,
			~if_else(. == '', NA_character_, .)
		 ) %>%
		 select(-starts_with("CENTRAL_ID")) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
		 )

	  .log_info("Getting the new HARP Tx Datasets.")
	  harp$tx$new_reg <- ohasis$get_data("harp_tx-reg", coverage$curr_yr, coverage$curr_mo) %>%
		 read_dta() %>%
		 # convert Stata string missing data to NAs
		 mutate_if(
			.predicate = is.character,
			~if_else(. == '', NA_character_, .)
		 ) %>%
		 select(-starts_with("CENTRAL_ID")) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
		 )

	  harp$tx$new_outcome <- ohasis$get_data("harp_tx-outcome", coverage$curr_yr, coverage$curr_mo) %>%
		 read_dta() %>%
		 # convert Stata string missing data to NAs
		 mutate_if(
			.predicate = is.character,
			~if_else(. == '', NA_character_, .)
		 ) %>%
		 select(-starts_with("CENTRAL_ID")) %>%
		 left_join(
			y  = harp$tx$new_reg %>%
			   select(art_id, CENTRAL_ID),
			by = "art_id"
		 )

	  .log_info("Getting HARP Dead Dataset.")
	  harp$dead_reg <- ohasis$get_data("harp_dead", coverage$curr_yr, coverage$curr_mo) %>%
		 read_dta() %>%
		 # convert Stata string missing data to NAs
		 mutate_if(
			.predicate = is.character,
			~if_else(. == '', NA_character_, .)
		 ) %>%
		 select(-starts_with("CENTRAL_ID")) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID       = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
			proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
			ref_death_date   = if_else(
			   condition = is.na(date_of_death),
			   true      = proxy_death_date,
			   false     = date_of_death
			)
		 )

	  .log_info("Getting the new PrEP Datasets.")
	  harp$prep$new_reg <- ohasis$get_data("prep-reg", coverage$curr_yr, coverage$curr_mo) %>%
		 read_dta() %>%
		 # convert Stata string missing data to NAs
		 mutate_if(
			.predicate = is.character,
			~if_else(. == '', NA_character_, .)
		 ) %>%
		 select(-starts_with("CENTRAL_ID")) %>%
		 left_join(
			y  = forms$id_registry,
			by = "PATIENT_ID"
		 ) %>%
		 mutate(
			CENTRAL_ID = if_else(
			   condition = is.na(CENTRAL_ID),
			   true      = PATIENT_ID,
			   false     = CENTRAL_ID
			),
		 )

	  harp$prep$new_outcome <- ohasis$get_data("prep-outcome", coverage$curr_yr, coverage$curr_mo) %>%
		 read_dta() %>%
		 # convert Stata string missing data to NAs
		 mutate_if(
			.predicate = is.character,
			~if_else(. == '', NA_character_, .)
		 ) %>%
		 select(-starts_with("CENTRAL_ID")) %>%
		 left_join(
			y  = harp$prep$new_reg %>%
			   select(prep_id, CENTRAL_ID),
			by = "prep_id"
		 )
   })
}
rm(check)