dqai        <- new.env()
dqai$msh    <- list()
dqai$msh$ss <- "16hYVzr3uHxZl2naFfjD23fbO3YuJ6j6-tGcpEC1217g"

local(envir = dqai, {
   lw_conn <- ohasis$conn("lw")
   forms   <- list()

   .log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- dbTable(
	  lw_conn,
	  "ohasis_warehouse",
	  "id_registry",
	  cols = c("CENTRAL_ID", "PATIENT_ID")
   )

   .log_success("Done.")
   dbDisconnect(lw_conn)
   rm(lw_conn)
})

# dqai$forms$id_registry <- gf$forms$id_registry

local(envir = dqai, {
   harp <- list()

   .log_info("Getting the new HARP Tx Datasets.")
   harp$tx$reg <- ohasis$get_data("harp_tx-reg", "2022", "06") %>%
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

   harp$tx$outcome <- ohasis$get_data("harp_tx-outcome", "2022", "06") %>%
	  read_dta() %>%
	  # convert Stata string missing data to NAs
	  mutate_if(
		 .predicate = is.character,
		 ~if_else(. == '', NA_character_, .)
	  ) %>%
	  select(-starts_with("CENTRAL_ID")) %>%
	  left_join(
		 y  = harp$tx$reg %>%
			select(art_id, CENTRAL_ID),
		 by = "art_id"
	  )
})

dqai$msh$data$hub_raw <- read_sheet(
   dqai$msh$ss,
   "MSH Masterlist 2022-06",
   col_types = "c"
) %>%
   select(
	  PATIENT_ID          = `OHASIS ID`,
	  dx_date             = DX,
	  confirm_date        = DX,
	  uic                 = UIC,
	  birthdate           = `Date of Birth\n(Day/Month/Year)`,
	  status              = STATUS,
	  last                = `LAST NAME`,
	  first               = `First Name`,
	  px_code             = `Hospital/Patient Code`,
	  confirmatory_code   = SACCL,
	  client_type         = `Type of Client`,
	  sex                 = Sex,
	  contact_no          = `Contact #`,
	  email               = Email,
	  age                 = Age,
	  baseline_cd4_result = `Baseline CD4 Count`,
	  artstart_date       = `Date Started on ARV\n(Day/Month/Year)`,
	  latest_regimen      = Regimen,
	  latest_ffupdate,
	  latest_nextpickup   = `Date of Next Refill\n(Day/Month/Year)`,
	  refill_remind,
	  ltfu                = `LOST TO FFUP`,
	  vl_date             = `Viral Load Date`,
	  vl_result           = `Viral Load Results`,
	  vl_suppressed       = `Virally Supressed`
   ) %>%
   mutate(
	  outcome = case_when(
		 stri_detect_fixed(status, "DECEASED") ~ "dead",
		 stri_detect_fixed(status, "/DIED") ~ "dead",
		 stri_detect_fixed(status, "NOT STARTED") ~ "(never started)",
		 stri_detect_fixed(status, "REFUSED TX") ~ "(never started)",
		 stri_detect_fixed(status, "DISCONTINUED") ~ "stopped",
		 stri_detect_fixed(status, "ONGOING") ~ "onart",
		 stri_detect_fixed(status, "T/O") ~ "transout",
		 stri_detect_fixed(status, "LTF") ~ "ltfu",
	  )
   ) %>%
   mutate_at(
	  .vars = vars(contains("date")),
	  ~case_when(
		 stri_detect_fixed(., "-") ~ as.Date(., format = "%Y-%m-%d"),
		 stri_detect_fixed(., "/") ~ as.Date(., format = "%m/%d/%Y"),
		 TRUE ~ NA_Date_
	  )
   )

dqai$msh$data$hub       <- read_sheet(dqai$msh$ss, "ZCMC Masterlist (2022-06)")
dqai$msh$data$eb        <- read_sheet(dqai$msh$ss, "EB Conso")
dqai$msh$data$dashboard <- bind_rows(
   read_sheet(dqai$msh$ss, "onart", col_types = "c"),
   read_sheet(dqai$msh$ss, "ltfu", col_types = "c"),
   read_sheet(dqai$msh$ss, "dead", col_types = "c"),
   read_sheet(dqai$msh$ss, "transout", col_types = "c")
)

dqai$msh$match$dashboard <- dqai$msh$data$dashboard %>%
   mutate(
	  final_px          = if_else(
		 condition = !is.na(`ZCMC Patient Code`),
		 true      = `ZCMC Patient Code`,
		 false     = `Patient Code`
	  ),
	  validated_outcome = tolower(`ZCMC Outcome`),
	  validated_outcome = case_when(
		 stri_detect_fixed(validated_outcome, "expire") ~ "dead",
		 stri_detect_fixed(validated_outcome, "transout") ~ "transout",
		 stri_detect_fixed(validated_outcome, "alive") ~ "onart",
		 validated_outcome %in% c("t.o", "to") ~ "transout",
		 TRUE ~ validated_outcome
	  )
   ) %>%
   filter(is.na(`ART ID #`) | !(`ART ID #` %in% c(44768, 79785)))

dqai$msh$match$eb <- dqai$msh$data$eb %>%
   filter(
	  is.na(`ART ID #`) | !(`ART ID #` %in% c(44768, 79785)),
	  is.na(chart_outcome) | !(chart_outcome %in% c("transient", "never client")),
	  !stri_detect_fixed(jun2022_faci_outcome, "never refill")
   )

dqai$msh$match$hub_raw <- dqai$msh$data$hub_raw %>%
   select(
	  num,
	  preval_outcome = outcome
   ) %>%
   full_join(
	  y  = dqai$msh$data$hub %>%
		 select(
			num,
			outcome,
			latest_regimen,
			duplicate_drop
		 ),
	  by = "num"
   ) %>%
   mutate(
	  latest_regimen = case_when(
		 latest_regimen == "N/A" ~ NA_character_,
		 latest_regimen == "N0" ~ NA_character_,
		 latest_regimen == "NA" ~ NA_character_,
		 TRUE ~ latest_regimen
	  )
   )

dqai$msh$match$hub <- dqai$msh$data$hub %>%
   mutate(
	  final_px       = px_code,
	  latest_regimen = case_when(
		 latest_regimen == "N/A" ~ NA_character_,
		 latest_regimen == "N0" ~ NA_character_,
		 latest_regimen == "NA" ~ NA_character_,
		 TRUE ~ latest_regimen
	  )
   ) %>%
   filter(
	  duplicate_drop == FALSE,
	  outcome %in% c("onart", "transout", "ltfu", "dead"),
	  !is.na(latest_regimen)
   )

dqai$msh$match$final <- dqai$msh$match$eb %>%
   full_join(
	  y  = dqai$msh$match$dashboard %>%
		 select(
			final_px,
			validated_outcome
		 ),
	  by = "final_px"
   ) %>%
   full_join(
	  y  = dqai$msh$match$hub %>%
		 select(
			final_px,
			faci_outcome = outcome
		 ),
	  by = "final_px"
   ) %>%
   filter(!is.na(outcome)) %>%
   relocate(faci_outcome, validated_outcome, .after = outcome) %>%
   mutate(
	  final_outcome = case_when(
		 outcome == faci_outcome & faci_outcome == validated_outcome ~ outcome,
		 outcome == validated_outcome ~ outcome,
		 outcome == "transout" & validated_outcome == "ltfu" ~ "transout",
		 outcome == "ltfu" & validated_outcome == "dead" ~ "dead",
		 (validated_outcome == "ltfu" | faci_outcome == "ltfu") & outcome == "ltfu" ~ "ltfu",
		 outcome == "never reported" & validated_outcome == "onart" ~ "onart",
		 outcome == "never reported" & validated_outcome == "ltfu" ~ "ltfu",
		 outcome == "never reported" & validated_outcome == "dead" ~ "dead",
		 outcome == "never reported" &
			faci_outcome == "transout" &
			validated_outcome == "transout" ~ "ltfu",
		 outcome == "ltfu" &
			faci_outcome == "onart" &
			validated_outcome == "onart" ~ "onart",
		 outcome == "ltfu" &
			faci_outcome == "onart" &
			validated_outcome == "transout" ~ "onart",
		 outcome == "never reported" &
			faci_outcome == "onart" &
			validated_outcome == "transout" ~ "onart",
		 outcome == "transout" &
			faci_outcome == "onart" &
			validated_outcome == "transout" ~ "onart",
		 outcome == "ltfu" & validated_outcome == "transout" ~ "ltfu",
		 outcome == "transout" & validated_outcome == "dead" ~ "dead",
		 outcome == "onart" & validated_outcome == "transout" ~ "onart",
		 outcome == "onart" & validated_outcome == "dead" ~ "dead",
		 outcome == "ltfu" & validated_outcome == "onart" ~ "onart",
	  ),
	  final_outcome = case_when(
		 final_outcome == "onart" & never_reported_outcome == "transout" ~ "transout",
		 TRUE ~ final_outcome
	  )
   )
# filter(is.na(final_outcome)) %>%
# View('data')
# .tab(final_outcome, outcome, faci_outcome, validated_outcome) %>%
# .tab(faci_outcome, outcome, final_outcome)

dqai$msh$match$eb %>%
   filter(outcome == "never reported") %>%
   select(
	  final_px,
	  PATIENT_ID = `OHASIS ID`,
   ) %>%
   left_join(
	  y  = dqai$forms$id_registry,
	  by = "PATIENT_ID"
   ) %>%
   left_join(
	  y  = dqai$harp$tx$reg %>%
		 select(
			art_id,
			CENTRAL_ID
		 ),
	  by = "CENTRAL_ID"
   ) %>%
   left_join(
	  y  = dqai$harp$tx$outcome %>%
		 select(
			art_id,
			hub,
			outcome
		 ),
	  by = "art_id"
   ) %>%
   select(
	  final_px,
	  hub,
	  outcome
   ) %>%
   write_clip()

dqai$msh$match$eb %>%
   filter(
	  jun2022_faci_outcome == "(pxcode not found in faci)"
   ) %>%
   left_join(
	  y  = dqai$msh$match$dashboard %>%
		 select(
			final_px,
			validated_outcome
		 ),
	  by = "final_px"
   ) %>%
   select(final_px, validated_outcome) %>%
   write_clip()
View("not in ml")


## final masterlist for the facility -------------------------------------------

dqai$msh$final$hub_ml <- dqai$msh$data$hub_raw %>%
   left_join(
	  y  = dqai$msh$data$hub %>%
		 select(
			num,
			duplicate_drop
		 ),
	  by = "num"
   ) %>%
   filter(duplicate_drop == FALSE) %>%
   full_join(
	  y  = dqai$msh$match$hub %>%
		 select(
			num,
			final_px,
		 ) %>%
		 full_join(
			y  = dqai$msh$match$eb %>%
			   filter(
				  is.na(`ART ID #`) | !(`ART ID #` %in% c(44768, 79785)),
			   ) %>%
			   rename(art_id = `ART ID #`) %>%
			   mutate(art_id = as.numeric(art_id)) %>%
			   left_join(
				  y  = dqai$harp$tx$reg %>%
					 select(
						art_id,
						idnum
					 ),
				  by = "art_id"
			   ) %>%
			   select(
				  final_px,
				  idnum
			   ),
			by = "final_px"
		 ),
	  by = "num"
   ) %>%
   mutate(
	  final_px = if_else(
		 condition = !is.na(final_px),
		 true      = final_px,
		 false     = px_code,
		 missing   = final_px
	  )
   ) %>%
   left_join(
	  y  = dqai$msh$data$dashboard %>%
		 filter(
			is.na(`ART ID #`) | !(`ART ID #` %in% c(44768, 79785)),
		 ) %>%
		 mutate(
			final_px = if_else(
			   condition = !is.na(`ZCMC Patient Code`),
			   true      = `ZCMC Patient Code`,
			   false     = `Patient Code`
			),
		 ) %>%
		 select(
			final_px,
			`First Name`,
			`Middle Name`,
			`Last Name`,
			`UIC`,
			`OHASIS ID`,
			`Confirmatory Code`,
			`Birth Date`
		 ),
	  by = "final_px"
   ) %>%
   left_join(
	  y  = dqai$msh$match$final %>%
		 select(
			final_px,
			final_outcome
		 ) %>%
		 mutate(
			`Given Meds?` = TRUE
		 ),
	  by = "final_px"
   ) %>%
   relocate(final_px, .after = px_code) %>%
   relocate(`Confirmatory Code`, .after = confirmatory_code) %>%
   relocate(`Birth Date`, .after = birthdate) %>%
   mutate(
	  birthdate         = if_else(
		 condition = !is.na(birthdate),
		 true      = birthdate,
		 false     = as.Date(`Birth Date`),
		 missing   = birthdate
	  ),
	  confirmatory_code = if_else(
		 condition = !is.na(idnum),
		 true      = `Confirmatory Code`,
		 false     = confirmatory_code,
		 missing   = confirmatory_code
	  )
   ) %>%
   select(
	  `OHASIS ID`,
	  `Client Status`       = final_outcome,
	  No.                   = num,
	  Code                  = final_px,
	  `UIC`,
	  `Last Name`,
	  `First Name`,
	  `Middle Name`,
	  `Confirmatory Code`   = confirmatory_code,
	  `Birth Date`          = birthdate,
	  `Date Dx`             = confirm_date,
	  `Place Diagnosed`     = place_dx,
	  `Address`             = address,
	  `Initial CD4: Date`   = baseline_cd4_date,
	  `Initial CD4: Result` = baseline_cd4_result,
	  `Date ART Started`    = artstart_date,
	  `ARV Regimen`         = latest_regimen,
	  `Given Meds?`,
	  `1st Hub Visit`       = transin_date,
	  `Remarks`             = faci_outcome,
	  `TB tx`,
	  `INH`,
	  `Hbsag+`
   ) %>%
   distinct_all()