dqai        <- new.env()
dqai$ddh    <- list()
dqai$ddh$ss <- "1TAye1Vyejbq-GFiVGpR9D1e9kNFyyxZUWAt26M9ZylM"

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

# dqai$ddh$data$hub_raw <- read_sheet(
#    dqai$ddh$ss,
#    "DDH Masterlist 2022-08",
#    col_types = "c"
# ) %>%
#    select(
# 	  PATIENT_ID          = `OHASIS ID`,
# 	  dx_date             = DX,
# 	  confirm_date        = DX,
# 	  uic                 = UIC,
# 	  birthdate           = `Date of Birth\n(Day/Month/Year)`,
# 	  status              = STATUS,
# 	  last                = `LAST NAME`,
# 	  first               = `First Name`,
# 	  px_code             = `Hospital/Patient Code`,
# 	  confirmatory_code   = SACCL,
# 	  client_type         = `Type of Client`,
# 	  sex                 = Sex,
# 	  contact_no          = `Contact #`,
# 	  email               = Email,
# 	  age                 = Age,
# 	  baseline_cd4_result = `Baseline CD4 Count`,
# 	  artstart_date       = `Date Started on ARV\n(Day/Month/Year)`,
# 	  latest_regimen      = Regimen,
# 	  latest_ffupdate,
# 	  latest_nextpickup   = `Date of Next Refill\n(Day/Month/Year)`,
# 	  refill_remind,
# 	  ltfu                = `LOST TO FFUP`,
# 	  vl_date             = `Viral Load Date`,
# 	  vl_result           = `Viral Load Results`,
# 	  vl_suppressed       = `Virally Supressed`
#    ) %>%
#    mutate(
# 	  outcome = case_when(
# 		 stri_detect_fixed(status, "DECEASED") ~ "dead",
# 		 stri_detect_fixed(status, "/DIED") ~ "dead",
# 		 stri_detect_fixed(status, "NOT STARTED") ~ "(never started)",
# 		 stri_detect_fixed(status, "REFUSED TX") ~ "(never started)",
# 		 stri_detect_fixed(status, "DISCONTINUED") ~ "stopped",
# 		 stri_detect_fixed(status, "ONGOING") ~ "onart",
# 		 stri_detect_fixed(status, "T/O") ~ "transout",
# 		 stri_detect_fixed(status, "LTF") ~ "ltfu",
# 	  )
#    ) %>%
#    mutate_at(
# 	  .vars = vars(contains("date")),
# 	  ~case_when(
# 		 stri_detect_fixed(., "-") ~ as.Date(., format = "%Y-%m-%d"),
# 		 stri_detect_fixed(., "/") ~ as.Date(., format = "%m/%d/%Y"),
# 		 TRUE ~ NA_Date_
# 	  )
#    )

dqai$ddh$data$hub       <- read_sheet(dqai$ddh$ss, "DDH Masterlist 2022-08")
dqai$ddh$data$eb        <- read_sheet(dqai$ddh$ss, "EB Conso")
dqai$ddh$data$dashboard <- bind_rows(
   read_sheet(dqai$ddh$ss, "onart", col_types = "c"),
   read_sheet(dqai$ddh$ss, "ltfu", col_types = "c"),
   read_sheet(dqai$ddh$ss, "dead", col_types = "c"),
   read_sheet(dqai$ddh$ss, "transout", col_types = "c")
)

dqai$ddh$match$dashboard <- dqai$ddh$data$dashboard %>%
   mutate(
      final_px          = case_when(
         !is.na(`DDH Patient Code`) ~ `DDH Patient Code`,
         !is.na(`Patient Code`) ~ `Patient Code`,
         !is.na(`DDH UIC`) ~ `DDH UIC`,
         !is.na(UIC) ~ UIC,
      ),
      validated_outcome = tolower(`DDH Outcome`),
      validated_outcome = case_when(
         stri_detect_fixed(validated_outcome, "expire") ~ "dead",
         stri_detect_fixed(validated_outcome, "transout") ~ "transout",
         stri_detect_fixed(validated_outcome, "alive") ~ "onart",
         validated_outcome %in% c("t.o", "to") ~ "transout",
         TRUE ~ validated_outcome
      )
   )

dqai$ddh$match$eb  <- dqai$ddh$data$eb %>%
   select(-final_px) %>%
   rename(final_px = `Final ID`)
dqai$ddh$match$hub <- dqai$ddh$data$hub

dqai$ddh$match$final <- dqai$ddh$match$eb %>%
   full_join(
      y  = dqai$ddh$match$dashboard %>%
         select(
            final_px,
            validated_outcome
         ),
      by = "final_px"
   ) %>%
   full_join(
      y  = dqai$ddh$match$hub %>%
         select(
            final_px,
            faci_outcome = Outcome
         ),
      by = "final_px"
   ) %>%
   filter(!is.na(outcome)) %>%
   relocate(faci_outcome, validated_outcome, .after = outcome) %>%
   mutate(
      final_outcome = case_when(
         validated_outcome == "stopped - financial" ~ "stopped - financial",
         outcome == "transient" ~ "transient",
         outcome == faci_outcome & faci_outcome == validated_outcome ~ outcome,
         outcome == validated_outcome ~ outcome,
         outcome == "transout" & validated_outcome == "ltfu" ~ "transout",
         outcome == "ltfu" & validated_outcome == "dead" ~ "dead",
         (validated_outcome == "ltfu" | faci_outcome == "ltfu") & outcome == "ltfu" ~ "ltfu",
         outcome == "never reported" & validated_outcome == "onart" ~ "onart",
         outcome == "never reported" & validated_outcome == "ltfu" ~ "ltfu",
         outcome == "never reported" & validated_outcome == "dead" ~ "dead",
         outcome == "never reported" & validated_outcome == "transout" ~ "ltfu",
         outcome == "never reported" &
            faci_outcome == "transout" &
            validated_outcome == "transout" ~ "ltfu",
         outcome == "never reported" &
            faci_outcome == "transout/PDL" &
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
         outcome == "onart" & validated_outcome == "transient" ~ "transient",
         outcome == "ltfu" & validated_outcome == "onart" ~ "onart",
      ),
      # final_outcome = case_when(
      #    final_outcome == "onart" & never_reported_outcome == "transout" ~ "transout",
      #    TRUE ~ final_outcome
      # )
   )
# filter(is.na(final_outcome)) %>%
# View('data')
# .tab(final_outcome, outcome, faci_outcome, validated_outcome) %>%
# .tab(faci_outcome, outcome, final_outcome)

dqai$ddh$match$eb %>%
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

dqai$ddh$match$eb %>%
   filter(
      jun2022_faci_outcome == "(pxcode not found in faci)"
   ) %>%
   left_join(
      y  = dqai$ddh$match$dashboard %>%
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

dqai$ddh$final$hub_ml <- dqai$ddh$data$hub_raw %>%
   left_join(
      y  = dqai$ddh$data$hub %>%
         select(
            num,
            duplicate_drop
         ),
      by = "num"
   ) %>%
   filter(duplicate_drop == FALSE) %>%
   full_join(
      y  = dqai$ddh$match$hub %>%
         select(
            num,
            final_px,
         ) %>%
         full_join(
            y  = dqai$ddh$match$eb %>%
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
      y  = dqai$ddh$data$dashboard %>%
         filter(
            is.na(`ART ID #`) | !(`ART ID #` %in% c(44768, 79785)),
         ) %>%
         mutate(
            final_px = if_else(
               condition = !is.na(`DDH Patient Code`),
               true      = `DDH Patient Code`,
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
      y  = dqai$ddh$match$final %>%
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