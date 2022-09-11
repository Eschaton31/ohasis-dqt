dqai        <- new.env()
dqai$spm    <- list()
dqai$spm$ss <- "19ed3X8Vza4LfL17ElHb15brpslQeRyskhmgnZrtHdTw"

local(envir = dqai, {
   lw_conn <- ohasis$conn("db")
   forms   <- list()

   .log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- dbTable(
      lw_conn,
      "ohasis_interim",
      "registry",
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

dqai$spm$data$hub       <- read_sheet(dqai$spm$ss, "SPM Masterlist 2022-06")
dqai$spm$data$eb        <- read_sheet(dqai$spm$ss, "EB Conso")
dqai$spm$data$dashboard <- bind_rows(
   read_sheet(dqai$spm$ss, "onart", col_types = "c"),
   read_sheet(dqai$spm$ss, "ltfu", col_types = "c"),
   read_sheet(dqai$spm$ss, "dead", col_types = "c"),
   read_sheet(dqai$spm$ss, "transout", col_types = "c")
)

dqai$spm$match$dashboard <- dqai$spm$data$dashboard %>%
   mutate(
      final_px          = if_else(
         condition = !is.na(`SPMC Patient Code`),
         true      = `SPMC Patient Code`,
         false     = `Patient Code`
      ),
      validated_outcome = tolower(`SPMC Outcome`),
      validated_outcome = case_when(
         stri_detect_fixed(validated_outcome, "active") ~ "onart",
         stri_detect_fixed(validated_outcome, "trans-") ~ "transout",
         stri_detect_fixed(validated_outcome, "trand-") ~ "transout",
         stri_detect_fixed(validated_outcome, "expire") ~ "dead",
         stri_detect_fixed(validated_outcome, "transout") ~ "transout",
         stri_detect_fixed(validated_outcome, "alive") ~ "onart",
         validated_outcome %in% c("t.o", "to") ~ "transout",
         TRUE ~ validated_outcome
      )
   )

dqai$spm$match$eb <- dqai$spm$data$eb %>%
   select(-final_px) %>%
   rename(
      final_px = `Confirmatory Code`,

   )

dqai$spm$match$hub <- dqai$spm$data$hub %>%
   as.data.frame() %>%
   as_tibble() %>%
   rename(
      outcome  = EB_Tag,
      final_px = `Confirmatory Code`
   ) %>%
   filter(
      is.na(outcome) | !stri_detect_fixed(outcome, "not started")
   )

dqai$spm$match$final <- dqai$spm$match$eb %>%
   full_join(
      y  = dqai$spm$match$dashboard %>%
         select(
            final_px = `Confirmatory Code`,
            validated_outcome
         ),
      by = "final_px"
   ) %>%
   full_join(
      y  = dqai$spm$match$hub %>%
         select(
            final_px,
            faci_outcome = outcome
         ) %>%
         mutate(final_px = as.character(final_px)),
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
         outcome == "onart" & validated_outcome == "ltfu" ~ "onart",
         outcome == "onart" & validated_outcome == "transient" ~ "transient",
         outcome == "transout" & validated_outcome == "onart" ~ "transout",
         outcome == "transout" & validated_outcome == "#n/a" ~ "transout",
         outcome == "transout" & validated_outcome == "transient" ~ "transient",
         outcome == "ltfu" & validated_outcome == "pep" ~ "pep",
         outcome == "ltfu" & validated_outcome == "ofw" ~ "transout - overseas",
         outcome == "dead" ~ "dead",
         is.na(validated_outcome) & is.na(faci_outcome) ~ outcome,
         outcome == "onart" & faci_outcome == "onart" ~ outcome,
      ),
      # final_outcome = case_when(
      #    final_outcome == "onart" & never_reported_outcome == "transout" ~ "transout",
      #    TRUE ~ final_outcome
      # )
   )
dqai$spm$match$final %>%
   tab(final_outcome, outcome, faci_outcome, validated_outcome) %>%
   print(n = 1000)
dqai$spm$match$final %>% tab(outcome, faci_outcome)
dqai$spm$match$final %>% tab(final_outcome, validated_outcome, `SPMC Facility (referred to)`)
dqai$spm$match$final %>% tab(final_outcome)
# filter(is.na(final_outcome)) %>%
# View('data')
# .tab(faci_outcome, outcome, final_outcome)

dqai$spm$match$eb %>%
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

dqai$spm$match$eb %>%
   filter(
      jun2022_faci_outcome == "(pxcode not found in faci)"
   ) %>%
   left_join(
      y  = dqai$spm$match$dashboard %>%
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

dqai$spm$final$hub_ml <- dqai$spm$data$hub_raw %>%
   left_join(
      y  = dqai$spm$data$hub %>%
         select(
            num,
            duplicate_drop
         ),
      by = "num"
   ) %>%
   filter(duplicate_drop == FALSE) %>%
   full_join(
      y  = dqai$spm$match$hub %>%
         select(
            num,
            final_px,
         ) %>%
         full_join(
            y  = dqai$spm$match$eb %>%
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
      y  = dqai$spm$data$dashboard %>%
         filter(
            is.na(`ART ID #`) | !(`ART ID #` %in% c(44768, 79785)),
         ) %>%
         mutate(
            final_px = if_else(
               condition = !is.na(`SPMC Patient Code`),
               true      = `SPMC Patient Code`,
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
      y  = dqai$spm$match$final %>%
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