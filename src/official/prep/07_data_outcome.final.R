##  Append w/ old Registry -----------------------------------------------------

finalize_outcomes <- function(data) {
   data %<>%
      arrange(prep_id) %>%
      mutate(
         curr_outcome     = case_when(
            prev_outcome == "2_ltfu" & curr_outcome == "5_not on prep" ~ "2_ltfu",
            TRUE ~ curr_outcome
         ),
         newonprep        = if_else(
            condition = year(prepstart_date) == as.numeric(ohasis$yr) &
               month(prepstart_date) == as.numeric(ohasis$mo),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         onprep           = if_else(
            condition = curr_outcome == "1_on prep",
            true      = 1,
            false     = 0,
            missing   = 0
         ),

         reinit_diff      = interval(prev_reinit, prep_reinit_date) %/% months(1),
         prep_reinit_date = case_when(
            prep_reinit_date > prev_reinit & (reinit_diff > 0 & reinit_diff <= 3) ~ prev_reinit,
            prep_reinit_date < prev_reinit & (reinit_diff > 3) ~ prep_reinit_date,
            TRUE ~ prep_reinit_date
         )
      )
   return(data)
}

finalize_faci <- function(data) {
   data %<>%
      select(
         -any_of(
            c(
               "prep_reg",
               "prep_prov",
               "prep_munc"
            )
         )
      ) %>%
      rename(
         faci   = curr_faci,
         branch = curr_branch,
      ) %>%
      mutate(
         branch = case_when(
            faci == "HASH" & branch == "HASH" ~ "HASH-QC",
            faci == "HASH" & is.na(branch) ~ "HASH-QC",
            faci == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            TRUE ~ branch
         ),
      ) %>%
      mutate_at(
         .vars = vars(faci),
         ~case_when(
            stri_detect_regex(., "^HASH") ~ "HASH",
            stri_detect_regex(., "^SAIL") ~ "SAIL",
            stri_detect_regex(., "^TLY") ~ "TLY",
            TRUE ~ .
         )
      ) %>%
      mutate(
         branch = case_when(
            faci == "HASH" & is.na(branch) ~ "HASH-QC",
            faci == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            faci == "SHP" & is.na(branch) ~ "SHIP-MAKATI",
            TRUE ~ branch
         ),
      ) %>%
      left_join(
         y  = ohasis$ref_faci_code %>%
            mutate(
               FACI_CODE     = case_when(
                  stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
                  stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
                  stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
                  TRUE ~ FACI_CODE
               ),
               SUB_FACI_CODE = if_else(
                  condition = nchar(SUB_FACI_CODE) == 3,
                  true      = NA_character_,
                  false     = SUB_FACI_CODE
               ),
               SUB_FACI_CODE = case_when(
                  FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
                  FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
                  FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
                  TRUE ~ SUB_FACI_CODE
               ),
            ) %>%
            select(
               faci      = FACI_CODE,
               branch    = SUB_FACI_CODE,
               prep_reg  = FACI_NHSSS_REG,
               prep_prov = FACI_NHSSS_PROV,
               prep_munc = FACI_NHSSS_MUNC,
            ),
         by = c("faci", "branch")
      ) %>%
      select(
         # REC_ID,
         # HTS_REC,
         # CENTRAL_ID,
         # prep_id,
         # mort_id,
         # uic,
         # px_code,
         # self_identity,
         # self_identity_other,
         # sexwithm,
         # sexwithf,
         # sexwithpro,
         # regularlya,
         # injectdrug,
         # chemsex,
         # curr_age,
         # prepstart_date,
         # prep_reinit_date,
         # with_hts,
         # faci,
         # branch,
         # prep_reg,
         # prep_prov,
         # prep_munc,
         # starts_with("kp_"),
         # starts_with("curr_", ignore.case = FALSE),
         -starts_with("prev_", ignore.case = FALSE),
         -any_of("age")
      ) %>%
      rename(
         latest_ffupdate   = curr_ffup,
         latest_nextpickup = curr_pickup,
         latest_regimen    = curr_regimen,
      ) %>%
      rename_at(
         .var = vars(starts_with("curr_")),
         ~stri_replace_all_regex(., "^curr_", "")
      ) %>%
      distinct_all() %>%
      arrange(prep_id) %>%
      rename(curr_age = age) %>%
      mutate(central_id = CENTRAL_ID)

   return(data)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- read_rds(file.path(wd, "outcome.converted.RDS"))
      data <- finalize_outcomes(data) %>%
         finalize_faci()

      .GlobalEnv$nhsss$prep$official$new_outcome <- data
   })
}
