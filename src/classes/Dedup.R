Dedup <- R6Class(
   "Dedup",
   public  = list(
      left         = list(
         data = tibble(),
         id   = character()
      ),
      right        = list(
         data = tibble(),
         id   = character()
      ),
      match        = list(),
      review       = list(),

      setMaster    = function(data, id) {
         self$left$data <- data
         self$left$id   <- id
      },

      setUsing     = function(data, id) {
         self$right$data <- data
         self$right$id   <- id
      },

      preparePii   = function() {
         self$match$left <- self$left$data %>%
            rename_all(private$renameColumns) %>%
            select(
               CENTRAL_ID,
               all_of(self$left$id),
               all_of(private$requiredColumns)
            ) %>%
            private$auxColumns(self$left$id)

         if (nrow(self$right$data) > 0) {
            self$match$right <- self$right$data %>%
               rename_all(private$renameColumns) %>%
               select(
                  CENTRAL_ID,
                  all_of(self$right$id),
                  all_of(private$requiredColumns)
               ) %>%
               private$auxColumns(self$right$id)
         }

         invisible(self)
      },

      reclink      = function() {
         reclink_df <- fastLink(
            dfA              = self$match$left,
            dfB              = self$match$right,
            varnames         = c(
               "given_name",
               "middle_name",
               "family_name",
               "suffix_name",
               "birth_yr",
               "birth_mo",
               "birth_dy"
            ),
            stringdist.match = c(
               "given_name",
               "middle_name",
               "family_name"
            ),
            partial.match    = c(
               "given_name",
               "family_name"
            ),
            numeric.match    = c(
               "birth_yr",
               "birth_mo",
               "birth_dy"
            ),
            threshold.match  = 0.95,
            cut.a            = 0.90,
            cut.p            = 0.85,
            dedupe.matches   = FALSE,
            n.cores          = 4
         )

         if (length(reclink_df$matches$inds.a) > 0) {
            reclink_matched <- getMatches(
               dfA         = self$match$left,
               dfB         = self$match$right,
               fl.out      = reclink_df,
               combine.dfs = FALSE
            )

            reclink_review <- reclink_matched$dfA.match %>%
               mutate(
                  match_id = row_number()
               ) %>%
               select(
                  posterior,
                  match_id,
                  left_cid          = central_id,
                  left_given_name   = given_name,
                  left_middle_name  = middle_name,
                  left_family_name  = family_name,
                  left_suffix_name  = suffix_name,
                  left_birthdate    = birthdate,
                  left_confirmatory = confirmatory_code,
                  left_uic          = uic,
                  left_pxcode       = patient_code
               ) %>%
               left_join(
                  y  = reclink_matched$dfB.match %>%
                     mutate(
                        match_id = row_number()
                     ) %>%
                     select(
                        match_id,
                        right_cid          = central_id,
                        right_given_name   = given_name,
                        right_middle_name  = middle_name,
                        right_family_name  = family_name,
                        right_suffix_name  = suffix_name,
                        right_birthdate    = birthdate,
                        right_confirmatory = confirmatory_code,
                        right_uic          = uic,
                        right_pxcode       = patient_code,
                     ),
                  by = join_by(match_id)
               ) %>%
               mutate(
                  left_name  = stri_c(left_family_name, ", ", left_given_name, " ", left_middle_name, " ", left_suffix_name, ignore_null = TRUE),
                  right_name = stri_c(right_family_name, ", ", right_given_name, " ", right_middle_name, " ", right_suffix_name, ignore_null = TRUE),
               ) %>%
               select(
                  -ends_with("given_name"),
                  -ends_with("middle_name"),
                  -ends_with("family_name"),
                  -ends_with("suffix_name"),
               ) %>%
               arrange(desc(posterior)) %>%
               # Additional sift through of matches
               mutate(
                  # levenshtein
                  name_levenshtein = stringsim(left_name, right_name, method = 'levenshtein'),
                  # jaro-winkler
                  name_jarowinkler = stringsim(left_name, right_name, method = 'jw'),
                  # qgram
                  name_qgram       = stringsim(left_name, right_name, method = 'qgram', q = 3),
                  avg_dist         = (name_levenshtein + name_jarowinkler + name_qgram) / 3,
               ) %>%
               # choose 60% and above match
               filter(avg_dist >= 0.60, !is.na(posterior))

            # assign to global env
            self$review$reclink <- reclink_review %>%
               mutate(
                  Bene  = NA_character_,
                  Gab   = NA_character_,
                  Lala  = NA_character_,
                  Angie = NA_character_,
                  # ) %>%
                  # anti_join(
                  #    y  = non_dupes %>%
                  #       select(USING_CID = PATIENT_ID, MASTER_CID = NON_PAIR_ID),
                  #    by = join_by(USING_CID, MASTER_CID)
               )
         }

         log_success("Done.")

         invisible(self)
      },

      splinkDedupe = function() {
         env <- "dqt-dedup"

         if (!virtualenv_exists(env)) {
            virtualenv_create(env)
            virtualenv_install(env, c("pandas", "splink"))
         }

         log_info("Initializing virtual environment.")
         suppress_warnings(use_virtualenv(env), "The request to")

         log_info("Loading Splink.")
         sp  <- import("splink", as = "sp", convert = FALSE)
         cl  <- import("splink.comparison_library", as = "cl", convert = FALSE)
         cll <- import("splink.comparison_level_library", as = "cll", convert = FALSE)

         log_info("Use DuckDB.")
         db_api <- sp$DuckDBAPI()

         uic_comparison       <- cl$CustomComparison(
            output_column_name     = "uic",
            comparison_description = "UIC",
            comparison_levels      = c(
               cll$NullLevel("uic"),
               cll$ExactMatchLevel("uic"),
               cll$CustomLevel("concat(uic_mom_l, uic_order_l, birthdate_l) = concat(uic_dad_r, uic_order_r, birthdate_r)"),
               cll$CustomLevel("concat(uic_dad_l, uic_order_l, birthdate_l) = concat(uic_mom_r, uic_order_r, birthdate_r)"),
               cll$ElseLevel()
            )
         )
         full_name_comparison <- cl$CustomComparison(
            output_column_name     = "full_name",
            comparison_description = "First+Last",
            comparison_levels      = c(
               cll$NullLevel("full_name"),
               cll$ExactMatchLevel("full_name"),
               cll$ColumnsReversedLevel("given_name", "family_name"),
               cll$ElseLevel()
            )
         )
         last_name_comparison <- cl$CustomComparison(
            output_column_name     = "last_name",
            comparison_description = "Middle+Last",
            comparison_levels      = c(
               cll$NullLevel("last_name"),
               cll$ExactMatchLevel("last_name"),
               cll$ColumnsReversedLevel("middle_name", "family_name"),
               cll$ElseLevel()
            )
         )

         log_info("Creating settings.")
         settings <- sp$SettingsCreator(
            unique_id_column_name                  = self$left$id,
            link_type                              = "dedupe_only",
            blocking_rules_to_generate_predictions = c(
               sp$block_on("given_name_sieve"),
               sp$block_on("family_name_sieve"),
               sp$block_on("birthdate"),
               sp$block_on("given_name_3", "family_name_sieve"),
               sp$block_on("birth_yr", "given_name_sieve"),
               sp$block_on("residence_province", "given_name_sieve"),
               sp$block_on("residence_province", "family_name_sieve")
            ),
            comparisons                            = c(
               cl$NameComparison("given_name"),
               cl$NameComparison("middle_name"),
               cl$NameComparison("family_name"),
               cl$NameComparison("given_name_metaphone"),
               cl$NameComparison("family_name_metaphone"),
               cl$DateOfBirthComparison(
                  "birthdate",
                  input_is_string     = TRUE,
                  datetime_metrics    = c("year", "month", "day"),
                  datetime_thresholds = c(1, 1, 10),
               ),
               cl$ExactMatch("residence_region"),
               cl$ExactMatch("residence_province"),
               uic_comparison,
               full_name_comparison,
               last_name_comparison
            )
         )

         log_info("Cleaning data.")
         df <- self$match$left %>%
            mutate_if(
               .predicate = is.labelled,
               ~to_character(.)
            ) %>%
            mutate_if(
               .predicate = is.Date,
               ~as.character(.)
            ) %>%
            mutate_if(
               .predicate = is.character,
               ~na_if(., "")
            ) %>%
            r_to_py()

         log_info("Starting linker.")
         linker <- sp$Linker(df, settings, db_api)

         # Model training: Estimate the parameters of the model
         log_info("Creating Fellegi-Sunter Model.")
         linker$
            training$
            estimate_probability_two_random_records_match(sp$block_on("given_name_sieve", "family_name_sieve"), recall = 0.7)
         linker$training$estimate_u_using_random_sampling(max_pairs = 1e6)

         log_info("EM Algorithm = {green('First Name')}.")
         linker$
            training$
            estimate_parameters_using_expectation_maximisation(sp$block_on("given_name_sieve"))

         log_info("EM Algorithm = {green('Last Name')}.")
         linker$
            training$
            estimate_parameters_using_expectation_maximisation(sp$block_on("family_name_sieve"))

         log_info("EM Algorithm = {green('Birth Date')}.")
         linker$
            training$
            estimate_parameters_using_expectation_maximisation(sp$block_on("birthdate"))

         log_info("Generating match pairs.")
         pairwise_predictions <- linker$inference$predict(threshold_match_weight = -10)

         log_info("Finalizing estimation object.")
         estimates <- pairwise_predictions$as_pandas_dataframe()

         log_info("Collecting estimation data.")
         matches <- bind_rows(py_to_r(estimates))

         log_info("Done!")
         self$review$splinkDedup <- matches %>%
            select(
               -starts_with("gamma"),
               -ends_with("metaphone")
            ) %>%
            arrange(desc(match_probability))

         invisible(self)
      }
   ),

   private = list(
      requiredColumns = c(
         "given_name",
         "middle_name",
         "family_name",
         "suffix_name",
         "birthdate",
         "sex",
         "uic",
         "confirmatory_code",
         "patient_code",
         "philhealth_no",
         "philsys_id",
         "client_mobile",
         "client_email",
         "residence_region",
         "residence_province",
         "residence_muncity"
      ),

      renameColumns   = function(name) {
         lower_name <- tolower(name)
         new        <- case_when(
            lower_name == "labcode2" ~ "confirmatory_code",
            lower_name == "confirmatory_code" ~ "confirmatory_code",
            lower_name == "uic" ~ "uic",
            lower_name == "px_code" ~ "patient_code",
            lower_name == "patient_code" ~ "patient_code",
            lower_name == "client_code" ~ "patient_code",
            lower_name == "firstname" ~ "given_name",
            lower_name == "first" ~ "given_name",
            lower_name == "fname" ~ "given_name",
            lower_name == "middle" ~ "middle_name",
            lower_name == "mname" ~ "middle_name",
            lower_name == "last" ~ "family_name",
            lower_name == "lname" ~ "family_name",
            lower_name == "suffix" ~ "suffix_name",
            lower_name == "name_suffix" ~ "suffix_name",
            lower_name == "bdate" ~ "birthdate",
            lower_name == "birthdate" ~ "birthdate",
            lower_name == "date_of_birth" ~ "birthdate",
            lower_name == "philhealth" ~ "philhealth_no",
            lower_name == "philhealth_no" ~ "philhealth_no",
            lower_name == "philhealth_num" ~ "philhealth_no",
            lower_name == "philsys" ~ "philsys_id",
            lower_name == "philsys_id" ~ "philsys_id",
            lower_name == "sex" ~ "sex",
            lower_name == "sex_at_birth" ~ "sex",
            lower_name == "client_mobile" ~ "client_mobile",
            lower_name == "mobile" ~ "client_mobile",
            lower_name == "mobile_no" ~ "client_mobile",
            lower_name == "email" ~ "client_email",
            lower_name == "email_address" ~ "client_email",
            lower_name == "permcurr_reg" ~ "residence_region",
            lower_name == "permcurr_prov" ~ "residence_province",
            lower_name == "permcurr_munc" ~ "residence_muncity",
            lower_name == "region" ~ "residence_region",
            lower_name == "province" ~ "residence_province",
            lower_name == "muncity" ~ "residence_muncity",
            TRUE ~ name
         )

         return(new)
      },

      ensureColumns   = function(data) {
         missing_cols <- setdiff(private$requireColumns, names(data))
         if (length(missing_cols) > 0) {
            for (col in missing_cols) {
               data %<>%
                  mutate(
                     new_col = if (col == "birthdate") new_col = NA_Date_ else NA_character_,
                  ) %>%
                  rename_at(
                     .vars = vars(new_col),
                     ~col
                  )
            }
         }

         return(data)
      },

      auxColumns      = function(data, id_col) {
         dedup_new <- data %>%
            mutate_if(is.character, toupper) %>%
            mutate_if(is.character, str_squish) %>%
            mutate_if(is.character, ~stri_trans_general(., "latin-ascii")) %>%
            mutate_if(is.character, clean_pii) %>%
            mutate(
               # get components of birthdate
               birth_yr                = as.numeric(year(birthdate)),
               birth_mo                = as.numeric(month(birthdate)),
               birth_dy                = as.numeric(day(birthdate)),

               # extract parent info from uic
               uic_mom                 = substr(uic, 1, 2),
               uic_dad                 = substr(uic, 3, 4),
               uic_order               = substr(uic, 5, 6),

               # variables for first 3 letters of names
               given_name_1            = substr(given_name, 1, 1),
               middle_name_1           = substr(middle_name, 1, 1),
               family_name_1           = substr(family_name, 1, 1),
               given_name_3            = substr(given_name, 1, 3),
               middle_name_3           = substr(middle_name, 1, 3),
               family_name_3           = substr(family_name, 1, 3),

               # family_name       = coalesce(family_name, middle_name),
               # middle_name       = coalesce(middle_name, family_name),
               full_name               = stri_c(given_name, " ", family_name, ignore_null = TRUE),
               last_name               = stri_c(middle_name, " ", family_name, ignore_null = TRUE),

               # clean ids
               confirmatory_code_sieve = confirmatory_code,
               patient_code_sieve      = patient_code,
               given_name_sieve        = given_name,
               middle_name_sieve       = middle_name,
               family_name_sieve       = family_name,
               philhealth_no_sieve     = philhealth_no,
               philsys_id_sieve        = philsys_id,
            ) %>%
            mutate_at(
               .vars = vars(ends_with("_sieve", ignore.case = TRUE)),
               ~str_replace_all(., "[^[:alnum:]]", "")
            ) %>%
            mutate_at(
               .vars = vars(given_name_sieve, middle_name_sieve, family_name_sieve),
               ~str_replace_all(., "([[:alnum:]])\\1+", "\\1")
            ) %>%
            mutate(
               # code standard names
               given_name_soundex    = soundex(given_name),
               given_name_metaphone  = metaphone(given_name),
               middle_name_soundex   = soundex(middle_name),
               middle_name_metaphone = metaphone(middle_name),
               family_name_soundex   = soundex(family_name),
               family_name_metaphone = metaphone(family_name),
            )

         log_info("Splitting UIC.")
         # genearte UIC w/o 1 parent, 2 combinations
         dedup_new_uic <- dedup_new %>%
            filter(!is.na(uic)) %>%
            rename(
               row_id = id_col
            ) %>%
            select(
               row_id,
               uic_mom,
               uic_dad
            ) %>%
            pivot_longer(
               cols      = c(uic_mom, uic_dad),
               names_to  = 'uic',
               values_to = 'given_name_two'
            ) %>%
            arrange(row_id, given_name_two) %>%
            group_by(row_id) %>%
            mutate(uic = row_number()) %>%
            ungroup() %>%
            pivot_wider(
               id_cols      = row_id,
               names_from   = uic,
               names_prefix = 'uic_',
               values_from  = given_name_two
            ) %>%
            rename(
               !!id_col := row_id
            )

         log_info("Sorting UIC.")
         dedup_new %<>%
            left_join(
               y  = dedup_new_uic,
               by = id_col
            ) %>%
            mutate(
               uic_sort = stri_c(uic_1, uic_2, substr(uic, 5, 14))
            )

         log_info("Sorting Names.")
         dedup_new_names <- dedup_new %>%
            rename(
               row_id = id_col
            ) %>%
            select(
               row_id,
               name_1 = given_name_sieve,
               name_2 = middle_name_sieve,
               name_3 = family_name_sieve
            ) %>%
            filter(if_any(c(name_1, name_2, name_3), ~!is.na(.))) %>%
            pivot_longer(
               cols      = c(name_1, name_2, name_3),
               names_to  = "name",
               values_to = "value",
            ) %>%
            mutate(
               value = if_else(nchar(value) == 1, NA_character_, value, value)
            ) %>%
            filter(!is.na(value)) %>%
            arrange(row_id, value) %>%
            group_by(row_id) %>%
            summarise(
               namesort_given_name  = first(value),
               namesort_family_name = last(value),
            ) %>%
            ungroup() %>%
            rename(
               !!id_col := row_id
            )

         dedup_new %<>%
            left_join(
               y  = dedup_new_names,
               by = id_col
            )

         return(dedup_new)
      }
   )
)

hs_download("harp_dx", "reg", 2024, 10)
dx <- read_dta(hs_data("harp_dx", "reg", 2024, 10)) %>%
   select(-first)
# new <- dx %>%
#    filter(year == 2024, month == 9) %>%
#    select(-first)
# old <- dx %>%
#    anti_join(new, join_by(idnum)) %>%
#    select(-first)


try <- Dedup$new()
try$setMaster(dx, "idnum")
# try$setUsing(old, "idnum")
try$preparePii()
# try$reclink()
try$splinkDedupe()

check <- try$review$splinkDedup %>%
   select(
      posterior = match_probability,
      idnum_l,
      idnum_r,
   ) %>%
   mutate(
      match_id = row_number()
   ) %>%
   left_join(
      y  = try$match$left %>%
         select(
            idnum,
            left_cid          = CENTRAL_ID,
            left_given_name   = given_name,
            left_middle_name  = middle_name,
            left_family_name  = family_name,
            left_suffix_name  = suffix_name,
            left_birthdate    = birthdate,
            left_confirmatory = confirmatory_code,
            left_uic          = uic,
            left_pxcode       = patient_code
         ),
      by = join_by(idnum_l == idnum)
   ) %>%
   left_join(
      y  = try$match$left %>%
         select(
            idnum,
            right_cid          = CENTRAL_ID,
            right_given_name   = given_name,
            right_middle_name  = middle_name,
            right_family_name  = family_name,
            right_suffix_name  = suffix_name,
            right_birthdate    = birthdate,
            right_confirmatory = confirmatory_code,
            right_uic          = uic,
            right_pxcode       = patient_code,
         ),
      by = join_by(idnum_r == idnum)
   ) %>%
   mutate(
      left_name  = stri_c(left_family_name, ", ", left_given_name, " ", left_middle_name, " ", left_suffix_name, ignore_null = TRUE),
      right_name = stri_c(right_family_name, ", ", right_given_name, " ", right_middle_name, " ", right_suffix_name, ignore_null = TRUE),
   ) %>%
   select(
      -ends_with("given_name"),
      -ends_with("middle_name"),
      -ends_with("family_name"),
      -ends_with("suffix_name"),
   ) %>%
   mutate(
      .before = 1,
      Bene    = NA_character_,
      Gab     = NA_character_,
      Lala    = NA_character_,
      Angie   = NA_character_,
   )

db           <- "nhsss_validations"
surv_name    <- "harp_dx"
process_step <- "dedup_old"
issue        <- "splink"
table        <- paste0(surv_name, "-", process_step, "-", issue)
schema       <- Id(schema = db, table = table)
id_col       <- "match_id"

lw_conn <- connect("ohasis-lw")
if (dbExistsTable(lw_conn, schema)) {
   dbRemoveTable(lw_conn, schema)
}
ohasis$upsert(lw_conn, db, table, check, id_col)
dbDisconnect(lw_conn)
