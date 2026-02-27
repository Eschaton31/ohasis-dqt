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
               central_id,
               all_of(self$left$id),
               all_of(private$requiredColumns)
            ) %>%
            private$auxColumns(self$left$id)

         if (nrow(self$right$data) > 0) {
            self$match$right <- self$right$data %>%
               rename_all(private$renameColumns) %>%
               select(
                  central_id,
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
                  left_pxcode       = patient_code,
                  left_region       = residence_region,
                  left_province     = residence_province,
                  left_muncity      = residence_muncity,
                  left_philhealth   = philhealth_no,
                  left_philsys      = philsys_id,
                  left_mobile       = client_mobile,
                  left_email        = client_email,
                  left_occupation   = occupation,
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
                        right_region       = residence_region,
                        right_province     = residence_province,
                        right_muncity      = residence_muncity,
                        right_philhealth   = philhealth_no,
                        right_philsys      = philsys_id,
                        right_mobile       = client_mobile,
                        right_email        = client_email,
                        right_occupation   = occupation,
                     ),
                  by = join_by(match_id)
               ) %>%
               mutate_at(
                  .vars = vars(ends_with("_name")),
                  ~coalesce(., "")
               ) %>%
               mutate(
                  left_name  = stri_c(
                     coalesce(left_family_name, ''),
                     ", ",
                     coalesce(left_given_name, ''),
                     " ",
                     coalesce(left_middle_name, ''),
                     " ",
                     coalesce(left_suffix_name, '')
                  ),
                  right_name = stri_c(
                     coalesce(right_family_name, ''),
                     ", ",
                     coalesce(right_given_name, ''),
                     " ",
                     coalesce(right_middle_name, ''),
                     " ",
                     coalesce(right_suffix_name, '')
                  ),
               ) %>%
               mutate_at(
                  .vars = vars(left_name, right_name),
                  ~na_if(str_squish(.), ",")
               ) %>%
               # select(
               #    -ends_with("given_name"),
               #    -ends_with("middle_name"),
               #    -ends_with("family_name"),
               #    -ends_with("suffix_name"),
               # ) %>%
               arrange(desc(posterior)) %>%
               # Additional sift through of matches
               mutate(
                  # levenshtein
                  name_levenshtein = stringsim(
                     left_name,
                     right_name,
                     method = 'lv'
                  ),
                  # jaro-winkler
                  name_jarowinkler = stringsim(
                     left_name,
                     right_name,
                     method = 'jw'
                  ),
                  # qgram
                  name_qgram       = stringsim(
                     left_name,
                     right_name,
                     method = 'qgram',
                     q      = 3
                  ),
                  avg_dist         = (name_levenshtein +
                     name_jarowinkler +
                     name_qgram) /
                     3,
               ) %>%
               # choose 60% and above match
               filter(avg_dist >= 0.60, !is.na(posterior)) %>%
               select(-name_levenshtein, -name_jarowinkler, -name_qgram, -avg_dist)

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

      exact        = function() {
         dedup_old <- list()
         group_pii <- list(
            "uic.base"           = "uic",
            "uic.fixed"          = "uic_sort",
            "philhealth.fixed"   = "philhealth_no_sieve",
            "philsys.fixed"      = "philsys_id_sieve",
            "pxuic.base"         = c("patient_code", "uic"),
            "pxuic.fixed"        = c("patient_code_sieve", "uic_sort"),
            "firstuic.base"      = c("given_name_sieve", "uic_sort"),
            "firstuic.fixed"     = c("given_name_metaphone", "uic_sort"),
            "firstuic.partial"   = c("given_name_3", "uic_sort"),
            "firstuic.sort"      = c("namesort_given_name", "uic_sort"),
            "firstbd.base"       = c("given_name_sieve", "birthdate"),
            "firstbd.fixed"      = c("given_name_metaphone", "birthdate"),
            "firstbd.sort"       = c("namesort_given_name", "birthdate"),
            "pxbd.base"          = c("patient_code", "birthdate"),
            "pxbd.fixed"         = c("patient_code_sieve", "birthdate"),
            "email"              = "client_email",
            "mobile"             = "client_mobile",
            "email.mobile"       = c("client_email", "client_mobile"),
            "name.base"          = c("given_name_sieve", "family_name_sieve", "birthdate"),
            "name.fixed"         = c("given_name_metaphone", "family_name_metaphone", "birthdate"),
            "name.partial"       = c("given_name_3", "family_name_3", "birthdate"),
            "name.sort"          = c("namesort_given_name", "namesort_family_name", "birthdate"),
            "ym.bd-name.base"    = c("given_name_sieve", "family_name_sieve", "birth_yr", "birth_mo"),
            "yd.bd-name.base"    = c("given_name_sieve", "family_name_sieve", "birth_yr", "birth_dy"),
            "md.bd-name.base"    = c("given_name_sieve", "family_name_sieve", "birth_mo", "birth_dy"),
            "ym.bd-name.fixed"   = c("given_name_metaphone", "family_name_metaphone", "birth_yr", "birth_mo"),
            "yd.bd-name.fixed"   = c("given_name_metaphone", "family_name_metaphone", "birth_yr", "birth_dy"),
            "md.bd-name.fixed"   = c("given_name_metaphone", "family_name_metaphone", "birth_mo", "birth_dy"),
            "ym.bd-name.partial" = c("given_name_3", "family_name_3", "birth_yr", "birth_mo"),
            "yd.bd-name.partial" = c("given_name_3", "family_name_3", "birth_yr", "birth_dy"),
            "md.bd-name.partial" = c("given_name_3", "family_name_3", "birth_mo", "birth_dy"),
            "ym.bd-name.sort"    = c("namesort_given_name", "namesort_family_name", "birth_yr", "birth_mo"),
            "yd.bd-name.sort"    = c("namesort_given_name", "namesort_family_name", "birth_yr", "birth_dy"),
            "md.bd-name.sort"    = c("namesort_given_name", "namesort_family_name", "birth_mo", "birth_dy")
         )
         for (i in seq_len(length(group_pii))) {
            dedup_name <- names(group_pii)[[i]]
            dedup_id   <- group_pii[[i]]

            # tag duplicates based on grouping
            df <- self$match$left %>%
               select(
                  any_of(c(
                     self$left$id,
                     dedup_id
                  ))
               ) %>%
               filter(if_all(any_of(dedup_id), ~!is.na(.))) %>%
               get_dupes(all_of(dedup_id)) %>%
               filter(dupe_count > 0) %>%
               group_by(across(all_of(dedup_id))) %>%
               mutate(
                  grp_id = str_c(collapse = ",", sort(!!as.name(self$left$id))),
               ) %>%
               ungroup() %>%
               arrange(grp_id, !!as.name(self$left$id), across(all_of(dedup_id)))

            # if any found, include in list for review
            dedup_old[[dedup_name]] <- df
         }
         all_dedup <- bind_rows(dedup_old, .id = 'var') %>%
            distinct(grp_id, var) %>%
            mutate(matched = 1, var = str_c('issue_', var)) %>%
            pivot_wider(
               id_cols     = grp_id,
               names_from  = var,
               values_from = matched,
            )

         adjust_score <- list(
            `issue_uic.base`           = 3,
            `issue_uic.fixed`          = 3,
            `issue_philhealth.fixed`   = 1,
            `issue_philsys.fixed`      = 1,
            `issue_confirmcode.base`   = 3,
            `issue_confirmcode.fixed`  = 3,
            `issue_pxcode.base`        = 1,
            `issue_pxcode.fixed`       = 1,
            `issue_pxconfirm.base`     = 3,
            `issue_pxconfirm.fixed`    = 3,
            `issue_confirmuic.base`    = 4,
            `issue_confirmuic.fixed`   = 4,
            `issue_pxuic.base`         = 3,
            `issue_pxuic.fixed`        = 3,
            `issue_firstuic.base`      = 3,
            `issue_firstuic.fixed`     = 3,
            `issue_firstuic.partial`   = 1,
            `issue_firstuic.sort`      = 3,
            `issue_pxbd.base`          = 1,
            `issue_pxbd.fixed`         = 1,
            `issue_name.base`          = 4,
            `issue_name.fixed`         = 4,
            `issue_name.partial`       = 1,
            `issue_name.sort`          = 3,
            `issue_ym.bd-name.base`    = 3,
            `issue_yd.bd-name.base`    = 3,
            `issue_md.bd-name.base`    = 3,
            `issue_ym.bd-name.fixed`   = 2,
            `issue_yd.bd-name.fixed`   = 2,
            `issue_md.bd-name.fixed`   = 2,
            `issue_ym.bd-name.partial` = 1,
            `issue_yd.bd-name.partial` = 1,
            `issue_md.bd-name.partial` = 1,
            `issue_ym.bd-name.sort`    = 2,
            `issue_yd.bd-name.sort`    = 2,
            `issue_md.bd-name.sort`    = 2
         )
         adjust_only  <- intersect(names(adjust_score), names(all_dedup))
         for (var in adjust_only) {
            all_dedup %<>%
               mutate_at(
                  .vars = vars(matches(var)),
                  ~if_else(. == 1, adjust_score[[var]], 0, 0)
               )
         }

         all_dedup %<>%
            mutate(posterior = rowMeans(select(., starts_with("issue")), na.rm = TRUE)) %>%
            select(posterior, grp_id) %>%
            arrange(desc(posterior), grp_id) %>%
            mutate(ids = grp_id) %>%
            separate_longer_delim(
               ids,
               ",",
            ) %>%
            group_by(grp_id) %>%
            mutate(
               num = row_number()
            ) %>%
            ungroup() %>%
            pivot_wider(
               id_cols      = c(posterior, grp_id),
               names_from   = num,
               names_prefix = "id_",
               values_from  = ids
            )

         n_copies <- names(all_dedup)[length(names(all_dedup))]
         n_copies <- str_split(n_copies, "_", simplify = TRUE)[[2]]

         exact_review <- tibble()
         for (i in 2:n_copies) {
            var          <- as.name(str_c('id_', i))
            exact_review <- bind_rows(
               exact_review,
               all_dedup %>%
                  filter(!is.na(!!var)) %>%
                  select(
                     posterior,
                     left_id  = id_1,
                     right_id = !!var
                  )
            )
         }

         exact_review %<>%
            mutate(
               match_id = row_number(),
               left_id  = as.integer(left_id),
               right_id = as.integer(right_id),
            ) %>%
            left_join(
               y  = self$match$left %>%
                  select(
                     left_id           = self$left$id,
                     left_cid          = central_id,
                     left_given_name   = given_name,
                     left_middle_name  = middle_name,
                     left_family_name  = family_name,
                     left_suffix_name  = suffix_name,
                     left_birthdate    = birthdate,
                     left_confirmatory = confirmatory_code,
                     left_uic          = uic,
                     left_pxcode       = patient_code,
                     left_region       = residence_region,
                     left_province     = residence_province,
                     left_muncity      = residence_muncity,
                     left_philhealth   = philhealth_no,
                     left_philsys      = philsys_id,
                     left_mobile       = client_mobile,
                     left_email        = client_email,
                     left_occupation   = occupation,
                  ),
               by = join_by(left_id)
            ) %>%
            left_join(
               y  = self$match$left %>%
                  select(
                     right_id           = self$left$id,
                     right_cid          = central_id,
                     right_given_name   = given_name,
                     right_middle_name  = middle_name,
                     right_family_name  = family_name,
                     right_suffix_name  = suffix_name,
                     right_birthdate    = birthdate,
                     right_confirmatory = confirmatory_code,
                     right_uic          = uic,
                     right_pxcode       = patient_code,
                     right_region       = residence_region,
                     right_province     = residence_province,
                     right_muncity      = residence_muncity,
                     right_philhealth   = philhealth_no,
                     right_philsys      = philsys_id,
                     right_mobile       = client_mobile,
                     right_email        = client_email,
                     right_occupation   = occupation,
                  ),
               by = join_by(right_id)
            ) %>%
            mutate_at(
               .vars = vars(ends_with("_name")),
               ~coalesce(., "")
            ) %>%
            mutate(
               left_name  = stri_c(
                  coalesce(left_family_name, ''),
                  ", ",
                  coalesce(left_given_name, ''),
                  " ",
                  coalesce(left_middle_name, ''),
                  " ",
                  coalesce(left_suffix_name, '')
               ),
               right_name = stri_c(
                  coalesce(right_family_name, ''),
                  ", ",
                  coalesce(right_given_name, ''),
                  " ",
                  coalesce(right_middle_name, ''),
                  " ",
                  coalesce(right_suffix_name, '')
               ),
            ) %>%
            mutate_at(
               .vars = vars(left_name, right_name),
               ~na_if(str_squish(.), ",")
            ) %>%
            # select(
            #    -ends_with("given_name"),
            #    -ends_with("middle_name"),
            #    -ends_with("family_name"),
            #    -ends_with("suffix_name"),
            # ) %>%
            arrange(desc(posterior)) %>%
            # Additional sift through of matches
            mutate(
               # levenshtein
               name_levenshtein = stringsim(
                  left_name,
                  right_name,
                  method = 'lv'
               ),
               # jaro-winkler
               name_jarowinkler = stringsim(
                  left_name,
                  right_name,
                  method = 'jw'
               ),
               # qgram
               name_qgram       = stringsim(
                  left_name,
                  right_name,
                  method = 'qgram',
                  q      = 3
               ),
               avg_dist         = (name_levenshtein +
                  name_jarowinkler +
                  name_qgram) /
                  3,
            ) %>%
            # choose 60% and above match
            filter(avg_dist >= 0.60, !is.na(posterior)) %>%
            select(-name_levenshtein, -name_jarowinkler, -name_qgram, -avg_dist)

         # assign to global env
         self$review$exact <- exact_review %>%
            mutate(
               Bene  = NA_character_,
               Gab   = NA_character_,
               Lala  = NA_character_,
               Angie = NA_character_,
            ) %>%
            select(-left_id, -right_id)

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
         cll <- import(
            "splink.comparison_level_library",
            as      = "cll",
            convert = FALSE
         )

         log_info("Use DuckDB.")
         db_api <- sp$DuckDBAPI()

         uic_comparison       <- cl$CustomComparison(
            output_column_name     = "uic",
            comparison_description = "UIC",
            comparison_levels      = c(
               cll$NullLevel("uic"),
               cll$ExactMatchLevel("uic"),
               cll$CustomLevel(
                  "concat(uic_mom_l, uic_order_l, birthdate_l) = concat(uic_dad_r, uic_order_r, birthdate_r)"
               ),
               cll$CustomLevel(
                  "concat(uic_dad_l, uic_order_l, birthdate_l) = concat(uic_mom_r, uic_order_r, birthdate_r)"
               ),
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
               sp$block_on("birth_yr", "birth_mo", "given_name_metaphone"),
               sp$block_on("birth_yr", "birth_dy", "given_name_metaphone"),
               sp$block_on("birth_mo", "birth_dy", "given_name_metaphone"),
               sp$block_on("residence_province", "given_name_metaphone"),
               sp$block_on("residence_province", "family_name_metaphone")
            ),
            comparisons                            = c(
               cl$NameComparison("given_name_sieve"),
               cl$NameComparison("middle_name_sieve"),
               cl$NameComparison("family_name_sieve"),
               # cl$NameComparison("given_name_metaphone"),
               # cl$NameComparison("family_name_metaphone"),
               cl$DateOfBirthComparison(
                  "birthdate",
                  input_is_string     = TRUE,
                  datetime_metrics    = c("year", "month", "day"),
                  datetime_thresholds = c(1, 1, 10),
               ),
               # uic_comparison,
               # full_name_comparison,
               # last_name_comparison,
               cl$ExactMatch("residence_region"),
               cl$ExactMatch("residence_province")
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
            estimate_probability_two_random_records_match(
            sp$block_on("given_name_sieve", "family_name_sieve"),
            recall = 0.7
         )
         linker$training$estimate_u_using_random_sampling(max_pairs = 1e6)

         # log_info("EM Algorithm = {green('First Name')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on(
         #    "given_name_sieve"
         # ))

         # log_info("EM Algorithm = {green('First Name (3)')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on(
         #    "given_name_3"
         # ))

         # log_info("EM Algorithm = {green('Last Name')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on(
         #    "family_name_sieve"
         # ))

         # log_info("EM Algorithm = {green('First Name(3)+Last Name')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on("given_name_3", "family_name_sieve"))
         #
         # log_info("EM Algorithm = {green('Birth Date')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on("birthdate"))

         log_info("EM Algorithm = {green('First Name+Birth Date')}.")
         linker$
            training$
            estimate_parameters_using_expectation_maximisation(sp$block_on("given_name_3", "birthdate"))
         log_info("EM Algorithm = {green('Last Name+Birth Date')}.")
         linker$
            training$
            estimate_parameters_using_expectation_maximisation(sp$block_on("family_name_sieve", "birthdate"))

         # log_info("EM Algorithm = {green('First Name+Birth Yr+Birth Mo')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on(
         #    "given_name_sieve",
         #    "birth_yr",
         #    "birth_mo"
         # ))
         # log_info("EM Algorithm = {green('First Name+Birth Yr+Birth Dy')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on(
         #    "given_name_sieve",
         #    "birth_yr",
         #    "birth_dy"
         # ))
         # log_info("EM Algorithm = {green('First Name+Birth Mo+Birth Dy')}.")
         # linker$
         #    training$
         #    estimate_parameters_using_expectation_maximisation(sp$block_on(
         #    "given_name_sieve",
         #    "birth_mo",
         #    "birth_dy"
         # ))

         log_info("Generating match pairs.")
         pairwise_predictions <- linker$inference$predict(
            threshold_match_weight = -10
         )

         log_info("Finalizing estimation object.")
         estimates <- pairwise_predictions$as_pandas_dataframe()

         log_info("Collecting estimation data.")
         matches <- bind_rows(py_to_r(estimates))

         log_info("Done!")
         id       <- as.name(self$left$id)
         left_by  <- join_by(left_id == !!id)
         right_by <- join_by(right_id == !!id)

         self$review$splinkDedup <- matches %>%
            rename_all(
               ~case_when(
                  . == "idnum_l" ~ "left_id",
                  . == "idnum_r" ~ "right_id",
                  . == "art_id_l" ~ "left_id",
                  . == "art_id_r" ~ "right_id",
                  . == "mort_id_l" ~ "left_id",
                  . == "mort_id_r" ~ "right_id",
                  . == "prep_id_l" ~ "left_id",
                  . == "prep_id_r" ~ "right_id",
                  . == "row_id_l" ~ "left_id",
                  . == "row_id_r" ~ "right_id",
                  . == "id_l" ~ "left_id",
                  . == "id_r" ~ "right_id",
                  TRUE ~ .
               )
            ) %>%
            select(
               posterior = match_probability,
               left_id,
               right_id,
            ) %>%
            arrange(desc(posterior)) %>%
            mutate(
               match_id = row_number()
            ) %>%
            left_join(
               y  = self$match$left %>%
                  select(
                     !!id,
                     left_cid          = central_id,
                     left_given_name   = given_name,
                     left_middle_name  = middle_name,
                     left_family_name  = family_name,
                     left_suffix_name  = suffix_name,
                     left_birthdate    = birthdate,
                     left_confirmatory = confirmatory_code,
                     left_uic          = uic,
                     left_pxcode       = patient_code,
                     left_region       = residence_region,
                     left_province     = residence_province,
                     left_muncity      = residence_muncity,
                     left_philhealth   = philhealth_no,
                     left_philsys      = philsys_id,
                     left_mobile       = client_mobile,
                     left_email        = client_email,
                     left_occupation   = occupation,
                  ),
               by = left_by
            ) %>%
            left_join(
               y  = self$match$left %>%
                  select(
                     !!id,
                     right_cid          = central_id,
                     right_given_name   = given_name,
                     right_middle_name  = middle_name,
                     right_family_name  = family_name,
                     right_suffix_name  = suffix_name,
                     right_birthdate    = birthdate,
                     right_confirmatory = confirmatory_code,
                     right_uic          = uic,
                     right_pxcode       = patient_code,
                     right_region       = residence_region,
                     right_province     = residence_province,
                     right_muncity      = residence_muncity,
                     right_philhealth   = philhealth_no,
                     right_philsys      = philsys_id,
                     right_mobile       = client_mobile,
                     right_email        = client_email,
                     right_occupation   = occupation,
                  ),
               by = right_by
            ) %>%
            mutate_at(
               .vars = vars(ends_with("_name")),
               ~coalesce(., "")
            ) %>%
            mutate(
               left_name  = stri_c(
                  coalesce(left_family_name, ''),
                  ", ",
                  coalesce(left_given_name, ''),
                  " ",
                  coalesce(left_middle_name, ''),
                  " ",
                  coalesce(left_suffix_name, '')
               ),
               right_name = stri_c(
                  coalesce(right_family_name, ''),
                  ", ",
                  coalesce(right_given_name, ''),
                  " ",
                  coalesce(right_middle_name, ''),
                  " ",
                  coalesce(right_suffix_name, '')
               ),
            ) %>%
            mutate_at(
               .vars = vars(left_name, right_name),
               ~na_if(str_squish(.), ",")
            ) %>%
            # select(
            #    -ends_with("given_name"),
            #    -ends_with("middle_name"),
            #    -ends_with("family_name"),
            #    -ends_with("suffix_name"),
            # ) %>%
            mutate(
               .before = 1,
               Bene    = NA_character_,
               Gab     = NA_character_,
               Lala    = NA_character_,
               Angie   = NA_character_,
            )

         conn     <- ohasis$conn("lw")
         nonDupes <- QB$new(conn)$from("ohasis_warehouse.non_dupes")$select(
            "patient_id AS left_cid",
            "non_pair_id AS right_cid"
         )$get()
         dbDisconnect(conn)

         self$review$splinkDedup %<>%
            anti_join(nonDupes)

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
         "residence_muncity",
         "occupation"
      ),

      renameColumns   = function(name) {
         lower_name <- tolower(name)
         new        <- case_when(
            lower_name == "labcode2" ~ "confirmatory_code",
            lower_name == "confirmatory_code" ~ "confirmatory_code",
            lower_name == "saccl_lab_code" ~ "confirmatory_code",
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
            lower_name == "sname" ~ "suffix_name",
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
            lower_name == "client_email" ~ "client_email",
            lower_name == "permcurr_reg" ~ "residence_region",
            lower_name == "permcurr_prov" ~ "residence_province",
            lower_name == "permcurr_munc" ~ "residence_muncity",
            lower_name == "curr_reg" ~ "residence_region",
            lower_name == "curr_prov" ~ "residence_province",
            lower_name == "curr_munc" ~ "residence_muncity",
            lower_name == "permcurr_reg" ~ "residence_region",
            lower_name == "permcurr_prov" ~ "residence_province",
            lower_name == "permcurr_munc" ~ "residence_muncity",
            lower_name == "region" ~ "residence_region",
            lower_name == "province" ~ "residence_province",
            lower_name == "muncity" ~ "residence_muncity",
            lower_name == "final_region" ~ "residence_region",
            lower_name == "final_province" ~ "residence_province",
            lower_name == "final_muncity" ~ "residence_muncity",
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
                     new_col = if (col == "birthdate") {
                        new_col = NA_Date_
                     } else {
                        NA_character_
                     },
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
               full_name               = stri_c(
                  given_name,
                  " ",
                  family_name,
                  ignore_null = TRUE
               ),
               last_name               = stri_c(
                  middle_name,
                  " ",
                  family_name,
                  ignore_null = TRUE
               ),

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
               .vars = vars(
                  given_name_sieve,
                  middle_name_sieve,
                  family_name_sieve
               ),
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
            transmute(
               row_id = .data[[id_col]],
               uic_mom,
               uic_dad
            ) %>%
            pivot_longer(
               cols           = c(uic_mom, uic_dad),
               names_to       = 'uic',
               values_to      = 'given_name_two',
               values_drop_na = TRUE
            ) %>%
            arrange(row_id, given_name_two) %>%
            mutate(uic = row_number(), .by = row_id) %>%
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
            transmute(
               !!id_col := .data[[id_col]],
               name_1   = given_name_sieve,
               name_2   = middle_name_sieve,
               name_3   = family_name_sieve
            ) %>%
            filter(!(is.na(name_1) & is.na(name_2) & is.na(name_3))) %>%
            pivot_longer(
               cols           = c(name_1, name_2, name_3),
               names_to       = "name",
               values_to      = "value",
               values_drop_na = TRUE
            ) %>%
            filter(nchar(value) > 1) %>%
            arrange(.data[[id_col]], value) %>%
            summarise(
               namesort_given_name  = first(value),
               namesort_family_name = last(value), .by = !!sym(id_col)
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

upload_splink <- function(data, surv_name, dedup_type) {
   issue  <- "splink"
   table  <- paste0(dedup_type, "-", issue)
   id_col <- "match_id"

   lw_conn <- connect(surv_name)
   dbExecute(lw_conn, glue(r"(TRUNCATE `{surv_name}`.`{table}`)"))
   ohasis$upsert(lw_conn, surv_name, table, data, id_col)
   dbDisconnect(lw_conn)
}

upload_exact <- function(data, surv_name, dedup_type) {
   issue  <- "exact"
   table  <- paste0(dedup_type, "-", issue)
   id_col <- "match_id"

   lw_conn <- connect(surv_name)
   dbExecute(lw_conn, glue(r"(TRUNCATE `{surv_name}`.`{table}`)"))
   ohasis$upsert(lw_conn, surv_name, table, data, id_col)
   dbDisconnect(lw_conn)
}

upload_reclink <- function(data, surv_name, dedup_type) {
   issue  <- "reclink"
   table  <- paste0(dedup_type, "-", issue)
   id_col <- "match_id"

   lw_conn <- connect(surv_name)
   dbExecute(lw_conn, glue(r"(TRUNCATE `{surv_name}`.`{table}`)"))
   ohasis$upsert(lw_conn, surv_name, table, data, id_col)
   dbDisconnect(lw_conn)
}

generate_splink <- function(yr, mo, surv_name) {
   table <- str_c('reg_', yr, stri_pad_left(mo, 2, '0'))

   idreg <- update_idreg()

   conn <- connect('mariadb-lw')
   data <- switch(
      surv_name,
      harp_dx   = QB$new(conn)$select(
         patient_id,
         idnum,
         firstname,
         middle,
         last,
         name_suffix,
         bdate,
         sex,
         uic,
         labcode2,
         patient_code,
         philhealth,
         philsys_id,
         mobile,
         email,
         region,
         province,
         muncity,
         curr_work,
         prev_work,
         job
      )$from(stri_c(surv_name, ".", table))$get(),
      harp_tx   = QB$new(conn)$select(
         patient_id,
         art_id,
         idnum,
         first,
         middle,
         last,
         suffix,
         birthdate,
         sex,
         uic,
         confirmatory_code,
         px_code,
         philhealth_no,
         philsys_id,
         mobile,
         email
      )$from(stri_c(surv_name, ".", table))$get(),
      harp_dead = QB$new(conn)$select(
         patient_id,
         mort_id,
         idnum,
         fname,
         mname,
         lname,
         sname,
         birthdate,
         sex,
         uic,
         saccl_lab_code,
         patient_code,
         philhealth,
         philsys_id,
         mobile,
         email
      )$from(stri_c(surv_name, ".", table))$get(),
      prep      = QB$new(conn)$select(
         patient_id,
         prep_id,
         first,
         middle,
         last,
         suffix,
         birthdate,
         sex,
         uic,
         px_code,
         philhealth_no,
         philsys_id,
         mobile,
         email,
         curr_reg,
         curr_prov,
         curr_munc
      )$from(stri_c(surv_name, ".", table))$get(),
   )

   if (surv_name %in% c("harp_tx", "harp_dead")) {
      dx <- QB$new(conn)$select(
         idnum,
         region,
         province,
         muncity,
         job,
         curr_work,
         prev_work
      )$from(stri_c("harp_dx.", table))$get()
      data %<>%
         select(-any_of(c("curr_reg", "curr_prov", "curr_munc"))) %>%
         left_join(
            y  = dx,
            by = join_by(idnum)
         )
   }
   dbDisconnect(conn)

   if (surv_name == "prep") {
      data %<>%
         mutate(
            job               = NA_character_,
            curr_work         = NA_character_,
            prev_work         = NA_character_,
            confirmatory_code = NA_character_
         )
   }

   data %<>%
      mutate_at(vars(job, curr_work, prev_work, job), ~na_if(., "")) %>%
      mutate(
         occupation = coalesce(curr_work, prev_work, job)
      ) %>%
      rename_all(tolower) %>%
      get_cid(idreg, patient_id)

   id <- switch(
      surv_name,
      harp_dx   = "idnum",
      harp_tx   = "art_id",
      harp_dead = "mort_id",
      prep      = "prep_id",
   )

   dedup <- Dedup$new()
   dedup$setMaster(data, id)
   dedup$preparePii()
   dedup$splinkDedupe()

   return(dedup)
}

generate_exact <- function(yr, mo, surv_name) {
   table <- str_c('reg_', yr, stri_pad_left(mo, 2, '0'))

   idreg <- update_idreg()

   conn <- connect('mariadb-lw')
   data <- switch(
      surv_name,
      harp_dx   = QB$new(conn)$select(
         patient_id,
         idnum,
         firstname,
         middle,
         last,
         name_suffix,
         bdate,
         sex,
         uic,
         labcode2,
         patient_code,
         philhealth,
         philsys_id,
         mobile,
         email,
         region,
         province,
         muncity,
         curr_work,
         prev_work,
         job
      )$from(stri_c(surv_name, ".", table))$get(),
      harp_tx   = QB$new(conn)$select(
         patient_id,
         art_id,
         idnum,
         first,
         middle,
         last,
         suffix,
         birthdate,
         sex,
         uic,
         confirmatory_code,
         px_code,
         philhealth_no,
         philsys_id,
         mobile,
         email
      )$from(stri_c(surv_name, ".", table))$get(),
      harp_dead = QB$new(conn)$select(
         patient_id,
         mort_id,
         idnum,
         fname,
         mname,
         lname,
         sname,
         birthdate,
         sex,
         uic,
         saccl_lab_code,
         patient_code,
         philhealth,
         philsys_id,
         mobile,
         email
      )$from(stri_c(surv_name, ".", table))$get(),
      prep      = QB$new(conn)$select(
         patient_id,
         prep_id,
         first,
         middle,
         last,
         suffix,
         birthdate,
         sex,
         uic,
         px_code,
         philhealth_no,
         philsys_id,
         mobile,
         email,
         curr_reg,
         curr_prov,
         curr_munc
      )$from(stri_c(surv_name, ".", table))$get(),
   )

   if (surv_name %in% c("harp_tx", "harp_dead")) {
      dx <- QB$new(conn)$select(
         idnum,
         region,
         province,
         muncity,
         job,
         curr_work,
         prev_work
      )$from(stri_c("harp_dx.", table))$get()
      data %<>%
         select(-any_of(c("curr_reg", "curr_prov", "curr_munc"))) %>%
         left_join(
            y  = dx,
            by = join_by(idnum)
         )
   }
   dbDisconnect(conn)

   if (surv_name == "prep") {
      data %<>%
         mutate(
            job               = NA_character_,
            curr_work         = NA_character_,
            prev_work         = NA_character_,
            confirmatory_code = NA_character_
         )
   }

   data %<>%
      mutate_at(vars(job, curr_work, prev_work, job), ~na_if(., "")) %>%
      mutate(
         occupation = coalesce(curr_work, prev_work, job)
      ) %>%
      rename_all(tolower) %>%
      get_cid(idreg, patient_id)

   id <- switch(
      surv_name,
      harp_dx   = "idnum",
      harp_tx   = "art_id",
      harp_dead = "mort_id",
      prep      = "prep_id",
   )

   dedup <- Dedup$new()
   dedup$setMaster(data, id)
   dedup$preparePii()
   dedup$exact()

   return(dedup)
}

reclink_to_dx <- function(yr, mo, surv_name) {
   table <- str_c('reg_', yr, stri_pad_left(mo, 2, '0'))
   log_info(table)

   idreg <- update_idreg()

   conn <- connect('mariadb-lw')
   data <- switch(
      surv_name,
      harp_tx   = QB$new(conn)$select(
         patient_id,
         art_id,
         idnum,
         first,
         middle,
         last,
         suffix,
         birthdate,
         sex,
         uic,
         confirmatory_code,
         px_code,
         philhealth_no,
         philsys_id,
         mobile,
         email
      )$from(stri_c(surv_name, ".", table))$get(),
      harp_dead = QB$new(conn)$select(
         patient_id,
         mort_id,
         idnum,
         fname,
         mname,
         lname,
         sname,
         birthdate,
         sex,
         uic,
         saccl_lab_code,
         patient_code,
         philhealth,
         philsys_id,
         mobile,
         email
      )$from(stri_c(surv_name, ".", table))$get()
   )

   dx <- QB$new(conn)$select(
      patient_id,
      idnum,
      firstname,
      middle,
      last,
      name_suffix,
      bdate,
      sex,
      uic,
      labcode2,
      patient_code,
      philhealth,
      philsys_id,
      mobile,
      email,
      region,
      province,
      muncity,
      curr_work,
      prev_work,
      job
   )$from(stri_c("harp_dx.", table))$get()
   data %<>%
      select(-any_of(c("curr_reg", "curr_prov", "curr_munc"))) %>%
      left_join(
         y  = dx %>%
            select(
               idnum,
               region,
               province,
               muncity,
               job,
               curr_work,
               prev_work
            ),
         by = join_by(idnum)
      )
   dbDisconnect(conn)

   if (surv_name == "prep") {
      data %<>%
         mutate(
            job               = NA_character_,
            curr_work         = NA_character_,
            prev_work         = NA_character_,
            confirmatory_code = NA_character_
         )
   }

   data %<>%
      mutate_at(vars(job, curr_work, prev_work, job), ~na_if(., "")) %>%
      mutate(
         occupation = coalesce(curr_work, prev_work, job)
      ) %>%
      rename_all(tolower) %>%
      get_cid(idreg, patient_id)

   dx %<>%
      mutate_at(vars(job, curr_work, prev_work, job), ~na_if(., "")) %>%
      mutate(
         occupation = coalesce(curr_work, prev_work, job)
      ) %>%
      rename_all(tolower) %>%
      get_cid(idreg, patient_id)

   id <- switch(
      surv_name,
      harp_tx   = "art_id",
      harp_dead = "mort_id",
      prep      = "prep_id",
   )

   dedup <- Dedup$new()
   dedup$setMaster(data %>% filter(is.na(idnum)), id)
   dedup$setUsing(dx, 'idnum')
   dedup$preparePii()
   dedup$reclink()

   return(dedup)
}

## sample run for surveillance
# surv <- "harp_dx"
# data <- generate_splink(2025, 7, surv)
# upload_splink(data, surv, "dedup_old")
