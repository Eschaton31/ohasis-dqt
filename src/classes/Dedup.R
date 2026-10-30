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
         self$left$data <- distinct(data, !!as.name(id), .keep_all = TRUE)
         self$left$id   <- id
      },

      setUsing     = function(data, id) {
         self$right$data <- distinct(data, !!as.name(id), .keep_all = TRUE)
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

      reclink      = function(n_cores = 6) {
         reclink_df <- fastlink_wsl(
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
            n.cores          = n_cores
         )

         if (length(reclink_df$matches$inds.a) > 0) {
            # row positions let the matched rows be joined back to their details
            left_rows       <- mutate(self$match$left, .row = row_number())
            right_rows      <- mutate(self$match$right, .row = row_number())
            reclink_matched <- getMatches(
               dfA         = left_rows,
               dfB         = right_rows,
               fl.out      = reclink_df,
               combine.dfs = FALSE
            )

            reclink_review <- tibble(
               posterior = reclink_matched$dfA.match$posterior,
               left_id   = reclink_matched$dfA.match$.row,
               right_id  = reclink_matched$dfB.match$.row,
            ) %>%
               mutate(
                  match_id = row_number()
               ) %>%
               private$enrichPairs(left_rows, ".row", right_rows, ".row", min_name_sim = 0.60) %>%
               select(-left_id, -right_id)

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
         id_col    <- self$left$id
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
         # weight of each criterion; every criterion in group_pii must have one
         score_pii <- c(
            "uic.base"           = 3,
            "uic.fixed"          = 3,
            "philhealth.fixed"   = 1,
            "philsys.fixed"      = 1,
            "pxuic.base"         = 3,
            "pxuic.fixed"        = 3,
            "firstuic.base"      = 3,
            "firstuic.fixed"     = 3,
            "firstuic.partial"   = 1,
            "firstuic.sort"      = 3,
            "firstbd.base"       = 1,
            "firstbd.fixed"      = 1,
            "firstbd.sort"       = 1,
            "pxbd.base"          = 1,
            "pxbd.fixed"         = 1,
            "email"              = 1,
            "mobile"             = 1,
            "email.mobile"       = 1,
            "name.base"          = 4,
            "name.fixed"         = 4,
            "name.partial"       = 1,
            "name.sort"          = 3,
            "ym.bd-name.base"    = 3,
            "yd.bd-name.base"    = 3,
            "md.bd-name.base"    = 3,
            "ym.bd-name.fixed"   = 2,
            "yd.bd-name.fixed"   = 2,
            "md.bd-name.fixed"   = 2,
            "ym.bd-name.partial" = 1,
            "yd.bd-name.partial" = 1,
            "md.bd-name.partial" = 1,
            "ym.bd-name.sort"    = 2,
            "yd.bd-name.sort"    = 2,
            "md.bd-name.sort"    = 2
         )
         stopifnot(setequal(names(group_pii), names(score_pii)))

         # per criterion, pair every record in a duplicate group with the
         # group's lowest id; singletons drop out since their id == left_id
         pairs <- lapply(names(group_pii), function(criterion) {
            keys <- group_pii[[criterion]]
            self$match$left %>%
               select(right_id = all_of(id_col), all_of(keys)) %>%
               filter(if_all(all_of(keys), ~!is.na(.))) %>%
               mutate(left_id = min(right_id), .by = all_of(keys)) %>%
               filter(right_id != left_id) %>%
               distinct(left_id, right_id) %>%
               mutate(score = score_pii[[criterion]])
         })

         # score each pair once across all criteria, over a fixed denominator
         exact_review <- bind_rows(pairs) %>%
            summarise(
               posterior = sum(score) / length(group_pii),
               .by       = c(left_id, right_id)
            ) %>%
            arrange(desc(posterior), left_id, right_id) %>%
            mutate(
               match_id = row_number()
            ) %>%
            private$enrichPairs(self$match$left, id_col, self$match$left, id_col, min_name_sim = 0.60)

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
         splink <- private$loadSplink()
         sp     <- splink$sp
         cl     <- splink$cl
         cll    <- splink$cll

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
            blocking_rules_to_generate_predictions = private$splinkBlocking(sp),
            comparisons                            = private$splinkComparisons(cl)
            # uic_comparison, full_name_comparison and last_name_comparison
            # can be appended to the comparisons above when needed
         )

         log_info("Cleaning data.")
         df <- private$splinkFrame(self$match$left, self$left$id) %>%
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

         log_info("Collecting estimation data.")
         # pull only the pair ids and probability out of DuckDB
         id      <- self$left$id
         matches <- linker$misc$query_sql(
            stri_c(
               "SELECT match_probability AS posterior, ",
               id, "_l AS left_id, ",
               id, "_r AS right_id ",
               "FROM ", py_to_r(pairwise_predictions$physical_name)
            ),
            output_type = "pandas"
         ) %>%
            py_to_r() %>%
            as_tibble()

         log_info("Done!")
         self$review$splinkDedup <- matches %>%
            arrange(desc(posterior)) %>%
            mutate(
               match_id = row_number()
            ) %>%
            private$enrichPairs(self$match$left, id, self$match$left, id) %>%
            mutate(
               .before = 1,
               Bene    = NA_character_,
               Gab     = NA_character_,
               Lala    = NA_character_,
               Angie   = NA_character_,
            ) %>%
            private$dropNonDupes()

         invisible(self)
      },

      # splink version of reclink(): link master (left) records to the using
      # (right) registry, scoring only the pairs that the blocking rules produce
      splinkReclink = function(threshold_match_weight = -10, min_name_sim = 0.60) {
         splink <- private$loadSplink()
         sp     <- splink$sp
         cl     <- splink$cl

         log_info("Use DuckDB.")
         db_api <- sp$DuckDBAPI()

         log_info("Creating settings.")
         # link_only needs one shared id column; ids may repeat across the two
         # tables since splink keys each record by source_dataset + rec_id
         settings <- sp$SettingsCreator(
            unique_id_column_name                  = "rec_id",
            link_type                              = "link_only",
            blocking_rules_to_generate_predictions = private$splinkBlocking(sp),
            comparisons                            = private$splinkComparisons(cl)
         )

         log_info("Cleaning data.")
         to_splink <- function(data, id) {
            private$splinkFrame(data, id) %>%
               mutate(rec_id = as.character(.data[[id]]), .keep = "unused", .before = 1)
         }
         dfs <- r_to_py(list(
            to_splink(self$match$left, self$left$id),
            to_splink(self$match$right, self$right$id)
         ))

         log_info("Starting linker.")
         linker <- sp$Linker(dfs, settings, db_api, input_table_aliases = c("tbl_master", "tbl_using"))

         # Model training: Estimate the parameters of the model
         log_info("Creating Fellegi-Sunter Model.")
         linker$
            training$
            estimate_probability_two_random_records_match(
            sp$block_on("given_name_sieve", "family_name_sieve"),
            recall = 0.7
         )
         linker$training$estimate_u_using_random_sampling(max_pairs = 1e6)

         log_info("EM Algorithm = {green('First Name+Birth Date')}.")
         linker$
            training$
            estimate_parameters_using_expectation_maximisation(sp$block_on("given_name_3", "birthdate"))
         log_info("EM Algorithm = {green('Last Name+Birth Date')}.")
         linker$
            training$
            estimate_parameters_using_expectation_maximisation(sp$block_on("family_name_sieve", "birthdate"))

         log_info("Generating match pairs.")
         pairwise_predictions <- linker$inference$predict(
            threshold_match_weight = threshold_match_weight
         )

         log_info("Collecting estimation data.")
         # splink does not fix which table lands on _l, so orient each pair
         matches <- linker$misc$query_sql(
            stri_c(
               "SELECT match_probability AS posterior, ",
               "CASE WHEN source_dataset_l = 'tbl_master' THEN rec_id_l ELSE rec_id_r END AS left_id, ",
               "CASE WHEN source_dataset_l = 'tbl_master' THEN rec_id_r ELSE rec_id_l END AS right_id ",
               "FROM ", py_to_r(pairwise_predictions$physical_name)
            ),
            output_type = "pandas"
         ) %>%
            py_to_r() %>%
            as_tibble()

         # ids went to splink as character, so match them back as character
         left_rows  <- mutate(self$match$left, across(all_of(self$left$id), as.character))
         right_rows <- mutate(self$match$right, across(all_of(self$right$id), as.character))

         log_info("Done!")
         self$review$reclink <- matches %>%
            arrange(desc(posterior)) %>%
            mutate(
               match_id = row_number()
            ) %>%
            private$enrichPairs(left_rows, self$left$id, right_rows, self$right$id, min_name_sim = min_name_sim) %>%
            select(-left_id, -right_id) %>%
            mutate(
               Bene  = NA_character_,
               Gab   = NA_character_,
               Lala  = NA_character_,
               Angie = NA_character_,
            ) %>%
            private$dropNonDupes()

         invisible(self)
      }
   ),

   private = list(
      # columns used by the splink blocking rules, training and comparisons;
      # add here if a comparison that needs another column is turned on
      splinkColumns   = c(
         "given_name_sieve",
         "middle_name_sieve",
         "family_name_sieve",
         "given_name_3",
         "given_name_metaphone",
         "family_name_metaphone",
         "birthdate",
         "birth_yr",
         "birth_mo",
         "birth_dy",
         "residence_region",
         "residence_province"
      ),

      loadSplink      = function() {
         env <- "dqt-dedup"

         if (!virtualenv_exists(env)) {
            virtualenv_create(env)
            # splink 5 drops the db_api / input_table_aliases Linker arguments
            virtualenv_install(env, c("pandas", "splink<5"))
         }

         log_info("Initializing virtual environment.")
         suppress_warnings(use_virtualenv(env), "The request to")

         log_info("Loading Splink.")
         list(
            sp  = import("splink", as = "sp", convert = FALSE),
            cl  = import("splink.comparison_library", as = "cl", convert = FALSE),
            cll = import("splink.comparison_level_library", as = "cll", convert = FALSE)
         )
      },

      splinkBlocking  = function(sp) {
         c(
            sp$block_on("given_name_sieve"),
            sp$block_on("family_name_sieve"),
            sp$block_on("birthdate"),
            sp$block_on("given_name_3", "family_name_sieve"),
            sp$block_on("birth_yr", "birth_mo", "given_name_metaphone"),
            sp$block_on("birth_yr", "birth_dy", "given_name_metaphone"),
            sp$block_on("birth_mo", "birth_dy", "given_name_metaphone"),
            sp$block_on("residence_province", "given_name_metaphone"),
            sp$block_on("residence_province", "family_name_metaphone")
         )
      },

      splinkComparisons = function(cl) {
         c(
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
            cl$ExactMatch("residence_region"),
            cl$ExactMatch("residence_province")
         )
      },

      # only the id and splink columns are sent to Python, as plain strings
      splinkFrame     = function(data, id) {
         data %>%
            select(all_of(c(id, private$splinkColumns))) %>%
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
            )
      },

      dropNonDupes    = function(review) {
         conn     <- ohasis$conn("lw")
         nonDupes <- QB$new(conn)$from("ohasis_warehouse.non_dupes")$select(
            "patient_id AS left_cid",
            "non_pair_id AS right_cid"
         )$get(format = 'Arrow')
         dbDisconnect(conn)

         # a non-dupe pair can come out of splink in either order
         nonDupes <- nonDupes %>%
            bind_rows(select(nonDupes, left_cid = right_cid, right_cid = left_cid)) %>%
            distinct()

         anti_join(review, nonDupes, by = join_by(left_cid, right_cid))
      },

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

      # review column suffix = source column; prefixed with left_/right_
      reviewColumns   = c(
         cid          = "central_id",
         given_name   = "given_name",
         middle_name  = "middle_name",
         family_name  = "family_name",
         suffix_name  = "suffix_name",
         birthdate    = "birthdate",
         confirmatory = "confirmatory_code",
         uic          = "uic",
         pxcode       = "patient_code",
         region       = "residence_region",
         province     = "residence_province",
         muncity      = "residence_muncity",
         philhealth   = "philhealth_no",
         philsys      = "philsys_id",
         mobile       = "client_mobile",
         email        = "client_email",
         occupation   = "occupation"
      ),

      # Attach both records' review columns to each pair (left_id/right_id
      # matched against left_key/right_key) and build display names. With
      # min_name_sim, keep only pairs whose names are at least that similar
      # (average of levenshtein, jaro-winkler and 3-gram similarity).
      enrichPairs     = function(pairs, left_data, left_key, right_data, right_key, min_name_sim = NULL) {
         side <- function(data, key, prefix) {
            cols <- c(id = key, private$reviewColumns)
            select(data, all_of(setNames(cols,stri_c(prefix, "_", names(cols)))))
         }
         full_name <- function(family, given, middle, suffix) {
            na_if(str_squish(stri_c(family, ", ", given, " ", middle, " ", suffix)), ",")
         }

         review <- pairs %>%
            left_join(side(left_data, left_key, "left"), by = join_by(left_id)) %>%
            left_join(side(right_data, right_key, "right"), by = join_by(right_id)) %>%
            mutate(across(ends_with("_name"), ~coalesce(., ""))) %>%
            mutate(
               left_name  = full_name(left_family_name, left_given_name, left_middle_name, left_suffix_name),
               right_name = full_name(right_family_name, right_given_name, right_middle_name, right_suffix_name),
            ) %>%
            arrange(desc(posterior))

         if (!is.null(min_name_sim)) {
            # score each distinct name pair once instead of once per row
            similar <- review %>%
               distinct(left_name, right_name) %>%
               filter(!is.na(left_name), !is.na(right_name)) %>%
               mutate(
                  name_sim = (
                     stringsim(left_name, right_name, method = "lv") +
                        stringsim(left_name, right_name, method = "jw") +
                        stringsim(left_name, right_name, method = "qgram", q = 3)
                  ) / 3
               ) %>%
               filter(name_sim >= min_name_sim)

            review <- review %>%
               filter(!is.na(posterior)) %>%
               semi_join(similar, by = join_by(left_name, right_name))
         }

         review
      },

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
            # clean_pii already upper-cases and squishes
            mutate(across(where(is.character), ~clean_pii(stri_trans_general(., "latin-ascii")))) %>%
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
               # sieved names: phonics returns NA for spaces/punctuation
               given_name_soundex    = soundex(given_name_sieve),
               given_name_metaphone  = metaphone(given_name_sieve),
               middle_name_soundex   = soundex(middle_name_sieve),
               middle_name_metaphone = metaphone(middle_name_sieve),
               family_name_soundex   = soundex(family_name_sieve),
               family_name_metaphone = metaphone(family_name_sieve),
            )

         log_info("Sorting UIC.")
         # order the parent codes so a swapped mother/father still matches
         dedup_new %<>%
            mutate(
               uic_1    = pmin(uic_mom, uic_dad),
               uic_2    = pmax(uic_mom, uic_dad),
               uic_sort = stri_c(uic_1, uic_2, substr(uic, 5, 14))
            )

         log_info("Sorting Names.")
         # alphabetically first/last of the name parts longer than 1 letter
         sort_part <- function(x) if_else(stri_length(x) > 1, x, NA_character_)
         dedup_new %<>%
            mutate(
               namesort_given_name  = pmin(
                  sort_part(given_name_sieve),
                  sort_part(middle_name_sieve),
                  sort_part(family_name_sieve),
                  na.rm = TRUE
               ),
               namesort_family_name = pmax(
                  sort_part(given_name_sieve),
                  sort_part(middle_name_sieve),
                  sort_part(family_name_sieve),
                  na.rm = TRUE
               )
            )

         return(dedup_new)
      }
   )
)

fastlink_wsl <- function(dfA, dfB, ..., workdir = "C:/fastlink_tmp", distro = "Ubuntu") {
   dir.create(workdir, showWarnings = FALSE, recursive = TRUE)
   args <- list(...)
   cols <- args$varnames
   id   <- format(Sys.time(), "%Y%m%d%H%M%S")

   worker <- file.path(workdir, "fastlink_worker.R")
   writeLines(c(
      "a   <- commandArgs(trailingOnly = TRUE)",
      "job <- readRDS(a[1])",
      "suppressPackageStartupMessages(library(fastLink))",
      "fl  <- do.call(fastLink, c(list(dfA = job$dfA, dfB = job$dfB), job$args))",
      "saveRDS(fl, a[2])"
   ), worker)

   job_in  <- file.path(workdir, paste0("job_", id, ".rds"))
   job_out <- file.path(workdir, paste0("out_", id, ".rds"))
   on.exit(unlink(c(job_in, job_out)), add = TRUE)

   saveRDS(list(
      dfA  = as.data.frame(dfA)[, cols, drop = FALSE],
      dfB  = as.data.frame(dfB)[, cols, drop = FALSE],
      args = args
   ), job_in)

   # string-only conversion; works for files that don't exist yet
   to_wsl <- function(p) {
      p <- gsub("\\\\", "/", p)
      sub("^([A-Za-z]):", "/mnt/\\L\\1", p, perl = TRUE)
   }

   Sys.setenv(WSL_UTF8 = "1")   # make wsl.exe messages readable in R
   out <- suppressWarnings(system2(
      "wsl",
      c("-d", distro, "-e", "Rscript", to_wsl(worker), to_wsl(job_in), to_wsl(job_out)),
      stdout = TRUE, stderr = TRUE
   ))
   status <- attr(out, "status")
   cat(out, sep = "\n")

   if ((!is.null(status) && status != 0) || !file.exists(job_out)) {
      stop("fastLink in WSL failed (exit code ", status, "). See output above.")
   }

   readRDS(job_out)
}

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

# registry columns pulled for deduplication, per surveillance
registry_columns <- list(
   harp_dx   = c(
      "patient_id", "idnum", "firstname", "middle", "last", "name_suffix", "bdate", "sex", "uic",
      "labcode2", "patient_code", "philhealth", "philsys_id", "mobile", "email",
      "region", "province", "muncity", "curr_work", "prev_work", "job"
   ),
   harp_tx   = c(
      "patient_id", "art_id", "idnum", "first", "middle", "last", "suffix", "birthdate", "sex", "uic",
      "confirmatory_code", "px_code", "philhealth_no", "philsys_id", "mobile", "email"
   ),
   harp_dead = c(
      "patient_id", "mort_id", "idnum", "fname", "mname", "lname", "sname", "birthdate", "sex", "uic",
      "saccl_lab_code", "patient_code", "philhealth", "philsys_id", "mobile", "email"
   ),
   prep      = c(
      "patient_id", "prep_id", "first", "middle", "last", "suffix", "birthdate", "sex", "uic",
      "px_code", "philhealth_no", "philsys_id", "mobile", "email", "curr_reg", "curr_prov", "curr_munc"
   )
)

registry_id <- c(
   harp_dx   = "idnum",
   harp_tx   = "art_id",
   harp_dead = "mort_id",
   prep      = "prep_id"
)

# Load a monthly registry with occupation and central ids. harp_tx and
# harp_dead take residence and work info from their harp_dx record.
load_registry <- function(yr, mo, surv_name, idreg = update_idreg()) {
   surv_name <- match.arg(surv_name, names(registry_columns))
   table     <- str_c('reg_', yr, stri_pad_left(mo, 2, '0'))

   conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(conn), add = TRUE)

   # QB$select() captures its arguments, so pass the names through do.call
   get_registry <- function(db, columns) {
      qb <- QB$new(conn)
      do.call(qb$select, as.list(columns))
      qb$from(stri_c(db, ".", table))$get(format = 'Arrow')
   }

   data <- get_registry(surv_name, registry_columns[[surv_name]])

   if (surv_name %in% c("harp_tx", "harp_dead")) {
      dx   <- get_registry("harp_dx", c("idnum", "region", "province", "muncity", "job", "curr_work", "prev_work"))
      data <- left_join(data, dx, by = join_by(idnum))
   }

   if (surv_name == "prep") {
      data %<>%
         mutate(
            job               = NA_character_,
            curr_work         = NA_character_,
            prev_work         = NA_character_,
            confirmatory_code = NA_character_
         )
   }

   data %>%
      mutate_at(vars(job, curr_work, prev_work), ~na_if(., "")) %>%
      mutate(
         occupation = coalesce(curr_work, prev_work, job)
      ) %>%
      rename_all(tolower) %>%
      get_cid(idreg, patient_id)
}

generate_splink <- function(yr, mo, surv_name) {
   dedup <- Dedup$new()
   dedup$setMaster(load_registry(yr, mo, surv_name), registry_id[[surv_name]])
   dedup$preparePii()
   dedup$splinkDedupe()

   return(dedup)
}

generate_exact <- function(yr, mo, surv_name) {
   dedup <- Dedup$new()
   dedup$setMaster(load_registry(yr, mo, surv_name), registry_id[[surv_name]])
   dedup$preparePii()
   dedup$exact()

   return(dedup)
}

# link harp_tx / harp_dead records that have no idnum yet to harp_dx
reclink_to_dx <- function(yr, mo, surv_name = c("harp_tx", "harp_dead"), method = c("splink", "fastlink")) {
   surv_name <- match.arg(surv_name)
   method    <- match.arg(method)
   idreg     <- update_idreg()

   data <- load_registry(yr, mo, surv_name, idreg)
   dx   <- load_registry(yr, mo, "harp_dx", idreg)

   dedup <- Dedup$new()
   dedup$setMaster(data %>% filter(is.na(idnum)), registry_id[[surv_name]])
   dedup$setUsing(dx, 'idnum')
   dedup$preparePii()
   if (method == "splink") dedup$splinkReclink() else dedup$reclink()

   return(dedup)
}

## sample run for surveillance
# surv <- "harp_dx"
# data <- generate_splink(2025, 7, surv)
# upload_splink(data, surv, "dedup_old")
