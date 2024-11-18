splinkDedup <- function(data) {
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

   uic_comparison         <- cl$CustomComparison(
      output_column_name     = "uic",
      comparison_description = "UIC",
      comparison_levels      = c(
         cll$NullLevel("uic"),
         cll$ExactMatchLevel("uic"),
         cll$CustomLevel("concat(substr(uic_l, 1, 2), substr(uic_l, 5, 10)) = substr(uic_r, 3, 12)"),
         cll$CustomLevel("concat(substr(uic_r, 1, 2), substr(uic_r, 5, 10)) = substr(uic_l, 3, 12)"),
         cll$ElseLevel()
      )
   )
   first_last_comparison  <- cl$CustomComparison(
      output_column_name     = "first_last",
      comparison_description = "First+Last",
      comparison_levels      = c(
         cll$NullLevel("first_last"),
         cll$ExactMatchLevel("first_last"),
         cll$ColumnsReversedLevel("firstname", "last"),
         cll$ElseLevel()
      )
   )
   middle_last_comparison <- cl$CustomComparison(
      output_column_name     = "middle_last",
      comparison_description = "Middle+Last",
      comparison_levels      = c(
         cll$NullLevel("middle_last"),
         cll$ExactMatchLevel("middle_last"),
         cll$ColumnsReversedLevel("middle", "last"),
         cll$ElseLevel()
      )
   )


   log_info("Creating settings.")
   settings <- sp$SettingsCreator(
      unique_id_column_name                  = "idnum",
      link_type                              = "dedupe_only",
      blocking_rules_to_generate_predictions = c(
         sp$block_on("firstname"),
         sp$block_on("last"),
         sp$block_on("bdate"),
         sp$block_on("substr(firstname, 1, 3)", "last"),
         sp$block_on("substr(bdate, 1, 4)", "firstname"),
         sp$block_on("province", "firstname"),
         sp$block_on("province", "last")
      ),
      comparisons                            = c(
         cl$NameComparison("firstname"),
         cl$NameComparison("middle"),
         cl$NameComparison("last"),
         cl$DateOfBirthComparison(
            "bdate",
            input_is_string     = TRUE,
            datetime_metrics    = c("year", "month", "day"),
            datetime_thresholds = c(1, 1, 10),
         ),
         cl$ExactMatch("region"),
         cl$ExactMatch("province")
      )
   )

   log_info("Cleaning data.")
   df <- data %>%
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
      estimate_probability_two_random_records_match(sp$block_on("firstname", "last"), recall = 0.7)
   linker$training$estimate_u_using_random_sampling(max_pairs = 1e6)

   log_info("EM Algorithm = {green('First Name')}.")
   linker$
      training$
      estimate_parameters_using_expectation_maximisation(sp$block_on("firstname"))

   log_info("EM Algorithm = {green('Last Name')}.")
   linker$
      training$
      estimate_parameters_using_expectation_maximisation(sp$block_on("last"))

   log_info("EM Algorithm = {green('Birth Date')}.")
   linker$
      training$
      estimate_parameters_using_expectation_maximisation(sp$block_on("bdate"))

   log_info("Generating match pairs.")
   pairwise_predictions <- linker$inference$predict(threshold_match_weight = -10)

   log_info("Finalizing estimation object.")
   estimates <- pairwise_predictions$as_pandas_dataframe()

   log_info("Collecting estimation data.")
   matches <- estimates %>% py_to_r() %>% as_tibble()

   log_info("Done!")
   return(matches)
}

lw_conn <- connect("ohasis-lw")
dx      <- QB$new(lw_conn)$from("harp_dx.reg_202409")$get()
dbDisconnect(lw_conn)

dx <- read_dta(hs_data("harp_dx", "reg", 2024, 10), col_select = c(
   idnum,
   firstname,
   middle,
   last,
   name_suffix,
   uic,
   sex,
   bdate,
   philhealth,
   philsys_id,
   region,
   province,
   muncity,
   email,
   mobile
))


matches <- dx %>%
   mutate(
      first_last  = stri_c(firstname, " ", last),
      middle_last = stri_c(middle, " ", last),
   ) %>%
   splinkDedup()

try <- matches %>%
   select(-starts_with("gamma")) %>%
   # filter(match_probability >= .7) %>%
   arrange(desc(match_probability))