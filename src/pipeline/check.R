#' @param ... possible arguments (first, middle, last, sex, birthdate)
check_pii <- function(data, checklist, view_vars = everything(), ...) {
   vars <- match.call(expand.dots = FALSE)$`...`
   data %<>%
      mutate(
         FIRST          = if (!is.null(vars$first)) !!vars$first else FIRST,
         MIDDLE         = if (!is.null(vars$middle)) !!vars$middle else MIDDLE,
         LAST           = if (!is.null(vars$last)) !!vars$last else LAST,
         SEX            = if (!is.null(vars$sex)) !!vars$sex else SEX,
         BIRTHDATE      = if (!is.null(vars$birthdate)) !!vars$birthdate else BIRTHDATE,

         STANDARD_FIRST = stri_trans_general(FIRST, "latin-ascii"),
         SEX            = StrLeft(SEX, 1),
         SEX            = case_when(
            SEX %in% c("1", "M") ~ "1_Male",
            SEX %in% c("2", "F") ~ "2_Female",
            TRUE ~ SEX
         ),

         retain         = 0
      ) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST),
         ~if_else(. == "", NA_character_, ., .)
      )

   log_info("Checking missing PIIs.")
   checklist[["missing_pii"]] <- data %>%
      filter(if_any(c("FIRST", "LAST", "SEX", "BIRTHDATE"), ~is.na(.))) %>%
      select(any_of(view_vars))

   log_info("Checking short names.")
   checklist[["short_name"]] <- data %>%
      mutate(
         n_first  = nchar(FIRST),
         n_middle = nchar(MIDDLE),
         n_last   = nchar(LAST),
         n_name   = n_first + n_middle + n_last,
      ) %>%
      filter(
         n_name <= 10 | n_first <= 3 | n_last <= 3
      ) %>%
      select(any_of(view_vars))

   log_info("Checking for extensions in first/last.")
   extensions <- c("JR", "SR", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X")
   for (suffix_word in extensions)
      data %<>%
         mutate(
            retain = case_when(
               str_detect(FIRST, str_c("\\b", suffix_word, "+\\.*$")) ~ 1,
               str_detect(LAST, str_c("\\b", suffix_word, "+\\.*$")) ~ 1,
               TRUE ~ retain
            )
         )

   checklist[["suffix_in_first/last"]] <- data %>%
      filter(
         retain == 1
      ) %>%
      select(any_of(view_vars))

   log_info("Checking possible wrong sex.")
   genders                  <- gender(unique(data$STANDARD_FIRST), method = "ssa")
   checklist[["wrong_sex"]] <- data %>%
      left_join(
         y  = genders %>%
            mutate(
               PROBABLE_SEX       = case_when(
                  gender == "male" ~ "1_Male",
                  gender == "female" ~ "2_Female",
                  TRUE ~ gender
               ),
               GENDER_PROBABILITY = case_when(
                  gender == "male" ~ proportion_male,
                  gender == "female" ~ proportion_female,
               )
            ) %>%
            filter(GENDER_PROBABILITY >= 0.65) %>%
            select(STANDARD_FIRST = name, PROBABLE_SEX, GENDER_PROBABILITY),
         by = "STANDARD_FIRST"
      ) %>%
      filter(SEX != PROBABLE_SEX) %>%
      relocate(PROBABLE_SEX, .after = SEX) %>%
      select(any_of(view_vars), SEX, PROBABLE_SEX)

   return(checklist)
}

check_addr_psgc <- function(data, checklist, view_vars = everything(), addr_type) {
   addr_type <- toupper(addr_type)
   addr_type <- ifelse(grepl("PSGC", addr_type), gsub("PSGC", "", addr_type), addr_type)
   addr_type <- ifelse(grepl("_", addr_type), gsub("_", "", addr_type), addr_type)
   addr_name <- paste0(tolower(addr_type), "_addr")
   addr_type <- paste0(addr_type, "_PSGC_")

   log_info("Checking incomplete address.")
   checklist[[addr_name]] <- data %>%
      filter(if_any(starts_with(addr_type, ignore.case = FALSE), ~is.na(.))) %>%
      select(any_of(view_vars), starts_with(addr_type, ignore.case = FALSE))

   return(checklist)
}

check_unknown <- function(data, checklist, unknown_name, view_vars = everything(), ...) {

   is_unknown <- function(col) {
      return(ifelse(is.na(col) | toupper(col) == "UNKNOWN", TRUE, FALSE))
   }

   log_info("Checking incomplete address.")
   checklist[[unknown_name]] <- data %>%
      filter(if_any(c(...), ~is_unknown(.))) %>%
      select(any_of(view_vars), ...) %>%
      relocate(..., .after = last_col())

   return(checklist)
}

check_dates <- function(data, checklist, view_vars = everything(), date_vars) {
   log_info("Checking dates.")
   for (var in date_vars) {
      var              <- as.symbol(var)
      checklist[[var]] <- data %>%
         mutate(!!var := as.Date(!!var)) %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var <= -25567
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )
   }

   return(checklist)
}

check_nonnegotiables <- function(data, checklist, view_vars = everything(), nonnegotiables) {
   log_info("Checking if non-negotiable variables are missing.")
   for (var in nonnegotiables) {
      var              <- as.symbol(var)
      checklist[[var]] <- data %>%
         filter(
            is.na(!!var) | toupper(!!var) == "UNKNOWN"
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )
   }

   return(checklist)
}

check_preggy <- function(data, checklist, view_vars = everything(), ...) {
   vars <- match.call(expand.dots = FALSE)$`...`
   data %<>%
      mutate(
         SEX = if (!is.null(vars$sex)) !!vars$sex else SEX,
         SEX = StrLeft(SEX, 1),
         SEX = case_when(
            SEX %in% c("1", "M") ~ "1_Male",
            SEX %in% c("2", "F") ~ "2_Female",
            TRUE ~ SEX
         )
      )

   log_info("Checking for males tagged as pregnant.")
   checklist[["pregnant_m"]] <- data %>%
      filter(StrLeft(SEX, 1) == "1") %>%
      filter_at(
         .vars = vars(any_of(c("MED_IS_PREGNANT", "IS_PREGNANT", "pregnant"))),
         ~StrLeft(., 1) == "1"
      ) %>%
      select(
         any_of(view_vars),
         any_of(c("IS_PREGNANT", "MED_IS_PREGNANT"))
      )

   log_info("Checking for pregnant females.")
   checklist[["pregnant_f"]] <- data %>%
      filter(StrLeft(SEX, 1) == "2") %>%
      filter_at(
         .vars = vars(any_of(c("MED_IS_PREGNANT", "IS_PREGNANT", "pregnant"))),
         ~StrLeft(., 1) == "1"
      ) %>%
      select(
         any_of(view_vars),
         any_of(c("IS_PREGNANT", "MED_IS_PREGNANT"))
      )

   return(checklist)
}

check_age <- function(data, checklist, view_vars = everything(), ...) {
   vars <- match.call(expand.dots = FALSE)$`...`
   data %<>%
      mutate(
         AGE        = if (!is.null(vars$age)) !!vars$age else AGE,
         BIRTHDATE  = if (!is.null(vars$birthdate)) !!vars$birthdate else BIRTHDATE,
         VISIT_DATE = if (!is.null(vars$visit_date)) !!vars$visit_date else VISIT_DATE,
      )

   log_info("Checking calculated age vs computed age.")
   checklist[["mismatch_age"]] <- data %>%
      mutate(
         AGE_DTA = if_else(
            condition = !is.na(BIRTHDATE),
            true      = floor((VISIT_DATE - BIRTHDATE) / 365.25) %>% as.numeric(),
            false     = as.numeric(NA)
         ),
      ) %>%
      mutate_at(.vars = vars(AGE, AGE_DTA), ~as.integer(.)) %>%
      filter(
         AGE != AGE_DTA
      ) %>%
      select(
         any_of(view_vars),
         AGE,
         AGE_DTA
      )

   return(checklist)
}

check_tabstat <- function(data, checklist, vars) {
   log_info("Checking range-median of data.")
   checklist[["tabstat"]] <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)

      checklist[["tabstat"]] <- data %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(checklist[["tabstat"]])
   }

   return(checklist)
}