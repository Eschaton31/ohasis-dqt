##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_vl`.`initial`.")

##  Set parameters for vl qr ---------------------------------------------------

vl_mo     <- input(prompt = "What is the reporting month?", max.char = 2)
vl_yr     <- input(prompt = "What is the reporting year?", max.char = 4)
vl_mo     <- vl_mo %>% stri_pad_left(width = 2, pad = "0")
vl_yr     <- vl_yr %>% stri_pad_left(width = 4, pad = "0")
vl_report <- glue("{vl_yr}-{vl_mo}")

# reference dates
end_vl   <- as.character(ceiling_date(as.Date(glue("{ohasis$yr}-{ohasis$mo}-01")), "months") - 1)
start_vl <- ceiling_date(as.Date(end_vl), "months") %m-% months(12) %>% as.character()

# ohasis ids
.log_info("Downloading OHASIS IDs.")
.log_info("Opening connections.")
lw_conn     <- ohasis$conn("lw")
id_registry <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "id_registry",
   cols = c("CENTRAL_ID", "PATIENT_ID")
)
dbDisconnect(lw_conn)

##  Get HARP datasets ----------------------------------------------------------

.log_info("Loading HARP Tx Data.")
nhsss$harp_tx$official$new_reg     <- ohasis$get_data("harp_tx-reg", ohasis$yr, ohasis$mo) %>%
   read_dta() %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   select(-starts_with("CENTRAL_ID")) %>%
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      )
   )
nhsss$harp_tx$official$new_outcome <- ohasis$get_data("harp_tx-outcome", ohasis$yr, ohasis$mo) %>%
   read_dta() %>%
   # convert Stata string missing data to NAs
   select(-starts_with("CENTRAL_ID")) %>%
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   left_join(
      y  = nhsss$harp_tx$official$new_reg %>% select(art_id, CENTRAL_ID),
      by = "art_id"
   )

.log_info("Loading HARP Dx Data.")
nhsss$harp_dx$official$new <- ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo) %>%
   read_dta() %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   select(-CENTRAL_ID) %>%
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
      labcode2   = if_else(is.na(labcode2), labcode, labcode2)
   )

##  Process forms data ---------------------------------------------------------

# Form BC + Lab Data
.log_info("Processing forms data.")
lw_conn    <- ohasis$conn("lw")
data_forms <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "form_art_bc",
   raw_where = TRUE,
   where     = glue(r"(
(LAB_VIRAL_DATE IS NOT NULL OR LAB_VIRAL_RESULT) AND
  (VISIT_DATE >= '{start_vl}' OR LAB_VIRAL_DATE >= '{start_vl}')
)"),
   cols      = c("PATIENT_ID", "VISIT_DATE", "LAB_VIRAL_DATE", "LAB_VIRAL_RESULT")
)
dbDisconnect(lw_conn)

data_forms <- data_forms %>%
   # get latest central ids
   left_join(id_registry) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   select(
      CENTRAL_ID,
      vl_date_2 = LAB_VIRAL_DATE,
      vl_result = LAB_VIRAL_RESULT
   )


##  Get masterlist data from the past 4 quarters -------------------------------

# calculate datasets needed
# prev_yr <- as.character(as.numeric(vl_yr) - 1)
# if (vl_mo == "1") {
#    data_ml <- data.frame() %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q1")) %>% mutate(qr = glue("{vl_yr}-Q1"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", prev_yr, "Q4")) %>% mutate(qr = glue("{prev_yr}-Q4"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", prev_yr, "Q3")) %>% mutate(qr = glue("{prev_yr}-Q3"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", prev_yr, "Q2")) %>% mutate(qr = glue("{prev_yr}-Q2")))
# } else if (vl_mo == "2") {
#    data_ml <- data.frame() %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q2")) %>% mutate(qr = glue("{vl_yr}-Q2"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q1")) %>% mutate(qr = glue("{vl_yr}-Q1"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", prev_yr, "Q4")) %>% mutate(qr = glue("{prev_yr}-Q4"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", prev_yr, "Q3")) %>% mutate(qr = glue("{prev_yr}-Q3")))
# } else if (vl_mo == "3") {
#    data_ml <- data.frame() %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q3")) %>% mutate(qr = glue("{vl_yr}-Q3"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q2")) %>% mutate(qr = glue("{vl_yr}-Q2"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q1")) %>% mutate(qr = glue("{vl_yr}-Q1"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", prev_yr, "Q4")) %>% mutate(qr = glue("{prev_yr}-Q4")))
# } else if (vl_mo == "4") {
#    data_ml <- data.frame() %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q4")) %>% mutate(qr = glue("{vl_yr}-Q4"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q3")) %>% mutate(qr = glue("{vl_yr}-Q3"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q2")) %>% mutate(qr = glue("{vl_yr}-Q2"))) %>%
#       bind_rows(read_dta(ohasis$get_data("harp_vl-ml", vl_yr, "Q1")) %>% mutate(qr = glue("{vl_yr}-Q1")))
# }
ml_files <- list.files(Sys.getenv("HARP_VL"), "*\\.dta", full.names = TRUE)
data_ml  <- lapply(ml_files, function(ml) {
   qr <- substr(ml,
                stri_locate_first_fixed(ml, "vl_ml_") + 6,
                stri_locate_first_fixed(ml, ".dta") - 1)
   if (qr != "ever")
      read_dta(ml) %>%
         mutate(
            id = as.character(id),
            qr = qr
         )
})
data_ml  <- bind_rows(data_ml)

# get only needed columns
data_ml %<>%
   select(
      PATIENT_ID,
      hub,
      src_file,
      src_sheet,
      qr,
      starts_with("vl_result"),
      starts_with("vl_date")
   )

##  Process masterlist data ----------------------------------------------------

.log_info("Processing massterlist data.")
data_ml %<>%
   mutate_if(
      .predicate = is.character,
      ~if_else(. == "", NA_character_, as.character(.))
   ) %>%
   # tag data to be dropped
   mutate(
      drop = case_when(
         toupper(vl_date) %in% c("DECEASED", "LTFU", "TRANS OUT", "FOR VL AFTER 2 MONTHS", "HIV 1 NOT DET", "NON-REACTIVE") ~ 1,
         TRUE ~ 0
      )
   ) %>%
   # drop data w/o matched PATIENT_ID
   filter(!is.na(PATIENT_ID), drop == 0) %>%
   mutate(
      # generate row_id for the appended data
      px_hub        = glue("{PATIENT_ID}{hub}{qr}"),

      # fix dates first
      first_slash   = stri_locate_first_fixed(vl_date, "/")[, 1],
      first_dash    = stri_locate_first_fixed(vl_date, "-")[, 1],
      first_dot     = stri_locate_first_fixed(vl_date, ".")[, 1],
      vl_excel_date = if_else(
         condition = StrIsNumeric(vl_date),
         true      = excel_numeric_to_date(as.numeric(vl_date)),
         false     = NA_Date_
      ),
      month_name    = case_when(
         stri_detect_fixed(toupper(vl_date), toupper(month.name[1])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[2])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[3])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[4])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[5])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[6])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[7])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[8])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[9])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[10])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[11])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[12])) ~ 1,
         TRUE ~ 0
      ),
      month_abb     = case_when(
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[1])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[2])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[3])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[4])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[5])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[6])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[7])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[8])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[9])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[10])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[11])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[12])) ~ 1,
         TRUE ~ 0
      ),
      vl_date_2     = case_when(
         !is.na(vl_excel_date) ~ vl_excel_date,
         stri_detect_fixed(vl_date, "/") & hub == "tch" ~ as.Date(StrLeft(vl_date, 10), "%d/%m/%Y"),
         stri_detect_fixed(vl_date, "/") & hub == "dcs" ~ as.Date(StrLeft(vl_date, 10), "%d/%m/%Y"),
         stri_detect_fixed(vl_date, "(") & hub == "ccc" ~ as.Date(substr(vl_result, 2, 11), "%m/%d/%Y"),
         stri_detect_fixed(vl_date, "-") & StrIsNumeric(StrLeft(vl_date, 4)) ~ as.Date(StrLeft(vl_date, 10), "%Y-%m-%d"),
         stri_detect_fixed(vl_date, "/") & StrIsNumeric(StrLeft(vl_date, 4)) ~ as.Date(StrLeft(vl_date, 10), "%Y/%m/%d"),
         first_slash %in% c(2, 3) ~ as.Date(StrLeft(vl_date, 10), "%m/%d/%Y"),
         first_dash %in% c(2, 3) ~ as.Date(StrLeft(vl_date, 10), "%m-%d-%Y"),
         first_dot %in% c(2, 3) ~ as.Date(StrLeft(vl_date, 10), "%m.%d.%Y"),
         stri_detect_fixed(vl_date, ",") & month_name == 1 ~ as.Date(vl_date, "%B %d, %Y"),
         !stri_detect_fixed(vl_date, ",") & month_name == 1 ~ as.Date(vl_date, "%B %d %Y"),
         stri_detect_fixed(vl_date, ",") & month_abb == 1 ~ as.Date(vl_date, "%B %d, %Y"),
         !stri_detect_fixed(vl_date, ",") & month_abb == 1 ~ as.Date(vl_date, "%B %d %Y"),
      ),
   ) %>%
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   )

data_ml <- .cleaning_list(data_ml, nhsss$harp_vl$corr$vl_ml, "PX_HUB", "character")

##  Append data and clean results ----------------------------------------------

vl_data <- data_forms %>%
   # res_tag <- check source of the data
   mutate(res_tag = 2) %>%
   bind_rows(
      data_ml %>%
         select(
            CENTRAL_ID,
            vl_date_2,
            vl_result,
            vl_result_alt
         ) %>%
         mutate(res_tag = 1)
   ) %>%
   distinct_all() %>%
   # clean results
   mutate(
      vl_result      = toupper(str_squish(vl_result)),
      vl_result      = stri_replace_all_fixed(vl_result, "\"", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 VIRAL RNA DETECTED AT ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV - 1 DECTECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV - 1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV0-1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV - 1 DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED,", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV -1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV DETECTED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECETED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED,", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HOV-1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV1 DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIVDETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIVDETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HI-1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECETD, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1- DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HI-1 DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV- DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 NO DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPPIES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "COPIES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CP/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CPM", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIS/ML", ""),
      vl_result      = stri_replace_all_regex(vl_result, "^DETECTED ", ""),
      vl_result      = stri_replace_all_regex(vl_result, "^- ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "C/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "CP/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "COPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "COIPES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIE", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " C0PIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CPOIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "COIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIS", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " XOPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CELLS", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " /ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " M/L", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " / ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "/ ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " PER ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " C/U", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "C/U", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CPPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, ", ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " ,", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " `", ""),
      vl_result      = stri_replace_last_regex(vl_result, "\\.00$", ""),
      vl_result      = stri_replace_last_regex(vl_result, "([:digit:])EO", "$1E0"),
      vl_result      = stri_replace_all_fixed(vl_result, "(12/11/2020) 2.51 E05 (LOG 5.40)", "2.51 E05 (LOG 5.40)"),

      # tag those w/ less than data
      less_than      = case_when(
         stri_detect_fixed(vl_result, ">") ~ 1,
         stri_detect_fixed(vl_result, "+") ~ 1,
         stri_detect_fixed(vl_result, "<") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LESS THAN") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LESS ") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LES THAN") ~ 1,
         TRUE ~ 0
      ),
      has_alpha      = if_else(
         condition = stri_detect_regex(vl_result, "[:alpha:]"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      sci_cal        = case_when(
         stri_detect_fixed(vl_result, "LOG") ~ "log",
         stri_detect_fixed(vl_result, "^") ~ "exponent",
         stri_detect_fixed(vl_result, "EO") ~ "eo",
         stri_detect_regex(vl_result, "[:digit:]E ") ~ "eo",
         stri_detect_regex(vl_result, "[:digit:]E") ~ "eo",
         stri_detect_fixed(vl_result, "X10") ~ "x10",
      ),

      vl_result_2    = case_when(
         sci_cal == "x10" ~ as.numeric(substr(vl_result, 1, stri_locate_first_fixed(vl_result, "X10") - 1)) * 10,
         sci_cal == "eo" ~ as.numeric(str_squish(substr(vl_result, 1, stri_locate_first_regex(vl_result, "[:digit:]E")))) * (10 * as.numeric(substr(vl_result, stri_locate_first_regex(vl_result, "[:digit:]E") + 2, stri_locate_first_regex(vl_result, "[:digit:]E") + 4))),
         less_than == 1 &
            stri_detect_fixed(vl_result, "40") &
            stri_detect_fixed(vl_result, "1.6") ~ 39,
         less_than == 1 & has_alpha == 0 ~ as.numeric(stri_replace_all_regex(vl_result, "[^[:digit:]]", "")) - 1,
         less_than == 1 & has_alpha == 1 & is.na(sci_cal) ~ as.numeric(stri_replace_all_regex(vl_result, "[^[:digit:]]", "")) - 1,
         less_than == 0 &
            has_alpha == 1 &
            is.na(sci_cal) &
            stri_detect_regex(vl_result, "M$") ~ as.numeric(stri_replace_all_regex(vl_result, "[^[:digit:]]", "")) * 1000000,
         StrIsNumeric(vl_result) ~ as.numeric(vl_result),
         stri_detect_fixed(vl_result, ", ") & has_alpha == 0 ~ as.numeric(stri_replace_all_fixed(vl_result, ", ", "")),
         stri_detect_fixed(vl_result, ",") & has_alpha == 0 ~ as.numeric(stri_replace_all_fixed(vl_result, ",", "")),
         stri_detect_fixed(vl_result, "NO T DET") ~ 0,
         stri_detect_fixed(vl_result, "NOT DET") ~ 0,
         stri_detect_fixed(vl_result, "MOT DET") ~ 0,
         stri_detect_fixed(vl_result, "NOT DETETED") ~ 0,
         stri_detect_fixed(vl_result, "UNDETECT") ~ 0,
         stri_detect_fixed(vl_result, "UNDETETABLE") ~ 0,
         stri_detect_fixed(vl_result, "UNDETECETD") ~ 0,
         stri_detect_fixed(vl_result, "NO MEASURABLE") ~ 0,
         stri_detect_fixed(vl_result, "NOT TEDECTED") ~ 0,
         stri_detect_fixed(vl_result, "NOTY DETECTED") ~ 0,
         str_detect(vl_result, glue("\\bND\\b")) ~ 0,
         vl_result %in% c("HND", "ND", "N/D", "UD", "TND", "UNDE", "UN", "N.D", "NP", "UNDECTABLE", "UNDECTED", "UNDET") ~ 0,
         vl_result %in% c("TARGET NOT DETECTED", "TARGET NOT DEFECTED", "TNDD", "TNDS", "U", "UNDETACTABLE", "UNDETE", "UNTEDECTABLE", "UP") ~ 0,
         vl_result %in% c("N", "NO DETECTED", "NO\\", "NONE DETECTED", "NOT", "NOT CONNECTED", "NOT DETECED", "NOT DTECTED", "HIV 1 NOT DTECTED") ~ 0,
         vl_result %in% c("BN", "BD") ~ 0,
         vl_result == "L40" ~ 39,
         vl_result == "L70" ~ 69,
         vl_result == "L70" ~ 69,
         vl_result %in% c("LT 34", "LT6 34") ~ 33,
         !is.na(as.numeric(stri_replace_all_fixed(vl_result, " ", ""))) ~ as.numeric(stri_replace_all_fixed(vl_result, " ", ""))
      ),

      # tag for dropping
      drop           = case_when(
         vl_result == "NOT DONE" ~ 1,
         vl_result == "PENDING" ~ 1,
         vl_result == "TO SECURE" ~ 1,
         vl_result == "NO RESULT" ~ 1,
         vl_result == "NON-REACTIVE" ~ 1,
         vl_result == "N/A" ~ 1,
         vl_result == "NONE" ~ 1,
         vl_result == "REPEAT COLLECTION" ~ 1,
         vl_result == "RC" ~ 1,
         vl_result == "NR" ~ 1,
         vl_result == "DONE.K.SILUNGAN" ~ 1,
         vl_result == "MTB DETECTED" ~ 1,
         vl_result == "INVALID" ~ 1,
         vl_result == "CPPIES" ~ 1,
         vl_result == "HIV I DETECTED" ~ 1,
         vl_result == "DETECTED" ~ 1,
         vl_result == "NA" ~ 1,
         vl_result == "O" ~ 1,
         vl_result == "T" ~ 1,
         vl_result == "." ~ 1,
         vl_result == "*" ~ 1,
         vl_result == "<" ~ 1,
         stri_detect_fixed(vl_result, "WAITING FOR") ~ 1,
         stri_detect_fixed(vl_result, "NOT SUFFICIENT") ~ 1,
         stri_detect_fixed(vl_result, "NOT YET") ~ 1,
         stri_detect_fixed(vl_result, "NO RESULT") ~ 1,
         stri_detect_fixed(vl_result, "TO FOLLOW") ~ 1,
         stri_detect_fixed(vl_result, "TO SECURE") ~ 1,
         stri_detect_fixed(vl_result, "ERROR") ~ 1,
         stri_detect_fixed(vl_result, "AWAITING") ~ 1,
         stri_detect_fixed(vl_result, "CP# 09750471506") ~ 1,
         is.na(vl_result) & is.na(vl_result_alt) ~ 1,
         TRUE ~ 0
      ),

      # log increase
      log_raw        = if_else(
         condition = stri_detect_fixed(vl_result, "LOG"),
         true      = substr(vl_result, 1, stri_locate_first_fixed(vl_result, "LOG") - 1),
         false     = NA_character_
      ),
      log_multiplier = if_else(
         condition = stri_detect_fixed(log_raw, "E"),
         true      = substr(log_raw, stri_locate_first_fixed(log_raw, "E") + 1, nchar(log_raw)),
         false     = NA_character_
      ) %>%
         stri_replace_all_regex("[^[:digit:]]", ""),
      log_multiplier = if_else(
         condition = !is.na(log_multiplier),
         true      = stri_pad_right("1", as.numeric(log_multiplier) + 1, "0"),
         false     = NA_character_
      ) %>% as.numeric(),
      log_raw        = if_else(
         condition = stri_detect_fixed(log_raw, "E"),
         true      = substr(log_raw, 1, stri_locate_first_fixed(log_raw, "E") - 1) %>% stri_replace_all_regex("[^[:digit:]]", ""),
         false     = log_raw
      ),

      log_increase   = if_else(
         condition = stri_detect_fixed(vl_result, "LOG"),
         true      = substr(vl_result, stri_locate_first_fixed(vl_result, "LOG"), nchar(vl_result)),
         false     = NA_character_
      ),
      log_increase   = stri_replace_all_fixed(log_increase, ")", ""),
      log_increase   = stri_replace_all_regex(log_increase, "[:alpha:]", ""),
      log_increase   = stri_replace_all_fixed(log_increase, " ", ""),
      log_increase   = as.numeric(log_increase) * 10,

      # apply calculations
      vl_result_2    = case_when(
         vl_result_2 == 99999 ~ 0,
         !is.na(log_increase) &
            is.na(log_multiplier) &
            !stri_detect_fixed(vl_result, "X") &
            !stri_detect_fixed(vl_result, "E") &
            !stri_detect_fixed(vl_result, "^") ~ as.numeric(stri_replace_all_regex(log_raw, "[^[:digit:]]", "")) * log_increase,
         !is.na(log_increase) &
            !is.na(log_multiplier) &
            stri_detect_fixed(vl_result, "E") &
            !stri_detect_fixed(vl_result, "X") &
            !stri_detect_fixed(vl_result, "^") ~ as.numeric(stri_replace_all_regex(log_raw, "[^[:digit:]]", "")) *
            log_increase *
            log_multiplier,
         is.na(vl_result_2) &
            stri_count_fixed(vl_result, ".") > 1 &
            StrIsNumeric(stri_replace_all_fixed(vl_result, ".", "")) ~ as.numeric(stri_replace_all_regex(vl_result, ".", "")),
         TRUE ~ vl_result_2
      )
   )
vl_data %<>%
   # remove for dropping data
   filter(
      drop == 0,
      !is.na(vl_date_2),
      !is.na(vl_result_2),
   ) %>%
   bind_rows(
      read_dta("E:/_R/library/hiv_vl/data/20220510_vl_ml_ever.dta") %>%
         filter(PATIENT_ID != "") %>%
         mutate_if(
            .predicate = is.character,
            ~if_else(. == "", NA_character_, .)
         ) %>%
         left_join(
            y  = id_registry,
            by = "PATIENT_ID"
         ) %>%
         select(
            CENTRAL_ID,
            vl_date_2,
            vl_result_2,
            res_tag
         )
   )

##  Merge w/ onart dataset -----------------------------------------------------

onart_prev <- read_dta(ohasis$get_data("harp_tx-reg", "2022", "10")) %>%
   left_join(
      y  = read_dta(hs_data("harp_tx", "outcome", "2022", "10")) %>%
         select(
            art_id,
            vl_date,
            vl_result,
            vlp12m
         ),
      by = "art_id"
   ) %>%
   # onart_prev <- read_dta("E:/_R/library/hiv_tx/data/20220207_onart_2021-12.dta") %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   # rename(PATIENT_ID = CENTRAL_ID) %>%
   select(-CENTRAL_ID) %>%
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )

onart_vl <- nhsss$harp_tx$official$new_reg %>%
   left_join(
      y  = vl_data %>%
         select(CENTRAL_ID, vl_date_2, vl_result_2, res_tag),
      by = "CENTRAL_ID"
   ) %>%
   # get data from previous dataset
   # left_join(
   #    y  = onart_prev %>% select(art_id, prev_vldate = vl_date, prev_vlresult = vl_result, prev_vlp12m = vlp12m),
   #    by = "art_id"
   # ) %>%
   mutate(
      # update data if missing in current, use previous
      # use_prev      = case_when(
      #    is.na(vl_result_2) & !is.na(prev_vlresult) ~ 1,
      #    prev_vldate > vl_date_2 ~ 1,
      #    TRUE ~ 0
      # ),
      # vl_result_2   = if_else(
      #    condition = use_prev == 1,
      #    true      = prev_vlresult,
      #    false     = vl_result_2,
      #    missing   = vl_result_2
      # ),
      # vl_date_2     = if_else(
      #    condition = use_prev == 1,
      #    true      = prev_vldate,
      #    false     = vl_date_2,
      #    missing   = vl_date_2
      # ),

      # tag baseline data
      baseline_vl   = if_else(
         condition = difftime(vl_date_2, artstart_date, units = "days") <= 92,
         true      = as.integer(1),
         false     = NA_integer_,
         missing   = NA_integer_
      ),

      # tag if suppressed
      vl_suppressed = if_else(
         condition = vl_result_2 < 1000,
         true      = 1,
         false     = 0,
         missing   = 0
      ) %>% as.integer(),

      # analysis variable
      vlp12m        = if_else(
         condition = vl_date_2 >= as.Date(start_vl) &
            vl_date_2 <= as.Date(end_vl) &
            vl_suppressed == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ) %>% as.integer(),
      vlp12m        = if_else(
         condition = is.na(vl_date_2) | vl_date_2 < as.Date(start_vl),
         true      = NA_integer_,
         false     = vlp12m,
         missing   = vlp12m
      ),

      vl_yr         = year(vl_date_2),
      vl_mo         = month(vl_date_2)
   ) %>%
   # get only data after artstart data
   filter(
      is.na(vl_date_2) | vl_date_2 <= as.Date(end_vl)
   ) %>%
   arrange(desc(vl_date_2), res_tag) %>%
   distinct(art_id, .keep_all = TRUE) %>%
   select(
      art_id,
      contains("vl")
   ) %>%
   right_join(
      y  = nhsss$harp_tx$official$new_outcome,
      by = "art_id"
   ) %>%
   rename(
      vl_date   = vl_date_2,
      vl_result = vl_result_2
   ) %>%
   relocate(CENTRAL_ID, art_id, .before = 1)


output_version <- format(Sys.time(), "%Y%m%d")
output_name.vl <- paste0(output_version, '_onart-vl_', ohasis$yr, '-', ohasis$mo)

nhsss$harp_tx$official$file_vl <- file.path(Sys.getenv("HARP_TX"), paste0(output_name.vl, ".dta"))

# write main file
.log_info("Saving in Stata data format.")
write_dta(
   data = onart_vl,
   path = nhsss$harp_tx$official$file_vl
)