##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
log_info("Generating `harp_vl`.`initial`.")

##  Set parameters for vl qr ---------------------------------------------------

vl_mo     <- input(prompt = "What is the reporting month?", max.char = 2)
vl_yr     <- input(prompt = "What is the reporting year?", max.char = 4)
vl_mo     <- vl_mo %>% stri_pad_left(width = 2, pad = "0")
vl_yr     <- vl_yr %>% stri_pad_left(width = 4, pad = "0")
vl_report <- glue("{vl_yr}-{vl_mo}")

# reference dates
end_vl   <- as.character(ceiling_date(as.Date(glue("{vl_yr}-{vl_mo}-01")), "months") - 1)
start_vl <- ceiling_date(as.Date(end_vl), "months") %m-% months(12) %>% as.character()

# ohasis ids
log_info("Downloading OHASIS IDs.")
log_info("Opening connections.")
lw_conn     <- ohasis$conn("lw")
id_registry <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "id_registry",
   cols = c("CENTRAL_ID", "PATIENT_ID")
)
dbDisconnect(lw_conn)

##  Process forms data ---------------------------------------------------------

# Form BC + Lab Data
log_info("Processing forms data.")
lw_conn    <- ohasis$conn("lw")
data_forms <- dbTable(
   lw_conn,
   "ohasis_lake",
   "lab_wide",
   raw_where = TRUE,
   where     = glue(r"(
(LAB_VIRAL_DATE IS NOT NULL OR LAB_VIRAL_RESULT IS NOT NULL)
)"),
   cols      = c("PATIENT_ID", "LAB_VIRAL_DATE", "LAB_VIRAL_RESULT"),
   join      = list(
      "ohasis_lake.px_pii"       = list(by = c("REC_ID" = "REC_ID"), cols = c("RECORD_DATE", "FACI_ID", "SUB_FACI_ID")),
      "ohasis_lake.px_faci_info" = list(by = c("REC_ID" = "REC_ID"), cols = c("REC_ID", "SERVICE_FACI", "SERVICE_SUB_FACI"), type = "left")
   )
)
dbDisconnect(lw_conn)

data_forms <- data_forms %>%
   # get latest central ids
   get_cid(id_registry, PATIENT_ID) %>%
   select(
      CENTRAL_ID,
      FACI_ID,
      SUB_FACI_ID,
      SERVICE_FACI,
      SERVICE_SUB_FACI,
      VISIT_DATE = RECORD_DATE,
      vl_date    = LAB_VIRAL_DATE,
      LAB_VIRAL_RESULT
   ) %>%
   process_vl("LAB_VIRAL_RESULT", "vl_result")


##  Get masterlist data from the past 4 quarters -------------------------------

data_ml <- lapply(0:11, function(i) {
   data <- data.frame()
   date <- end_vl %>% as.Date() %m-% months(i) %>% as.character()
   if (date >= "2022-09-30") {
      date <- strsplit(date, "-")[[1]]
      yr   <- date[[1]]
      mo   <- date[[2]]
      data <- read_dta(hs_data("harp_vl", "ml", yr, mo))
   }

   return(data)
})
data_ml <- bind_rows(data_ml) %>%
   mutate(
      vl_date = as.Date(vl_date),
   ) %>%
   bind_rows(
      read_dta(file.path(Sys.getenv("HARP_VL"), "20220510_vl_ml_ever.dta")) %>%
         rename(
            vl_date   = vl_date_2,
            vl_result = vl_result_2
         ) %>%
         mutate(
            vl_result        = as.numeric(vl_result),
            LAB_VIRAL_RESULT = as.character(vl_result)
         )
   )

# get only needed columns
data_ml %<>%
   select(
      PATIENT_ID,
      hub,
      any_of("vlml2022"),
      starts_with("vl_result"),
      starts_with("vl_date"),
      VL_ERROR,
      VL_DROP,
   ) %>%
   get_cid(id_registry, PATIENT_ID) %>%
   mutate(
      hub    = toupper(hub),
      branch = NA_character_
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(FACI_ID = "hub", SUB_FACI_ID = "branch")
   )

##  Append data and clean results ----------------------------------------------

vl_data <- data_forms %>%
   # res_tag <- check source of the data
   mutate(res_tag = 2) %>%
   bind_rows(
      data_ml %>%
         mutate(res_tag = 1)
   ) %>%
   distinct_all() %>%
   select(-hub) %>%
   mutate(
      FINAL_FACI   = coalesce(SERVICE_FACI, FACI_ID),
      FINAL_SUB    = coalesce(SERVICE_SUB_FACI, SUB_FACI_ID),
      FINAL_FACI_2 = FINAL_FACI,
      FINAL_SUB_2  = FINAL_SUB,
   ) %>%
   ohasis$get_faci(
      list("facility_name" = c("FINAL_FACI", "FINAL_SUB")),
      "name"
   ) %>%
   ohasis$get_faci(
      list("hub" = c("FINAL_FACI_2", "FINAL_SUB_2")),
      "code"
   ) %>%
   mutate(
      res_tag = labelled(
         res_tag,
         c(
            `ml`    = 1,
            `forms` = 2
         )
      )
   ) %>%
   select(
      CENTRAL_ID,
      hub,
      facility_name,
      res_tag,
      vl_date,
      vl_result_encoded = LAB_VIRAL_RESULT,
      vl_result_clean   = vl_result,
      VL_ERROR,
      VL_DROP
   ) %>%
   mutate(
      VL_SORT  = case_when(
         if_all(c(vl_date, vl_result_clean), ~!is.na(.)) ~ 1,
         !is.na(vl_date) & is.na(vl_result_clean) ~ 2,
         is.na(vl_date) & !is.na(vl_result_clean) ~ 3,
         TRUE ~ 9999
      ),
      VL_DROP  = coalesce(if_else(VL_SORT == 9999, 1, VL_DROP, VL_DROP), 0),
      VL_ERROR = coalesce(VL_ERROR, 0)
   )


output_version <- format(Sys.time(), "%Y%m%d")
output_name.vl <- paste0(output_version, '_vldata_', ohasis$yr, '-', ohasis$mo)
file_vl        <- file.path(Sys.getenv("HARP_VL"), paste0(output_name.vl, ".dta"))

# write main file
log_info("Saving in Stata data format.")
write_dta(
   data = format_stata(vl_data),
   path = file_vl
)

##  Merge w/ onart dataset -----------------------------------------------------
# ! RUN ONLY IF NOT YET INCORPORATED
onart_vl <- hs_data("harp_tx", "reg", vl_yr, vl_mo) %>%
   read_dta() %>%
   get_cid(id_registry, PATIENT_ID) %>%
   left_join(
      y  = vl_data %>%
         filter(VL_DROP == 0, VL_ERROR == 0) %>%
         filter(vl_date < end_vl) %>%
         filter(coalesce(CENTRAL_ID, "") != "") %>%
         arrange(VL_SORT, desc(vl_date), res_tag) %>%
         select(CENTRAL_ID, vl_date, vl_result = vl_result_clean) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      # tag baseline data
      baseline_vl   = if_else(
         condition = difftime(vl_date, artstart_date, units = "days") <= 82,
         true      = as.integer(1),
         false     = NA_integer_,
         missing   = NA_integer_
      ),

      # tag if suppressed
      # vl_suppressed = if_else(
      #    condition = vl_result_2 < 1000,
      #    true      = 1,
      #    false     = 0,
      #    missing   = 0
      # ) %>% as.integer(),
      vl_suppressed = if_else(
         condition = vl_result < 50,
         true      = 1,
         false     = 0,
         missing   = 0
      ) %>% as.integer(),

      # analysis variable
      vlp12m        = if_else(
         vl_date >= as.Date(start_vl) & vl_date <= as.Date(end_vl),
         as.integer(0),
         NA_integer_
      ),
      vlp12m        = if_else(
         condition = vlp12m == 0 & vl_suppressed == 1,
         true      = as.integer(1),
         false     = vlp12m,
         missing   = vlp12m
      ),

      vl_yr         = year(vl_date),
      vl_mo         = month(vl_date)
   ) %>%
   distinct(art_id, .keep_all = TRUE) %>%
   select(
      art_id,
      contains("vl")
   ) %>%
   right_join(
      y  = hs_data("harp_tx", "outcome", vl_yr, vl_mo) %>%
         read_dta() %>%
         select(-contains("vl")),
      by = "art_id"
   ) %>%
   relocate(CENTRAL_ID, art_id, .before = 1)


output_version <- format(Sys.time(), "%Y%m%d")
output_name.vl <- paste0(output_version, '_onart-vl_', ohasis$yr, '-', ohasis$mo)

file_vl <- file.path(Sys.getenv("HARP_TX"), paste0(output_name.vl, ".dta"))

# write main file
.log_info("Saving in Stata data format.")
write_dta(
   data = format_stata(onart_vl),
   path = file_vl
)
