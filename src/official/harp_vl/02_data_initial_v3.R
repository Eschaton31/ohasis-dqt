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
# vl_tly    <- input(prompt = "What is the UNIX path for the TLY VL dataset?")

# reference dates
end_vl   <- as.character(ceiling_date(as.Date(glue("{vl_yr}-{vl_mo}-01")), "months") - 1)
start_vl <- ceiling_date(as.Date(end_vl), "months") %m-% months(12) %>% as.character()

# ohasis ids
id_registry <- update_idreg()

##  Process forms data ---------------------------------------------------------

# Form BC + Lab Data
log_info("Processing forms data.")
lw_conn    <- connect('mariadb-lw')
data_forms <- QB$new(lw_conn)$
   from('ohasis_lake.lab_wide as form')$
   join('ohasis_lake.px_demographics as pii', 'form.rec_id', '=', 'pii.rec_id')$
   leftJoin('ohasis_lake.px_provider as provider', 'form.rec_id', '=', 'provider.rec_id')$
   select("form.rec_id", "pii.faci_id", "pii.sub_faci_id", "provider.service_faci", "provider.service_sub_faci", "form.lab_viral_date", "form.lab_viral_result", "pii.patient_id", "pii.record_date")$
   whereNotNull("form.lab_viral_date")$
   whereNotNull("form.lab_viral_result")$
   get()
dbDisconnect(lw_conn)

data_forms <- data_forms %>%
   # get latest central ids
   get_cid(id_registry, patient_id) %>%
   select(
      central_id,
      faci_id,
      sub_faci_id,
      service_faci,
      service_sub_faci,
      visit_date = record_date,
      vl_date    = lab_viral_date,
      lab_viral_result
   ) %>%
   process_vl("lab_viral_result", "vl_result")


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
   select(-LAB_VIRAL_RESULT) %>%
   rename_all(tolower) %>%
   bind_rows(
      read_dta(file.path(Sys.getenv("HARP_VL"), "20220510_vl_ml_ever.dta")) %>%
         rename(
            vl_date   = vl_date_2,
            vl_result = vl_result_2
         ) %>%
         mutate(
            vl_result        = as.numeric(vl_result),
            lab_viral_result = as.character(vl_result)
         ) %>%
         rename_all(tolower)
   )

# if (vl_tly != "") {
#    data_ml %<>%
#       filter(hub != "TLY") %>%
#       bind_rows(
#          read_dta(vl_tly) %>%
#             select(
#                PATIENT_ID,
#                vl_date          = VL_DATE,
#                LAB_VIRAL_RESULT = VL_RESULT
#             ) %>%
#             mutate(
#                hub = "TLY"
#             ) %>%
#             process_vl("LAB_VIRAL_RESULT", "vl_result")
#       )
# }

# get only needed columns
data_ml %<>%
   select(
      patient_id,
      hub,
      any_of("vlml2022"),
      starts_with("vl_result"),
      starts_with("vl_date"),
      vl_error,
      vl_drop,
   ) %>%
   get_cid(id_registry, patient_id) %>%
   mutate(
      hub    = toupper(hub),
      branch = NA_character_
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(faci_id = "hub", sub_faci_id = "branch")
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
      final_faci   = coalesce(service_faci, faci_id),
      final_sub    = coalesce(service_sub_faci, sub_faci_id),
      final_faci_2 = final_faci,
      final_sub_2  = final_sub,
   ) %>%
   ohasis$get_faci(
      list("facility_name" = c("final_faci", "final_sub")),
      "name"
   ) %>%
   ohasis$get_faci(
      list("hub" = c("final_faci_2", "final_sub_2")),
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
      central_id,
      hub,
      facility_name,
      res_tag,
      vl_date,
      vl_result_encoded = lab_viral_result,
      vl_result_clean   = vl_result,
      vl_error,
      vl_drop
   ) %>%
   mutate(
      vl_sort  = case_when(
         if_all(c(vl_date, vl_result_clean), ~!is.na(.)) ~ 1,
         !is.na(vl_date) & is.na(vl_result_clean) ~ 2,
         is.na(vl_date) & !is.na(vl_result_clean) ~ 3,
         TRUE ~ 9999
      ),
      vl_drop  = coalesce(if_else(vl_sort == 9999, 1, vl_drop, vl_drop), 0),
      vl_error = coalesce(vl_error, 0)
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
