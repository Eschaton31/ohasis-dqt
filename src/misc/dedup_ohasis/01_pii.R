dedup_reqs <- function() {
   ohasis$db_checks <- ohasis$check_consistency()
   proceed          <- input(
      prompt  = "Do you want to update required tables?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   if (proceed == "1") {
      ohasis$data_factory("lake", "px_pii", "upsert", TRUE)
      ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)
   }
   return(TRUE)
}

dedup_download <- function() {
   # open connections
   lw_conn <- connect("mariadb-lw")

   # instatiate list
   dedup     <- list()
   dedup$pii <- tibble(patient_id = NA_character_) %>%
      slice(0)
   if (file.exists(Sys.getenv("DEDUP_PII")))
      dedup$pii <- read_rds(Sys.getenv("DEDUP_PII"))

   # download latest records not found in previous copy
   min <- "1900-01-01 00:00:00"
   max <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   if (nrow(dedup$pii) > 0)
      min <- format(max(max(dedup$pii$created_at, na.rm = TRUE), max(dedup$pii$updated_at, na.rm = TRUE), max(dedup$pii$deleted_at, na.rm = TRUE)), "%Y-%m-%d %H:%M:00")

   # central id reference
   log_info("Downloading {green('id_registry')}.")
   dedup$id_registry <- update_idreg()

   # download data based on limits (min, max)
   log_info("Downloading {green('pii')}.")
   new_data <- QB$new(lw_conn)$from('ohasis_lake.patients')
   new_data$where(function(query = QB$new(lw_conn)) {
      query$whereBetween('created_at', c(min, max), "or")
      query$whereBetween('updated_at', c(min, max), "or")
      query$whereBetween('deleted_at', c(min, max), "or")
      query$whereNested
   })
   new_data <- new_data$get()

   new_data %<>%
      mutate_at(
         .vars = vars(
            first,
            middle,
            last,
            suffix,
            confirmatory_code,
            patient_code,
            uic,
            philhealth_no,
            philsys_id,
            client_mobile,
            client_email
         ),
         ~clean_pii(.)
      ) %>%
      mutate(
         client_mobile = str_replace_all(client_mobile, "[^[:digit:]]", ""),
         client_mobile = case_when(
            str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
            str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
            TRUE ~ client_mobile
         ),
         birthdate     = as.character(birthdate)
      )

   # finalize data
   dedup$pii <- dedup$pii %>%
      # remove old version of record
      anti_join(select(new_data, patient_id)) %>%
      # append new data
      mutate(birthdate = as.character(birthdate)) %>%
      bind_rows(new_data) %>%
      filter(is.na(deleted_at))

   # write to local file for later use
   write_rds(dedup$pii, Sys.getenv("DEDUP_PII"))

   # close connections
   dbDisconnect(lw_conn)

   return(dedup)
}

dedup_linelist <- function(dedup) {
   log_info("Getting latest data per column.")
   dedup$linelist <- dedup$pii %>%
      get_cid(dedup$id_registry, patient_id) %>%
      mutate(
         curr_psgc = coalesce(curr_brgy, curr_munc, curr_prov, curr_reg),
         perm_psgc = coalesce(perm_brgy, perm_munc, perm_prov, perm_reg),
         snapshot  = max(created_at, updated_at, deleted_at, na.rm = TRUE)
      ) %>%
      select(
         central_id,
         first,
         middle,
         last,
         suffix,
         uic,
         confirmatory_code,
         patient_code,
         birthdate,
         philsys_id,
         philhealth_no,
         client_email,
         client_mobile,
         sex,
         curr_psgc,
         perm_psgc,
         snapshot
      ) %>%
      pivot_longer(
         cols = c(
            first,
            middle,
            last,
            suffix,
            uic,
            confirmatory_code,
            patient_code,
            birthdate,
            philsys_id,
            philhealth_no,
            client_email,
            client_mobile,
            sex,
            curr_psgc,
            perm_psgc,
         )
      ) %>%
      mutate(
         sort = if_else(!is.na(value), 1, 9999, 9999)
      ) %>%
      arrange(sort, desc(snapshot)) %>%
      distinct(central_id, name, .keep_all = TRUE) %>%
      pivot_wider(
         id_cols     = central_id,
         names_from  = name,
         values_from = value
      )

   # load harp diagnosis
   log_info("Reloading HARP dataset.")
   dedup$dx <- hs_data("harp_dx", "reg", ohasis$yr, ohasis$mo) %>%
      read_dta(col_select = c(idnum, patient_id, labcode2)) %>%
      rename_all(tolower) %>%
      get_cid(dedup$id_registry, patient_id)

   log_info("Loading confirmatory data.")
   dedup$linelist %<>%
      left_join(
         y  = dedup$dx %>%
            select(
               central_id,
               labcode2
            ),
         by = "central_id"
      ) %>%
      mutate(
         confirmatory_code = coalesce(labcode2, confirmatory_code),
         birthdate         = if_else(!is.na(birthdate), as.Date(birthdate), NA_Date_)
      )

   # standardize for deduplication
   log_info("Dataset cleaning and preparation.")
   dedup$standard <- dedup$linelist %>%
      dedup_prep(
         name_f       = first,
         name_m       = middle,
         name_l       = last,
         name_s       = suffix,
         uic          = uic,
         birthdate    = birthdate,
         code_confirm = confirmatory_code,
         code_px      = patient_code,
         phic         = philhealth_no,
         philsys      = philsys_id
      ) %>%
      mutate(row_id = row_number()) %>% 
      left_join(
         y = ohasis$ref_addr %>% 
            select(
               curr_psgc = psgc,
               curr_reg = nhsss_reg,
               curr_prov = nhsss_prov,
               curr_munc = nhsss_munc
            ),
         by = join_by(curr_psgc)
      ) %>% 
      left_join(
         y = ohasis$ref_addr %>% 
            select(
               perm_psgc = psgc,
               perm_reg = nhsss_reg,
               perm_prov = nhsss_prov,
               perm_munc = nhsss_munc
            ),
         by = join_by(perm_psgc)
      )

   dedup$num_linked <- dedup$id_registry %>%
      group_by(central_id) %>%
      summarise(
         num_linked = n()
      ) %>%
      ungroup()

   return(dedup)
}

dedup_linelist2 <- function(dedup) {
   log_info("Deduplication standard.")
   dedup$standard %<>%
      select(-any_of("patient_id")) %>%
      rename(patient_id = central_id) %>%
      get_cid(dedup$id_registry, patient_id) %>%
      arrange(central_id, labcode2) %>%
      distinct(central_id, .keep_all = TRUE)

   log_info("Attaching new CID to dx.")
   dedup$dx %<>%
      select(idnum, patient_id, labcode2) %>%
      get_cid(dedup$id_registry, patient_id)

   log_info("Getting new number of matches.")
   dedup$num_linked <- dedup$id_registry %>%
      group_by(central_id) %>%
      summarise(
         num_linked = n()
      ) %>%
      ungroup()

   log_success("Done.")
   return(dedup)
}