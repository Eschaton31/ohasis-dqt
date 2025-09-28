LyVl <- R6Class(
   "LyVl",
   public = list(
      root              = "",
      data              = list(
         artDb     = tibble(),
         vl        = tibble(),
         ids       = tibble(),
         converted = tibble(),
         existing  = tibble(),
         forUpload = tibble()
      ),
      issues            = list(),
      tables            = list(),

      initialize        = function() {
         self$root <- file.path(getwd(), "data", "ly-imports", format(Sys.time(), "%Y%m%d"))

         invisible(self)
      },
      downloadArtDb     = function() {
         local_drive_quiet()
         local_gs4_quiet()

         ss     <- "1smORFFrPwFFrbXQuUUqxNnxyD9VInEPFL7XgL-dmvUM"
         sheets <- range_speedread(ss, "art", col_types = cols(.default = "c"))

         # ! ART
         dir <- file.path(self$root, "art")
         check_dir(dir)

         for (i in seq_len(nrow(sheets))) {
            branch <- sheets[i,]$branch
            link   <- sheets[i,]$link
            file   <- file.path(dir, stri_c(branch, ".ods"))
            log_info("Downloading ART = {green(branch)}.")
            drive_download(link, file, overwrite = TRUE)
         }

         invisible(self)
      },
      downloadVl        = function() {
         local_drive_quiet()
         local_gs4_quiet()

         # ! ARV
         dir <- file.path(self$root, "vl")
         check_dir(dir)

         ss <- "1nLaOoPB7_Or7BDsMKW9J0wOe5MOPxTAG2KWFrBLTYhc"
         # sheets <- sheet_names(ss)

         for (year in 2018:2025) {
            link <- as_id(ss)
            file <- file.path(dir, stri_c(year, ".ods"))
            log_info("Downloading ARV = {green(year)}.")
            write_ods(read_sheet(link, as.character(year), col_types = "c", range = "C:O"), file)
         }

         invisible(self)
      },
      readIds           = function() {

         con           <- connect("old-lw")
         self$data$ids <- QB$new(con)$from("ohasis_lake.ly_clients")$get()
         dbDisconnect(con)

         invisible(self)
      },
      readArtDb         = function() {
         files       <- list.files(file.path(self$root, "art"), full.names = TRUE)
         data        <- pblapply(files, read_ods, sheet = "Client Information", col_types = cols(.default = "c"), .name_repair = "unique_quiet")
         data        <- lapply(data, mutate_all, toupper)
         data        <- lapply(data, mutate_all, ~na_if(., "0"))
         data        <- lapply(data, mutate_all, ~na_if(., "-"))
         data        <- lapply(data, mutate_all, ~na_if(., "N/A"))
         data        <- lapply(data, mutate_all, ~na_if(., "Err:522"))
         data        <- lapply(data, mutate_all, ~na_if(., "NULL"))
         data        <- lapply(data, mutate_all, ~na_if(., "#REF!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#NAME!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#VALUE!"))
         data        <- lapply(data, mutate, Row = stri_c("A", row_number() + 1), .before = 1)
         data        <- lapply(data, rename_all, ~toupper(stri_replace_all_regex(., "\\s", "")))
         names(data) <- tools::file_path_sans_ext(basename(files))

         progress        <- pblapply(files, read_ods, sheet = "Client Progress Report", col_types = cols(.default = "c"), .name_repair = "unique_quiet")
         progress        <- lapply(progress, mutate_all, toupper)
         progress        <- lapply(progress, mutate_all, ~na_if(., "0"))
         progress        <- lapply(progress, mutate_all, ~na_if(., "-"))
         progress        <- lapply(progress, mutate_all, ~na_if(., "N/A"))
         progress        <- lapply(progress, mutate_all, ~na_if(., "Err:522"))
         progress        <- lapply(progress, mutate_all, ~na_if(., "NULL"))
         progress        <- lapply(progress, mutate_all, ~na_if(., "#REF!"))
         progress        <- lapply(progress, mutate_all, ~na_if(., "#NAME!"))
         progress        <- lapply(progress, mutate_all, ~na_if(., "#VALUE!"))
         progress        <- lapply(progress, mutate, Row = stri_c("A", row_number() + 1), .before = 1)
         progress        <- lapply(progress, rename_all, ~toupper(stri_replace_all_regex(., "\\s", "")))
         names(progress) <- tools::file_path_sans_ext(basename(files))

         self$data$artDb <- bind_rows(data, .id = "src") %>%
            mutate(row_id = row_number()) %>%
            mutate(Branch = if_else(src %in% c("ANGLO-1", "ANGLO-2"), "ANGLO", src, src)) %>%
            rename_all(tolower) %>%
            select(
               row_id,
               Branch           = branch,
               Row              = `row`,
               file             = `src`,
               status           = `status`,
               ly_started_tx    = `tlystartedtreatment`,
               patient_code     = `clientcode`,
               uic              = `uic`,
               accession_code   = `accessioncode`,
               last             = `legalsurname`,
               first            = `legalfirstname`,
               middle           = `legalmiddlename`,
               suffix           = `suffix`,
               nickname         = `preferredname`,
               birthdate_auto   = `dateofbirthmm/dd/yyyy(auto)`,
               sex              = `sexatbirth`,
               self_ident       = `genderidentity`,
               curr_addr        = `homeaddress`,
               work_addr        = `workaddress`,
               is_pregnant      = `pregonstart`,
               client_mobile    = `contact#`,
               client_email     = `emailaddress`,
               counselor        = `lifecoach/counselor`,
               philhealth_no    = `philhealth#`,
               birthdate_manual = `dateofbirthmm/dd/yyyy`,
            ) %>%
            mutate(
               birthdate_auto = if_else(is.na(birthdate_auto) & nchar(uic) == 14, stri_c(sep = "-", substr(uic, 7, 8), substr(uic, 9, 10), str_right(uic, 4)), birthdate_auto, birthdate_auto),
               birthdate      = as.Date(parse_date_time(coalesce(birthdate_manual, birthdate_auto), "mdY")),
               .after         = uic
            ) %>%
            remove_empty("rows", 0.154) %>%
            left_join(
               y  = bind_rows(progress, .id = "src") %>%
                  mutate(Branch = if_else(src %in% c("ANGLO-1", "ANGLO-2"), "ANGLO", src, src)) %>%
                  rename_all(tolower) %>%
                  select(
                     Branch           = branch,
                     file             = `src`,
                     patient_code     = 4,
                     uic              = 5,
                     lab_viral_date   = 7,
                     lab_viral_result = 6
                  ) %>%
                  filter(if_all(c(lab_viral_date, lab_viral_result), ~!is.na(.))),
               by = join_by(file, patient_code, uic)
            ) %>%
            mutate(
               lab_viral_date = as.Date(parse_date_time(lab_viral_date, "mdY")),
            )

         addr     <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "addr", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         ref_addr <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_addr", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         self$data$artDb %<>%
            select(-starts_with("CORR_NAME_")) %>%
            mutate(
               CURR_NAME_REG  = "UNKNOWN",
               CURR_NAME_PROV = "UNKNOWN",
               CURR_NAME_MUNC = curr_addr
            ) %>%
            left_join(
               y  = addr %>%
                  select(
                     CURR_NAME_REG  = NAME_REG,
                     CURR_NAME_PROV = NAME_PROV,
                     CURR_NAME_MUNC = NAME_MUNC,
                     CORR_NAME_REG,
                     CORR_NAME_PROV,
                     CORR_NAME_MUNC
                  ),
               by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
            ) %>%
            mutate(
               CURR_NAME_REG  = coalesce(CORR_NAME_REG, CURR_NAME_REG),
               CURR_NAME_PROV = coalesce(CORR_NAME_PROV, CURR_NAME_PROV),
               CURR_NAME_MUNC = coalesce(CORR_NAME_MUNC, CURR_NAME_MUNC),
            ) %>%
            left_join(
               y  = ref_addr %>%
                  mutate_at(
                     .vars = vars(NAME_REG, NAME_PROV, NAME_MUNC),
                     ~str_squish(toupper(.))
                  ) %>%
                  select(
                     CURR_NAME_REG  = NAME_REG,
                     CURR_NAME_PROV = NAME_PROV,
                     CURR_NAME_MUNC = NAME_MUNC,
                     CURR_PSGC_REG  = PSGC_REG,
                     CURR_PSGC_PROV = PSGC_PROV,
                     CURR_PSGC_MUNC = PSGC_MUNC
                  ),
               by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
            ) %>%
            mutate(
               CURR_PSGC = coalesce(CURR_PSGC_MUNC, CURR_PSGC_PROV, CURR_PSGC_REG),
            ) %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  select(
                     CURR_PSGC = psgc_old,
                     curr_reg  = reg,
                     curr_prov = prov,
                     curr_munc = munc
                  ),
               by = join_by(CURR_PSGC)
            )

         invisible(self)
      },
      readVl            = function() {
         files       <- list.files(file.path(self$root, "vl"), full.names = TRUE)
         data        <- pblapply(files, read_ods, col_types = cols(.default = "c"), .name_repair = "unique_quiet")
         data        <- lapply(data, mutate_all, toupper)
         data        <- lapply(data, mutate_all, ~na_if(., ""))
         data        <- lapply(data, mutate_all, ~na_if(., "0"))
         data        <- lapply(data, mutate_all, ~na_if(., "-"))
         data        <- lapply(data, mutate_all, ~na_if(., "---"))
         data        <- lapply(data, mutate_all, ~na_if(., "N/A"))
         data        <- lapply(data, mutate_all, ~na_if(., "#N/A"))
         data        <- lapply(data, mutate_all, ~na_if(., "Err:522"))
         data        <- lapply(data, mutate_all, ~na_if(., "NULL"))
         data        <- lapply(data, mutate_all, ~na_if(., "#REF!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#NAME!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#VALUE!"))
         data        <- lapply(data, mutate, Row = stri_c("A", row_number() + 1), .before = 1)
         data        <- lapply(data, rename_all, ~toupper(stri_replace_all_regex(., "\\s", "")))
         names(data) <- tools::file_path_sans_ext(basename(files))

         self$data$vl <- bind_rows(data, .id = "year") %>%
            mutate(row_id = row_number()) %>%
            rename_all(tolower) %>%
            select(
               row_id,
               year,
               name             = 4,
               patient_code     = 5,
               birthdate        = 7,
               age              = 8,
               sex              = 9,
               lab_viral_date   = 3,
               lab_viral_result = 14,
            ) %>%
            filter(!if_all(c(lab_viral_date, lab_viral_result), ~is.na(.))) %>%
            mutate(patient_code = coalesce(patient_code, name)) %>%
            mutate_at(
               .vars = vars(lab_viral_date, birthdate),
               ~as.Date(parse_date_time(., "mdY"))
            ) %>%
            mutate(
               sex = case_when(
                  sex == "M" ~ "MALE",
                  sex == "F" ~ "FEMALE",
                  TRUE ~ NA_character_
               ),
            )

         invisible(self)
      },
      convert           = function() {
         art                 <- self$data$artDb %>%
            left_join(
               y  = self$data$ids %>%
                  rename_all(tolower) %>%
                  filter(!is.na(central_id)) %>%
                  select(-row_id) %>%
                  rename(patient_code = client_code) %>%
                  distinct(
                     patient_code,
                     birthdate,
                     last,
                     first,
                     middle,
                     suffix,
                     sex,
                     client_mobile,
                     client_email,
                     uic,
                     philhealth_no,
                     .keep_all = TRUE
                  ),
               by = join_by(
                  patient_code,
                  birthdate,
                  last,
                  first,
                  middle,
                  suffix,
                  sex,
                  client_mobile,
                  client_email,
                  uic,
                  philhealth_no
               )
            )
         self$data$converted <- self$data$vl %>%
            fullname_to_components(name) %>%
            rename(
               first  = FirstName,
               middle = MiddleName,
               last   = LastName
            ) %>%
            left_join(art %>% select(-Row, -row_id, -first, -middle, -last, -sex, -lab_viral_result, -lab_viral_date), join_by(patient_code)) %>%
            mutate(
               birthdate = coalesce(birthdate.x, birthdate.y),
               birthdate = if_else(is.na(birthdate) & nchar(uic) == 14, as.Date(stri_c(sep = "-", str_right(uic, 4), substr(uic, 7, 8), substr(uic, 9, 10))), birthdate, birthdate),
            ) %>%
            select(-ends_with(".x"), -ends_with(".y")) %>%
            arrange(lab_viral_date) %>%
            distinct(row_id, .keep_all = TRUE) %>%
            bind_rows(
               art %>%
                  filter(!is.na(lab_viral_result))
            ) %>%
            mutate(
               sex = case_when(
                  sex == "MALE" ~ "1",
                  sex == "FEMALE" ~ "2",
                  TRUE ~ sex
               ),
            )

         invisible(self)
      },
      checkIssues       = function() {
         self$issues <- list(
            `categorical`    = self$data$converted %>%
               categorical_values(c(
                  "sex",
                  "self_ident",
                  "visit_type",
                  "tb_screen",
                  "tb_ipt_status",
                  "tx_status",
                  "client_type",
                  "final_arv",
                  "tb_status"
               )),
            `tx-not2025`     = self$data$converted %>% filter(disp_date < "2025-01-01" | disp_date > now()),
            `tx-no_name`     = self$data$converted %>% filter(is.na(first), is.na(last)),
            `tx-no_arv`      = self$data$converted %>% filter(is.na(final_arv)),
            `tx-no_dispense` = self$data$converted %>% filter(coalesce(parse_number(disp_total), 0) <= 0),
            `tx-not_in_txdb` = self$data$converted %>% filter(is.na(file)),
            `tx-no_bdate`    = self$data$converted %>% filter(is.na(birthdate)),
            `tx-more6mos`    = self$data$converted %>% filter(parse_number(disp_total) > 180),
            `tx-no_cid`      = self$data$converted %>%
               filter(is.na(central_id)) %>%
               distinct(
                  central_id,
                  confirmatory_code,
                  patient_code,
                  uic,
                  birthdate,
                  sex,
                  first,
                  middle,
                  last,
                  suffix,
                  philhealth_no,
                  client_mobile,
                  client_email,
                  curr_addr
               ),
            `tx-no_addr`     = self$data$artDb %>%
               filter(is.na(curr_reg)) %>%
               distinct(NAME_REG = CURR_NAME_REG, NAME_PROV = CURR_NAME_PROV, NAME_MUNC = CURR_NAME_MUNC)
         )

         invisible(self)
      },
      addNewPatients    = function() {
         max_id <- max(self$data$ids$row_id)
         new    <- self$data$converted %>%
            filter(is.na(central_id)) %>%
            mutate(
               row_id = max_id + row_number()
            ) %>%
            left_join(read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "facility_id", col_types = "c") %>% rename(Branch = SITE), join_by(Branch)) %>%
            left_join(read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "addr", col_types = "c") %>% rename(curr_addr = addr), join_by(curr_addr)) %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  select(
                     psgc,
                     curr_reg  = reg,
                     curr_prov = prov,
                     curr_munc = munc,
                     curr_brgy = brgy,
                  ),
               by = join_by(psgc)
            ) %>%
            distinct(
               row_id,
               faci_id     = FACI_ID,
               sub_faci_id = SUB_FACI_ID,
               patient_code,
               confirmatory_code,
               last,
               first,
               middle,
               suffix,
               uic,
               birthdate,
               philhealth_no,
               sex,
               client_mobile,
               client_email,
               curr_reg,
               curr_prov,
               curr_munc,
               curr_brgy,
               curr_addr,
               self_ident
            ) %>%
            mutate(
               drop = is.na(uic) & is.na(birthdate) & is.na(first)
            ) %>%
            filter(!drop) %>%
            select(-drop)

         created <- oh_batch_newpx(new, "row_id")

         con <- connect('old-lw')
         dbxUpsert(
            con,
            Id(schema = "ohasis_lake", table = "ly_clients"),
            created %>%
               rename(client_code = patient_code) %>%
               rename(central_id = patient_id) %>%
               select(-curr_addr, -curr_reg, -curr_prov, -curr_munc, -curr_brgy, -self_ident, -faci_id, -sub_faci_id, -philsys_id, -created_at, -created_by) %>%
               rename_all(toupper) %>%
               rename(row_id = ROW_ID) %>%
               select(any_of(names(self$data$ids))),
            "row_id"
         )
         dbDisconnect(con)

         invisible(self)
      },
      getExisting       = function() {
         self$data$idreg <- update_idreg()

         lw_conn            <- connect('mariadb-lw')
         self$data$existing <- QB$new(lw_conn)$
            from('ohasis_lake.lab_wide as form')$
            join('ohasis_lake.px_demographics as pii', 'form.rec_id', '=', 'pii.rec_id')$
            select("form.rec_id", "form.lab_viral_date", "form.lab_viral_result", "form.created_by", "form.created_at", "pii.patient_id")$
            whereNotNull("form.lab_viral_date")$
            whereNotNull("form.lab_viral_result")$
            get()

         self$data$existing %<>%
            mutate_if(
               .predicate = is.POSIXct,
               ~as.Date(.)
            ) %>%
            get_cid(self$data$idreg, patient_id)

         dbDisconnect(lw_conn)

         invisible(self)
      },
      prepareUpload     = function() {
         local_gs4_quiet()

         timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

         for_import <- self$data$converted %>%
            filter(lab_viral_date <= now()) %>%
            mutate(
               lab_viral_date = as.Date(lab_viral_date),
               disease        = '101000',
               module         = '5',
               faci_id        = '130001',
               sub_faci_id    = NA_character_,
            ) %>%
            rename(
               patient_id = central_id
            ) %>%
            get_cid(self$data$idreg, patient_id) %>%
            distinct(central_id, lab_viral_date, .keep_all = TRUE) %>%
            mutate(
               record_date = lab_viral_date,
            ) %>%
            # get records id if existing
            left_join(
               y  = self$data$existing %>%
                  select(
                     rec_id,
                     created_by,
                     created_at,
                     central_id,
                     lab_viral_date
                  ),
               by = join_by(central_id, record_date == lab_viral_date)
            ) %>%
            mutate(birthdate = as.Date(birthdate)) %>%
            # retain only not uploaded and those with changes
            filter(!is.na(patient_id)) %>%
            anti_join(
               y  = self$data$existing,
               by = join_by(rec_id),
            ) %>%
            mutate(
               old_rec    = if_else(!is.na(rec_id), 1, 0, 0),
               created_by = coalesce(created_by, "1300000048"),
               created_at = coalesce(as.character(created_at), timestamp),
               updated_by = if_else(old_rec == 1, "1300000048", NA_character_),
               updated_at = if_else(old_rec == 1, timestamp, NA_character_)
            ) %>%
            relocate(any_of(names(self$data$existing)), .before = 1) %>%
            select(-old_rec) %>%
            distinct(row_id, .keep_all = TRUE)

         final_import <- for_import %>%
            filter(!is.na(rec_id)) %>%
            bind_rows(
               batch_rec_ids(for_import %>% filter(is.na(rec_id)), rec_id, created_by, "row_id")
            )

         final_import %<>%
            mutate(
               updated_by = "1300000048",
               updated_at = timestamp
            ) %>%
            left_join(
               y  = self$data$existing %>%
                  select(rec_id, corr_pid = patient_id),
               by = join_by(rec_id)
            ) %>%
            mutate(
               patient_id = coalesce(corr_pid, patient_id)
            )

         self$data$forUpload <- final_import

         invisible(self)
      },
      deconstructTables = function() {
         # tables           <- list()
         # tables$px_record <- list(
         #    name = "px_record",
         #    pk   = c("rec_id", "patient_id"),
         #    data = self$data$forUpload %>%
         #       mutate(
         #          faci_id     = "130001",
         #          sub_faci_id = NA_character_,
         #          disease     = "101000",
         #          module      = 3
         #       ) %>%
         #       select(
         #          rec_id,
         #          patient_id,
         #          faci_id,
         #          sub_faci_id,
         #          record_date,
         #          disease,
         #          module,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_info <- list(
         #    name = "px_info",
         #    pk   = c("rec_id", "patient_id"),
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          patient_id,
         #          confirmatory_code,
         #          uic,
         #          patient_code,
         #          sex,
         #          birthdate,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_name <- list(
         #    name = "px_name",
         #    pk   = c("rec_id", "patient_id"),
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          patient_id,
         #          first,
         #          middle,
         #          last,
         #          suffix,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_contact <- list(
         #    name = "px_contact",
         #    pk   = c("rec_id", "contact_type"),
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #          client_mobile,
         #          client_email
         #       ) %>%
         #       pivot_longer(
         #          cols      = c(client_mobile, client_email),
         #          names_to  = "contact_type",
         #          values_to = "contact"
         #       ) %>%
         #       mutate(
         #          contact_type = case_when(
         #             contact_type == "client_mobile" ~ "1",
         #             contact_type == "client_email" ~ "2",
         #             TRUE ~ contact_type
         #          )
         #       )
         # )
         #
         # tables$px_faci <- list(
         #    name = "px_faci",
         #    pk   = c("rec_id", "service_type"),
         #    data = self$data$forUpload %>%
         #       mutate(
         #          service_type = "101201",
         #          service_faci = as.character(service_faci),
         #       ) %>%
         #       select(
         #          rec_id,
         #          faci_id     = service_faci,
         #          sub_faci_id = service_sub_faci,
         #          service_type,
         #          visit_type,
         #          client_type,
         #          tx_status,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(visit_type, client_type, tx_status),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_form <- list(
         #    name = "px_form",
         #    pk   = c("rec_id", "form"),
         #    data = self$data$forUpload %>%
         #       mutate(
         #          form    = "ART Form",
         #          version = "2021"
         #       ) %>%
         #       select(
         #          rec_id,
         #          form,
         #          version,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_profile <- list(
         #    name = "px_profile",
         #    pk   = "rec_id",
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          age,
         #          self_ident,
         #          self_ident_other,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(self_ident),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_tb <- list(
         #    name = "px_tb",
         #    pk   = "rec_id",
         #    data = self$data$forUpload %>%
         #       mutate(
         #          tb_status = case_when(
         #             !is.na(tb_ipt_status) ~ "0_No active TB",
         #             TRUE ~ tb_status
         #          )
         #       ) %>%
         #       select(
         #          rec_id,
         #          tb_screen,
         #          tb_status,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(tb_screen, tb_status),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_tb_ipt <- list(
         #    name = "px_tb_ipt",
         #    pk   = "rec_id",
         #    data = self$data$forUpload %>%
         #       mutate(
         #          tb_status = case_when(
         #             !is.na(tb_ipt_status) ~ "0_No active TB",
         #             TRUE ~ tb_status
         #          )
         #       ) %>%
         #       select(
         #          rec_id,
         #          tb_ipt_status,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(tb_ipt_status),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_medicine <- list(
         #    name = "px_medicine",
         #    pk   = c("rec_id", "medicine", "disp_num"),
         #    data = self$data$forUpload %>%
         #       separate_longer_delim(
         #          cols  = medicine_summary,
         #          delim = "+"
         #       ) %>%
         #       mutate(
         #          service_faci = as.character(service_faci),
         #          unit_basis   = "2",
         #          medicine     = case_when(
         #             medicine_summary == "tdf/3tc/dtg" ~ "2029",
         #             medicine_summary == "tdf/3tc/efv" ~ "2015",
         #             medicine_summary == "azt/3tc" ~ "2018",
         #             medicine_summary == "lpv/r" ~ "2009",
         #             medicine_summary == "dtg" ~ "2035",
         #             medicine_summary == "efv" ~ "2004",
         #             medicine_summary == "3tc" ~ "2023",
         #             medicine_summary == "abc" ~ "2002",
         #             medicine_summary == "ftc" ~ "2031",
         #             medicine_summary == "nvp" ~ "2011",
         #             medicine_summary == "ril" ~ "2014",
         #             medicine_summary == "tdf" ~ "2017",
         #             medicine_summary == "tdf/3tc" ~ "2016",
         #          ),
         #       ) %>%
         #       filter(!is.na(medicine)) %>%
         #       group_by(row_id) %>%
         #       mutate(
         #          disp_num = row_number(),
         #          # disp_total = typical_per_day * disp_total
         #       ) %>%
         #       ungroup() %>%
         #       select(
         #          rec_id,
         #          faci_id     = service_faci,
         #          sub_faci_id = service_sub_faci,
         #          medicine,
         #          disp_num,
         #          unit_basis,
         #          per_day,
         #          disp_total,
         #          medicine_left,
         #          medicine_missed,
         #          disp_date   = visit_date,
         #          next_date   = latest_next_date,
         #       )
         # )

         self$tables <- deconstruct_vl(self$data$forUpload)

         for (table in names(self$tables)) {
            value_cols <- names(self$tables[[table]]$data)
            value_cols <- setdiff(value_cols, self$tables[[table]]$pk)
            value_cols <- setdiff(value_cols, c('created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'faci_id', 'sub_faci_id'))

            self$tables[[table]]$data %<>%
               filter(if_any(all_of(value_cols), ~!is.na(.)))
         }

         invisible(self)
      },
      upload            = function() {
         db_conn <- ohasis$conn("db")
         lapply(self$tables, function(ref, db_conn) {
            log_info("Uploading {green(ref$name)}.")
            table_space <- Id(schema = "ohasis", table = ref$name)
            dbxUpsert(db_conn, table_space, ref$data, ref$pk)
         }, db_conn)
         dbDisconnect(db_conn)

         invisible(self)
      }
   )
)
