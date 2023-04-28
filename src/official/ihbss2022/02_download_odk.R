##  Download odk submissions ---------------------------------------------------

ihbss_odk_submissions <- function(odk_config, survey) {
   survey <- toupper(survey)
   log_info("Downloading ODK Central data for the {green(survey)} survey.")
   subs_data <- apply(odk_config, 1, function(config) {
      config <- as.list(config)
      ruODK::ru_setup(svc = config$OData, tz = "Asia/Hong_Kong", verbose = FALSE)

      data <- data.frame()
      tryCatch({
         log_info("Downloading data for {green(config$Context)}.")
         data <- odata_submission_get(download = FALSE) %>%
            mutate(
               Survey   = toupper(survey),
               Language = config$Language,
               Context  = config$Context,
               .before  = 1
            )

         age_vars <- get_names(data, "_age_")
         if (length(age_vars) != 0)
            data %<>%
               mutate_at(
                  .vars = vars(all_of(age_vars)),
                  ~as.integer(.)
               )
      },
         error = function(e) .log_warn("No data yet for {red(config$Context)}.")
      )
      return(data)
   })
   subs_list <- apply(odk_config, 1, function(config) {
      config <- as.list(config)
      ruODK::ru_setup(svc = config$OData, tz = "Asia/Hong_Kong", verbose = FALSE)

      subs <- data.frame()
      tryCatch({
         log_info("Downloading submission list for {green(config$Context)}.")
         subs <- ruODK::submission_list()
      },
         error = function(e) .log_warn("No submissions yet for {red(config$Context)}.")
      )
      return(subs)
   })

   data <- bind_rows(subs_data) %>%
      left_join(
         y  = bind_rows(subs_list) %>%
            select(
               id               = instance_id,
               submitter_name   = submitter_display_name,
               submitter_device = device_id,
               review_state
            ),
         by = join_by(id)
      ) %>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(.)
      )

   log_success("Done.")
   return(data)
}

##  Standardize variable names -------------------------------------------------

ihbss_rename <- function(data, survey, medtech = NULL) {
   survey <- tolower(survey)

   if (survey == "fsw")
      data %<>%
         left_join(
            y  = medtech %>%
               select(
                  sq_1_rid,
                  sq_1_rid_001,
                  int_2_name_interviewer,
                  int_ihbss_site,
                  int_type_of_kp,
                  sq_1_rid,
                  sq_1_rid_001,
                  n_1_hiv,
                  n_2_syph,
                  n_3_hepb,
                  n_3_hepb_001,
                  n_4_hepc,
                  n_casette,
                  n_note_end2,
                  odata_cassette,
                  uuid_cassette
               ),
            by = join_by(sq_1_coupon_number_001 == sq_1_rid)
         ) %>%
         mutate(
            odata_signature = odata_context,
            uuid_signature  = id,
         )

   if (survey == "pwid_m")
      data %<>%
         left_join(
            y  = medtech %>%
               select(
                  sq_1_rid,
                  sq_1_rid_001,
                  int_2_name_interviewer,
                  int_ihbss_site,
                  int_type_of_kp,
                  sq_1_rid,
                  sq_1_rid_001,
                  n_1_hiv,
                  n_2_syph,
                  n_3_hepb,
                  n_3_hepb_001,
                  n_4_hepc,
                  n_casette,
                  n_note_end2,
                  odata_cassette,
                  uuid_cassette
               ),
            by = join_by(sq_sq1_1_rid == sq_1_rid)
         ) %>%
         mutate(
            odata_signature = odata_context,
            uuid_signature  = id,
         ) %>%
         mutate(
            sq1_rid = sq_sq1_1_rid
         )

   if (survey == "pwid_f")
      data %<>%
         left_join(
            y  = medtech %>%
               select(
                  sq_1_rid,
                  sq_1_rid_001,
                  int_2_name_interviewer,
                  int_ihbss_site,
                  int_type_of_kp,
                  sq_1_rid,
                  sq_1_rid_001,
                  n_1_hiv,
                  n_2_syph,
                  n_3_hepb,
                  n_3_hepb_001,
                  n_4_hepc,
                  n_casette,
                  n_note_end2,
                  odata_cassette,
                  uuid_cassette
               ),
            by = join_by(sq_1_coupon_number == sq_1_rid)
         ) %>%
         mutate(
            odata_signature = odata_context,
            uuid_signature  = id,
         ) %>%
         mutate(
            sq1_rid = sq_1_coupon_number
         )

   if (survey == "msm")
      data %<>%
         mutate(
            odata_cassette  = odata_context,
            odata_signature = odata_context,
            uuid_cassette   = id,
            uuid_signature  = id
         )

   data %<>%
      rename_all(
         ~stri_replace_first_regex(., "_([0-9])", "$1")
      ) %>%
      rename_all(
         ~case_when(
            survey == "msm" & . == "sq1_rid" ~ "sq1_rid",
            survey == "fsw" & . == "sq1_coupon_number_001" ~ "sq1_rid",
            survey == "medtech" & . == "int_ihbss_site" ~ "int1_ihbss_site",
            TRUE ~ .
         )
      ) %>%
      mutate(
         .after       = int1_ihbss_site,
         site_decoded = case_when(
            int1_ihbss_site == "4_NCR" ~ "100",
            int1_ihbss_site == "ncr" ~ "100",
            int1_ihbss_site == "manila" ~ "100",
            int1_ihbss_site == "davao" ~ "106",
            int1_ihbss_site == "angeles" ~ "101",
            int1_ihbss_site == "angeles" ~ "101",
            TRUE ~ as.character(int1_ihbss_site)
         ),
         site_decoded = labelled(
            as.integer(site_decoded),
            c(
               "National Capital Region" = 100,
               "Angeles City"            = 101,
               "Baguio City"             = 102,
               "Cagayan de Oro City"     = 104,
               "Cebu City"               = 105,
               "Davao City"              = 106,
               "General Santos City"     = 107,
               "Iloilo City"             = 108,
               "Puerto Princesa City"    = 111,
               "Tuguegarao City"         = 113,
               "Zamboanga City"          = 114,
               "Batangas City"           = 117,
               "Bacolod City"            = 122,
               "Bacoor City"             = 123,
               "Naga City"               = 301,
               "Dasmariñas City"         = 300
            )
         ),
         # site_decoded = case_when(
         #    int1_ihbss_site == "4_NCR" ~ "National Capital Region",
         #    int1_ihbss_site == "100" ~ "National Capital Region",
         #    int1_ihbss_site == "101" ~ "Angeles City",
         #    int1_ihbss_site == "102" ~ "Baguio City",
         #    int1_ihbss_site == "104" ~ "Cagayan de Oro City",
         #    int1_ihbss_site == "105" ~ "Cebu City",
         #    int1_ihbss_site == "106" ~ "Davao City",
         #    int1_ihbss_site == "107" ~ "General Santos City",
         #    int1_ihbss_site == "108" ~ "Iloilo City",
         #    int1_ihbss_site == "111" ~ "Puerto Princesa City",
         #    int1_ihbss_site == "113" ~ "Tuguegarao City",
         #    int1_ihbss_site == "114" ~ "Zamboanga City",
         #    int1_ihbss_site == "117" ~ "Batangas City",
         #    int1_ihbss_site == "122" ~ "Bacolod City",
         #    int1_ihbss_site == "123" ~ "Bacoor City",
         #    int1_ihbss_site == "301" ~ "Naga City",
         #    int1_ihbss_site == "300" ~ "Dasmariñas City",
         #    TRUE ~ as.character(int1_ihbss_site)
         # ),
         City         = site_decoded
      )

   if (survey == "medtech") {
      data %<>%
         mutate(
            Language       = "English",
            City           = Context,
            odata_cassette = odata_context,
            uuid_cassette  = id,
         )
   } else {
      data %<>%
         rowwise() %>%
         mutate(
            .after = sq1_rid,
            cn     = str_squish(strsplit(sq1_rid, "\\-")[[1]][3]),
         ) %>%
         ungroup() %>%
         relocate(n_casette, sq2_current_age, .after = sq1_rid) %>%
         mutate(
            .after          = cn,
            cn              = if_else(!is.na(cn), cn, "", ""),
            c1              = paste0(cn, "1"),
            c2              = paste0(cn, "2"),
            c3              = paste0(cn, "3"),
            recruiter       = StrLeft(cn, nchar(cn) - 1),
            sq2_current_age = floor(sq2_current_age),
            Age_Band        = case_when(
               sq2_current_age < 15 ~ "<15>",
               sq2_current_age %in% seq(15, 17) ~ "15-17",
               sq2_current_age %in% seq(18, 24) ~ "18-24",
               sq2_current_age %in% seq(25, 35) ~ "25-35",
               sq2_current_age > 35 ~ "35+",
               TRUE ~ "(no data)"
            ),
         ) %>%
         # time start
         mutate(
            .after     = start,
            start_date = format(start, "%Y-%m-%d"),
            start_time = format(start, "%H:%M:%S"),
         ) %>%
         # time end
         mutate(
            .after   = end,
            end_date = format(end, "%Y-%m-%d"),
            end_time = format(end, "%H:%M:%S"),
         )
   }

   data %<>%
      # edit link
      mutate(
         .after   = 1,
         link_odk = stri_replace_all_fixed(odata_context, "$metadata#Submissions", ""),
         link_odk = stri_replace_all_fixed(link_odk, ".svc", ""),
         link_odk = stri_c(link_odk, "submissions/", urltools::url_encode(id), "/edit"),
         link_odk = gs4_formula(glue(r"(=HYPERLINK("{link_odk}", "Edit in ODK"))")),
      )

   return(data)
}

##  Flag coupon recruiter ------------------------------------------------------

ihbss_recruiter <- function(data) {
   # generate a list of control numbers as  "recruiters"
   recruiters <- data %>%
      mutate(
         recruiter_exist = "Y"
      ) %>%
      select(
         City,
         recruiter = cn,
         recruiter_exist
      ) %>%
      distinct_all()

   # list of specific coupons that are identified as "seeds"
   seeds <- c(
      "M-100-1",
      "M-100-2",
      "M-100-3",
      "M-100-4",
      "M-100-5",
      "M-100-6",
      "M-100-7",
      "M-100-8",
      "M-100-9",
      "M-100-14",
      "M-100-15",
      "M-100-16",
      "M-100-17",
      "M-100-18",
      "M-100-19",
      "M-100-20"
   )

   # tag if the recruiter has previously been reported in the survey
   data %<>%
      left_join(
         y  = recruiters,
         by = join_by(City, recruiter)
      ) %>%
      mutate(
         recruiter_exist = case_when(
            sq1_rid %in% seeds ~ "SEED",
            StrLeft(StrRight(sq1_rid, 2), 1) == "-" ~ "SEED",
            recruiter == "" ~ "N",
            recruiter_exist == "Y" ~ "Y",
            TRUE ~ "N"
         )
      ) %>%
      relocate(recruiter_exist, .after = recruiter) %>%
      distinct(id, .keep_all = TRUE)

   return(data)
}

##  Download ODK attachments ---------------------------------------------------

ihbss_odk_attachments <- function(data, survey) {
   survey <- toupper(survey)
   log_info("Downloading attachments for the {green(survey)} survey.")
   # get list of files that are already uploaded
   drive_curr <- Sys.getenv("IHBSS_2022_LOCAL") %>%
      dir_info(recurse = TRUE) %>%
      mutate(
         filename    = basename(path),
         submit_type = case_when(
            type == "file" & str_detect(path, "signature") ~ "signature",
            type == "file" & str_detect(path, "cassette") ~ "cassette"
         ),
         survey_city = stri_replace_first_fixed(path, stri_c(Sys.getenv("IHBSS_2022_LOCAL"), "/"), ""),
         survey_city = substr(survey_city, 1, stri_locate_first_fixed(survey_city, "/") - 1),
         .after      = path
      ) %>%
      separate_wider_delim(
         survey_city, " - ",
         names   = c("Survey", "City"),
         too_few = "align_start"
      )

   if (survey != "MEDTECH") {
      # get a list of signatures that have yet to be downloaded
      dl_signatures <- data %>%
         rename(filename = consent_consent_sign) %>%
         filter(!is.na(filename)) %>%
         anti_join(
            y  = drive_curr %>%
               filter(
                  submit_type == "signature"
               ) %>%
               select(
                  Survey,
                  City,
                  filename
               ),
            by = join_by(Survey, City, filename)
         ) %>%
         mutate(
            upload_path = file.path(Sys.getenv("IHBSS_2022_LOCAL"), stri_c(Survey, " - ", City), "signature", filename)
         )

      if (nrow(dl_signatures) > 0) {
         invisible(lapply(unique(path_dir(dl_signatures$upload_path)), check_dir))
         pb <- progress_bar$new(format = "Signatures | :current of :total | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(dl_signatures), width = 100, clear = FALSE)
         pb$tick(0)
         apply(dl_signatures, 1, function(row, pb) {
            row   <- as.list(row)
            odata <- row$odata_signature
            uuid  <- row$uuid_signature
            dir   <- path_dir(row$upload_path)

            ruODK::ru_setup(
               svc     = stri_replace_all_fixed(odata, "$metadata#Submissions", ""),
               tz      = "Asia/Hong_Kong",
               verbose = FALSE
            )
            attachment_get(uuid, row$filename, dir)

            pb$tick(1)
         }, pb)
      }
   }

   # get a list of cassette that have yet to be downloaded
   dl_cassette <- data %>%
      rename(filename = n_casette) %>%
      filter(!is.na(filename)) %>%
      anti_join(
         y  = drive_curr %>%
            filter(
               submit_type == "cassette"
            ) %>%
            select(
               Survey,
               City,
               filename
            ),
         by = join_by(Survey, City, filename)
      ) %>%
      mutate(
         upload_path = file.path(Sys.getenv("IHBSS_2022_LOCAL"), stri_c(Survey, " - ", City), "cassette", filename)
      )

   if (nrow(dl_cassette) > 0) {
      invisible(lapply(unique(path_dir(dl_cassette$upload_path)), check_dir))
      pb <- progress_bar$new(format = "Cassettes | :current of :total | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(dl_cassette), width = 100, clear = FALSE)
      pb$tick(0)
      apply(dl_cassette, 1, function(row, pb) {
         row   <- as.list(row)
         odata <- row$odata_cassette
         uuid  <- row$uuid_cassette
         dir   <- path_dir(row$upload_path)

         ruODK::ru_setup(
            svc     = stri_replace_all_fixed(odata, "$metadata#Submissions", ""),
            tz      = "Asia/Hong_Kong",
            verbose = FALSE
         )
         attachment_get(uuid, row$filename, dir)

         pb$tick(1)
      }, pb)
   }
}

##  Generate hyperlinks for the cassettes uploaded to drive --------------------

ihbss_drive_cassettes <- function(data, survey, ss) {
   survey <- toupper(survey)

   log_info("Getting city sub-directories.")
   drive_cities <- drive_ls(as_id(ss), pattern = survey)

   log_info("Getting cassette sub-directories.")
   drive_cassette <- apply(drive_cities, 1, function(row) {
      row     <- as.list(row)
      ss      <- row$id
      context <- row$name
      cst     <- drive_ls(as_id(ss), pattern = "cassette") %>%
         mutate(
            context = context,
            .before = 1
         )

      return(cst)
   })
   drive_cassette <- bind_rows(drive_cassette)

   log_info("Getting list of uploaded cassette files.")
   drive_files <- apply(drive_cassette, 1, function(row) {
      row     <- as.list(row)
      ss      <- row$id
      context <- row$context
      cst     <- drive_ls(as_id(ss)) %>%
         mutate(
            context = context,
            .before = 1
         ) %>%
         select(-id, -name) %>%
         unnest_wider(drive_resource)

      return(cst)
   })
   drive_files <- bind_rows(drive_files) %>%
      rename(
         n_casette     = name,
         link_cassette = webViewLink
      ) %>%
      mutate(
         Survey = survey,
         City   = stri_replace_all_fixed(context, stri_c(survey, " - "), "")
      )

   log_info("Attaching to the dataset.")
   data %<>%
      left_join(
         y  = drive_files %>%
            select(
               Survey,
               City,
               n_casette,
               link_cassette
            ),
         by = join_by(Survey, City, n_casette)
      ) %>%
      relocate(link_cassette, .after = link_odk) %>%
      mutate(
         link_cassette = gs4_formula(if_else(is.na(link_cassette), NA_character_, glue(r"(=HYPERLINK("{link_cassette}", "{n_casette}"))"))),
      )

   log_success("Done.")
   return(data)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), survey) {
   survey_config <- envir$odk$config[[survey]]
   survey_data   <- ihbss_odk_submissions(survey_config, survey)

   # rename per survey
   if (survey %in% c("fsw", "pwid_m", "pwid_f")) {
      if (is.null(envir$odk$data$medtech))
         envir$odk$data$medtech <- ihbss_odk_submissions(envir$odk$config$medtech, "medtech")

      survey_data %<>%
         ihbss_rename(survey, envir$odk$data$medtech) %>%
         relocate(Survey, City, Language, sq1_rid, review_state, .before = 1)
   } else {
      survey_data %<>%
         ihbss_rename(survey) %>%
         relocate(Survey, City, Language, sq1_rid, review_state, .before = 1)
   }

   survey_data <- switch(
      survey,
      msm     = ihbss_recruiter(survey_data),
      fsw     = ihbss_recruiter(survey_data),
      pwid_m  = ihbss_recruiter(survey_data),
      pwid_f  = ihbss_recruiter(survey_data),
      medtech = survey_data
   )

   # download attachments
   ihbss_odk_attachments(survey_data, survey)
   envir$odk$data[[survey]] <- ihbss_drive_cassettes(survey_data, survey, gdrive$submissions)
}
