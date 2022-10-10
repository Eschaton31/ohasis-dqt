# download submissions
ihbss$`2022`$odk$submissions <- list()
ihbss$`2022`$odk$data        <- list()
for (i in seq_len(nrow(ihbss$`2022`$odk$config))) {
   config <- ihbss$`2022`$odk$config[i,]
   form   <- paste(sep = " - ", config$City, config$Language)

   ruODK::ru_setup(svc = config$OData, tz = "Asia/Hong_Kong", verbose = FALSE)

   # check_dir(file.path(Sys.getenv("IHBSS_2022_LOCAL"), form))
   tryCatch({
      ihbss$`2022`$odk$submissions[[form]] <- ruODK::submission_list()
      .log_info("Downloading data for {green(form)}.")
      ihbss$`2022`$odk$data[[form]] <- odata_submission_get(download = FALSE) %>%
         mutate(
            City     = config$City,
            Language = config$Language,
         )
   },
      error = function(e) .log_warn("No submissions yet for {red(form)}.")
   )
}

# check directories
subs <- bind_rows(ihbss$`2022`$odk$submissions) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.)
   )
data <- bind_rows(ihbss$`2022`$odk$data) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.)
   ) %>%
   left_join(
      y = subs %>%
         select(
            id               = instance_id,
            submitter_name   = submitter_display_name,
            submitter_device = device_id,
            review_state
         )
   ) %>%
   rename_all(
      ~stri_replace_first_regex(., "_([0-9])", "$1")
   ) %>%
   relocate(City, Language, sq1_rid, review_state, .before = 1) %>%
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
   mutate(
      .after       = int1_ihbss_site,
      site_decoded = case_when(
         int1_ihbss_site == "100" ~ "National Capital Region",
         int1_ihbss_site == "101" ~ "Angeles City",
         int1_ihbss_site == "102" ~ "Baguio City",
         int1_ihbss_site == "104" ~ "Cagayan de Oro City",
         int1_ihbss_site == "105" ~ "Cebu City",
         int1_ihbss_site == "106" ~ "Davao City",
         int1_ihbss_site == "107" ~ "General Santos City",
         int1_ihbss_site == "108" ~ "Iloilo City",
         int1_ihbss_site == "111" ~ "Puerto Princesa City",
         int1_ihbss_site == "113" ~ "Tuguegarao City",
         int1_ihbss_site == "114" ~ "Zamboanga City",
         int1_ihbss_site == "117" ~ "Batangas City",
         int1_ihbss_site == "122" ~ "Bacolod City",
         int1_ihbss_site == "123" ~ "Bacoor City",
         int1_ihbss_site == "301" ~ "Naga City",
         int1_ihbss_site == "300" ~ "Dasmariñas City",
         TRUE ~ as.character(int1_ihbss_site)
      )
   ) %>%
   mutate(
      path_dta = file.path(Sys.getenv("IHBSS_2022_LOCAL"), City, "data"),
      path_sig = file.path(Sys.getenv("IHBSS_2022_LOCAL"), City, "signature"),
      path_cst = file.path(Sys.getenv("IHBSS_2022_LOCAL"), City, "cassette"),
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

data %<>%
   left_join(
      y  = data %>%
         mutate(
            recruiter_exist = "Y"
         ) %>%
         select(
            City,
            recruiter = cn,
            recruiter_exist
         ),
      by = c("City", "recruiter")
   ) %>%
   mutate(
      recruiter_exist = case_when(
         sq1_rid == "M-100-1" ~ "SEED",
         sq1_rid == "M-100-2" ~ "SEED",
         sq1_rid == "M-100-3" ~ "SEED",
         sq1_rid == "M-100-4" ~ "SEED",
         sq1_rid == "M-100-5" ~ "SEED",
         sq1_rid == "M-100-6" ~ "SEED",
         sq1_rid == "M-100-7" ~ "SEED",
         sq1_rid == "M-100-8" ~ "SEED",
         sq1_rid == "M-100-9" ~ "SEED",
         sq1_rid == "M-100-14" ~ "SEED",
         sq1_rid == "M-100-15" ~ "SEED",
         sq1_rid == "M-100-16" ~ "SEED",
         sq1_rid == "M-100-17" ~ "SEED",
         sq1_rid == "M-100-18" ~ "SEED",
         sq1_rid == "M-100-19" ~ "SEED",
         sq1_rid == "M-100-20" ~ "SEED",
         StrRight(recruiter, 1) == "-" ~ "SEED",
         recruiter_exist == "Y" ~ "Y",
         TRUE ~ "N"
      )
   ) %>%
   relocate(recruiter_exist, .after = recruiter) %>%
   distinct(id, .keep_all = TRUE)

invisible(lapply(unique(data$path_dta), check_dir))
invisible(lapply(unique(data$path_sig), check_dir))
invisible(lapply(unique(data$path_cst), check_dir))
invisible(lapply(unique(data$City), function(city) {
   # cassette
   cst_data <- filter(data, City == city)
   cst_done <- list.files(unique(cst_data$path_cst))
   cst_data <- cst_data %>% filter(!(n_casette %in% cst_done))
   if (nrow(cst_data) > 0) {
      .log_info("Downloading cassettes for {green(city)}.")
      for (locale in unique(cst_data$Language)) {
         cst_locale <- cst_data %>% filter(Language == locale)
         ruODK::ru_setup(
            svc     = stri_replace_all_fixed(cst_locale[1,]$odata_context, "$metadata#Submissions", ""),
            tz      = "Asia/Hong_Kong",
            verbose = FALSE
         )

         pb <- progress_bar$new(format = paste0(locale, " | :current of :total cassette files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed"), total = nrow(cst_locale), width = 100, clear = FALSE)
         pb$tick(0)
         for (i in seq_len(nrow(cst_locale))) {
            attachment_get(
               cst_locale[i,]$id,
               cst_locale[i,]$n_casette,
               cst_locale[i,]$path_cst
            )
            pb$tick(1)
         }
      }
   }

   # signatures
   sig_data <- filter(data, City == city)
   sig_done <- list.files(unique(sig_data$path_sig))
   sig_data <- sig_data %>% filter(!(consent_consent_sign %in% sig_done))
   if (nrow(sig_data) > 0) {
      .log_info("Downloading signatures for {green(city)}.")
      for (locale in unique(sig_data$Language)) {
         sig_locale <- sig_data %>% filter(Language == locale)
         ruODK::ru_setup(
            svc     = stri_replace_all_fixed(sig_locale[1,]$odata_context, "$metadata#Submissions", ""),
            tz      = "Asia/Hong_Kong",
            verbose = FALSE
         )

         pb <- progress_bar$new(format = paste0(locale, " | :current of :total signature files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed"), total = nrow(sig_locale), width = 100, clear = FALSE)
         pb$tick(0)
         for (i in seq_len(nrow(sig_locale))) {
            attachment_get(
               sig_locale[i,]$id,
               sig_locale[i,]$consent_consent_sign,
               sig_locale[i,]$path_sig
            )
            pb$tick(1)
         }
      }
   }
}))

.log_info("Getting GDrive City IDs.")
drive_cst <- data %>%
   distinct(City) %>%
   mutate(
      drive_cst = paste0("~/DQT/Data Factory/IHBSS - 2022/Submissions/", City, "/cassette/") %>% drive_get(),
   ) %>%
   rowwise() %>%
   mutate(
      drive_id = drive_cst$id
   ) %>%
   ungroup()

.log_info("Getting GDrive City Cassettes.")
path_cst <- data.frame()
for (i in seq_len(nrow(drive_cst))) {
   cst      <- drive_ls(as_id(drive_cst[i,]$drive_id)) %>%
      mutate(City = drive_cst[i,]$City)
   path_cst <- bind_rows(path_cst, cst)
}
path_cst %<>%
   rowwise() %>%
   mutate(
      link_cst = drive_resource$webViewLink
   ) %>%
   ungroup() %>%
   rename(
      n_casette = name
   )

data %<>%
   left_join(
      y  = path_cst %>%
         select(
            City,
            n_casette,
            link_cst
         ),
      by = c("City", "n_casette")
   ) %>%
   relocate(link_cst, .after = n_casette) %>%
   mutate(
      link_cst = gs4_formula(glue(r"(=HYPERLINK("{link_cst}", "{n_casette}"))"))
   )

ihbss$`2022`$conso$initial$data <- data
rm(config, data, form, i, subs, cst, drive_cst, path_cst)