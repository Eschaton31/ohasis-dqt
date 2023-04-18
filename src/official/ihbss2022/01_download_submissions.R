##  MSM ------------------------------------------------------------------------
local(envir = ihbss$`2022`, {
   odk$data$msm      <- list()
   odk$data$msm$data <- apply(odk$config$msm, 1, function(config) {
      config <- as.list(config)
      form   <- paste(sep = " - ", config$City, config$Language)

      ruODK::ru_setup(svc = config$OData, tz = "Asia/Hong_Kong", verbose = FALSE)

      # check_dir(file.path(Sys.getenv("IHBSS_2022_LOCAL"), form))
      data <- data.frame()
      tryCatch({
         log_info("Downloading data for {green(form)}.")
         data <- odata_submission_get(download = FALSE) %>%
            mutate(
               City     = config$City,
               Language = config$Language,
            )
      },
         error = function(e) .log_warn("No data yet for {red(form)}.")
      )
      return(data)
   })
   odk$data$msm$subs <- apply(odk$config$msm, 1, function(config) {
      config <- as.list(config)
      form   <- paste(sep = " - ", config$City, config$Language)

      ruODK::ru_setup(svc = config$OData, tz = "Asia/Hong_Kong", verbose = FALSE)

      # check_dir(file.path(Sys.getenv("IHBSS_2022_LOCAL"), form))
      subs <- data.frame()
      tryCatch({
         log_info("Downloading submission list for {green(form)}.")
         subs <- ruODK::submission_list()
      },
         error = function(e) .log_warn("No submissions yet for {red(form)}.")
      )
      return(subs)
   })

   odk$data$msm <- bind_rows(odk$data$msm$data) %>%
      left_join(
         y = bind_rows(odk$data$msm$subs) %>%
            select(
               id               = instance_id,
               submitter_name   = submitter_display_name,
               submitter_device = device_id,
               review_state
            )
      ) %>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(.)
      ) %>%
      ihbss_rename("msm") %>%
      relocate(City, Language, sq1_rid, review_state, .before = 1)
})

# tag recruiter
local(envir = ihbss$`2022`, {
   odk$data$msm %<>%
      left_join(
         y  = odk$data$msm %>%
            mutate(
               recruiter_exist = "Y"
            ) %>%
            select(
               City,
               recruiter = cn,
               recruiter_exist
            ),
         by = join_by(City, recruiter)
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
            StrLeft(StrRight(sq1_rid, 2), 1) == "-" ~ "SEED",
            recruiter_exist == "Y" ~ "Y",
            TRUE ~ "N"
         )
      ) %>%
      relocate(recruiter_exist, .after = recruiter) %>%
      distinct(id, .keep_all = TRUE)
})

# download files
local(envir = ihbss$`2022`, {
   invisible(lapply(unique(odk$data$msm$path_dta), check_dir))
   invisible(lapply(unique(odk$data$msm$path_sig), check_dir))
   invisible(lapply(unique(odk$data$msm$path_cst), check_dir))
   invisible(lapply(unique(odk$data$msm$City), function(city, data) {
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
   }, odk$data$msm))
})

local(envir = ihbss$`2022`, {
   log_info("Getting GDrive City IDs.")
   drive_cst <- drive_ls(as_id(gdrive$submissions), pattern = "MSM")
   drive_cst <- apply(drive_cst, 1, function(row) {
      ss      <- row$id
      context <- row$name
      cst     <- drive_ls(as_id(ss), pattern = "cassette") %>%
         mutate(
            context = context,
            .before = 1
         )

      return(cst)
   })
   drive_cst <- bind_rows(drive_cst)

   log_info("Getting GDrive City Cassettes.")
   path_cst <- apply(drive_cst, 1, function(row) {
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
   path_cst <- bind_rows(path_cst) %>%
      rename(
         n_casette = name,
         link_cst  = webViewLink
      ) %>%
      mutate(City = stri_replace_all_fixed(context, "MSM - ", ""))

   conso$msm$data <- odk$data$msm %>%
      left_join(
         y  = path_cst %>%
            select(
               City,
               n_casette,
               link_cst
            ),
         by = join_by(City, n_casette)
      ) %>%
      relocate(link_cst, .after = n_casette) %>%
      mutate(
         link_cst = gs4_formula(glue(r"(=HYPERLINK("{link_cst}", "{n_casette}"))")),
      )

   rm(drive_cst, path_cst)
})
