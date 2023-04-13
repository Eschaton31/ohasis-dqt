##  Download SACCL Results from Dropbox ----------------------------------------

# check if directories exist
.log_info("Checking drive/cloud directories.")

get_files_list <- function() {
   dir_output         <- file.path("data", ohasis$ym, "harp_dx", "confirmatory")
   dir_cloud_base     <- glue("{Sys.getenv('DRIVE_CLOUD')}/HARP Cloud/HARP Forms/Confirmatory")
   dir_cloud_report   <- glue("{dir_cloud_base}/{ohasis$ym}")
   dir_dropbox_report <- glue("{Sys.getenv('DRIVE_DROPBOX')}/File requests/SACCL Submissions/{ohasis$ym}/")
   check_dir(dir_output)

   # check if monthly report folder exists
   check_dir(dir_cloud_report)
   # if (!(glue("{ohasis$ym}/") %in% fs::dir_ls(dir_cloud_base)))
   #    create_folder(glue("{dir_cloud_base}/{ohasis$ym}"))

   # get list of files already in nextcloud & gdrive
   .log_info("Getting list of uploaded files.")
   pdf_output    <- dir_info(dir_output, glob = "*.pdf")
   pdf_nextcloud <- dir_info(dir_cloud_report, glob = "*.pdf")
   pdf_dropbox   <- dir_info(dir_dropbox_report, recurse = TRUE, glob = "*.pdf")

   # check with corrections list which files are not yet processed
   .log_info("Excluding files that were already processed before.")
   if ("pdf_results" %in% names(nhsss$harp_dx$corr)) {
      pdf_for_dl <- pdf_dropbox %>%
         left_join(
            y  = nhsss$harp_dx$corr$pdf_results,
            by = c("name" = "FILENAME_PDF")
         ) %>%
         mutate(
            file = if_else(
               condition = !is.na(LABCODE),
               true      = glue("{LABCODE}.pdf"),
               false     = NA_character_
            )
         ) %>%
         mutate(file = basename(path))
   } else {
      pdf_for_dl <- pdf_dropbox %>%
         mutate(file = basename(path))
   }

   if ("pdf_results" %in% names(nhsss$harp_dx$corr))
      pdf_for_dl %<>%
         anti_join(
            y  = pdf_nextcloud %>%
               mutate(file = basename(path)) %>%
               select(file),
            by = "file"
         )


   pdf_for_dl %<>%
      anti_join(
         y  = pdf_output %>%
            mutate(file = basename(path)) %>%
            select(file),
         by = "file"
      )

   return(pdf_for_dl)
}

download_results <- function(pdf_for_dl, dir_output) {
   # parallelize downloading of pdf files from Dropbox
   if (nrow(pdf_for_dl) > 0) {
      .log_info("Downloading SACCL PDF results.")
      pb <- progress_bar$new(format = ":current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(pdf_for_dl), width = 100, clear = FALSE)
      pb$tick(0)
      invisible(
         lapply(pdf_for_dl$path, function(file_old) {
            file_new <- file.path(dir_output, basename(file_old))

            if (file_new != file_old)
               file_copy(file_old, file_new, overwrite = TRUE)

            pb$tick(1)
         })
      )
   }
}

##  Read data from the pdf file ------------------------------------------------

read_pdf <- function() {
   file       <- input("Kindly provide the UNIX path to the SACCL PDF Logsheet.")
   lst        <- tabulizer::extract_tables(file = file, method = "lattice")
   confirm_df <- lapply(lst, function(data) {
      data %<>%
         as_tibble() %>%
         slice(-1, -2) %>%
         select(
            SPECIMEN_RECEIPT_DATE = V3,
            LABCODE               = V4,
            FULLNAME              = V5,
            BDATE                 = V6,
            AGE                   = V7,
            SEX                   = V8,
            SOURCE                = V9,
            RAPID                 = V10,
            SYSMEX                = V11,
            VIDAS                 = V14,
            GEENIUS               = V15,
            REMARKS               = V16,
            DATE_CONFIRM          = V17
         )

      return(data)
   })
   confirm_df %<>%
      bind_rows() %>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(.)
      ) %>%
      mutate_at(
         .vars = vars(
            LABCODE,
            FULLNAME,
            SOURCE,
            RAPID,
            SYSMEX,
            VIDAS,
            GEENIUS
         ),
         ~toupper(.)
      ) %>%
      mutate(
         T1_KIT                = "SYSMEX",
         T1_RESULT             = as.numeric(SYSMEX),
         T1_RESULT             = case_when(
            SYSMEX == ">100.000" ~ "10",
            T1_RESULT >= 1 ~ "10",
            T1_RESULT < 1 ~ "20",
            TRUE ~ "  "
         ),

         T2_KIT                = "VIDAS",
         T2_RESULT             = case_when(
            VIDAS == "REACTIVE" ~ "10",
            VIDAS == "NONREACTIVE" ~ "20",
            TRUE ~ "  "
         ),

         T3_KIT                = case_when(
            RAPID != "" ~ "STAT-PAK",
            GEENIUS != "" ~ "GEENIUS",
         ),
         T3_RESULT             = case_when(
            RAPID == "REACTIVE" ~ "10",
            RAPID == "NONREACTIVE" ~ "20",
            GEENIUS == "POSITIVE" ~ "10",
            GEENIUS == "NEGATIVE" ~ "20",
            GEENIUS == "INDETERMINATE" ~ "30",
            TRUE ~ "  "
         ),
         FINAL_INTERPRETATION  = stri_c(T1_RESULT, T2_RESULT, T3_RESULT),
         FINAL_INTERPRETATION  = case_when(
            FINAL_INTERPRETATION == "101010" ~ "Positive",
            FINAL_INTERPRETATION == "202020" ~ "Negative",
            FINAL_INTERPRETATION == "2020  " ~ "Negative",
            FINAL_INTERPRETATION == "20    " ~ "Negative",
            grepl("30", FINAL_INTERPRETATION) ~ "Indeterminate",
            grepl("20", FINAL_INTERPRETATION) ~ "Indeterminate",
            grepl("^SAME AS", REMARKS) ~ "Duplicate"
         ),

         SPECIMEN_RECEIPT_DATE = as.Date(SPECIMEN_RECEIPT_DATE, "%m/%d/%y"),
         DATE_CONFIRM          = as.Date(DATE_CONFIRM, "%m/%d/%y"),
         BDATE                 = as.Date(BDATE, "%m/%d/%Y"),

         EXIST_LOGSHEET        = 1
      ) %>%
      filter(SOURCE != "JAY DUMMY LAB")

   return(confirm_df)
}

##  Perform cleaning on the consolidated df ------------------------------------

clean_data <- function(confirm_df, pdf_results) {
   db_conn    <- ohasis$conn("db")
   px_confirm <- dbTable(db_conn, "ohasis_interim", "px_confirm", "CONFIRM_CODE", where = "CONFIRM_TYPE = 1", raw_where = TRUE)
   dbDisconnect(db_conn)

   confirm_df %<>%
      mutate(
         # Remove extra text from Duplicates forms
         FULLNAME = stri_replace_all_fixed(FULLNAME, "ALREADY HAS A PREVIOUS", ""),
         FULLNAME = stri_replace_all_fixed(FULLNAME, "ALREADY HAS A", ""),
         FULLNAME = stri_replace_all_fixed(FULLNAME, "ALREADY HAS", ""),
         FULLNAME = stri_replace_all_fixed(FULLNAME, "ALREADY", ""),
         FULLNAME = str_squish(FULLNAME),
      ) %>%
      full_join(
         y  = pdf_results %>%
            mutate(
               FILENAME  = basename(path),
               LABCODE   = FILENAME %>%
                  stri_replace_all_fixed(".pdf", "") %>%
                  substr(1, 12),
               EXIST_PDF = 1
            ) %>%
            select(LABCODE, EXIST_PDF, FILENAME),
         by = join_by(LABCODE)
      ) %>%
      left_join(
         y  = px_confirm %>%
            mutate(
               LABCODE  = CONFIRM_CODE,
               EXIST_OH = 1
            ) %>%
            select(LABCODE, EXIST_OH),
         by = join_by(LABCODE)
      )

   return(confirm_df)
}


##  Add OHASIS Conversions -----------------------------------------------------

match_ohasis <- function(confirm_df) {
   .log_info("Matching with OHASIS variables.")
   var_pairs <- ""
   for (var in names(nhsss$harp_dx$corr$pdf_saccl)) {
      if (var %in% names(confirm_df)) {
         confirm_df %<>%
            left_join(
               y  = nhsss$harp_dx$corr$pdf_saccl[[var]],
               by = var
            )

         key            <- names(nhsss$harp_dx$corr$pdf_saccl[[var]])[1]
         val            <- names(nhsss$harp_dx$corr$pdf_saccl[[var]])[2]
         var_pairs[key] <- val
      }
   }
   var_pairs                                       <- var_pairs[-1]
   nhsss$harp_dx$steps$y1_logsheet_saccl$var_pairs <- var_pairs

   return(confirm_df)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(confirm_df) {
   update <- input(
      prompt  = "Run `pdf_saccl` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)

   # TODO: add matching with ml
   check <- list()
   if (update == "1") {
      # check ohasis variables
      for (i in seq_len(length(var_pairs))) {
         saccl_var  <- names(var_pairs)[i] %>% as.symbol()
         ohasis_var <- var_pairs[i] %>% as.symbol()

         check[[saccl_var]] <- confirm_df %>%
            filter(
               !is.na(!!saccl_var),
               is.na(!!ohasis_var) | !!ohasis_var == "NULL"
            ) %>%
            distinct(
               !!saccl_var,
               !!ohasis_var
            )
      }
   }

   return(check)
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$harp_dx, "pdf_saccl", ohasis$ym)

##  Upload renamed confirmatories to Nextcloud ---------------------------------

upload_pdf <- function(confirm_df) {
   .log_info("Uploading renamed results to cloud.")
   dir_output <- file.path("data", ohasis$ym, "harp_dx", "confirmatory")
   dir_nc     <- file.path("N:/HARP Cloud/HARP Forms/Confirmatory/", ohasis$ym)
   pdf_for_ul <- confirm_df
   if ("pdf_results" %in% names(nhsss$harp_dx$corr))
      pdf_for_ul %<>%
         filter(
            !(FILENAME %in% list.files(dir_output))
         )

   check_dir(dir_nc)
   if (nrow(pdf_for_ul)) {
      pb <- progress_bar$new(format = ":current of :total PDFs [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(pdf_for_ul), width = 100, clear = FALSE)
      pb$tick(0)
      for (i in seq_len(nrow(confirm_df))) {
         file_old <- confirm_df[i, "FILENAME"] %>% as.character()
         file_new <- confirm_df[i, "LABCODE"] %>% as.character()

         if (file.exists(file_old) && !is.na(file_new)) {
            file_old <- file.path(dir_output, file_old)
            file_new <- file.path(dir_nc, glue("{file_new}.pdf"))

            if (file_new != file_old)
               file_copy(file_old, file_new, overwrite = TRUE)

            # upload_file(file_new, dir_cloud_report)

            # unlink(file_new)
         }
         pb$tick(1)
      }
   }
   nhsss$harp_dx$pdf_saccl$data <- confirm_df
}

.init <- function() {

   p <- parent.env(environment())
   local(envir = p, {
      pdf_for_dl <- get_files_list()
      dir_output <- file.path("data", ohasis$ym, "harp_dx", "confirmatory")
      download_results(pdf_for_dl, dir_output)
      pdf_results <- dir_info(dir_output, recurse = TRUE, glob = "*.pdf")

      confirm_pdf      <- read_pdf()
      confirm_pdf_orig <- confirm_pdf
      confirm_pdf      <- clean_data(confirm_pdf, pdf_results) %>%
         match_ohasis()

      check <- get_checks(confirm_pdf)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "logsheet_saccl", ohasis$ym))
}