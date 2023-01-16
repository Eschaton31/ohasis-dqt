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
      # plan(multisession)
      # invisible(
      #    future_map_chr(pdf_for_dl$path_display, function(file) {
      #       drop_download(
      #          file,
      #          dir_output,
      #          overwrite = TRUE
      #       )
      #    })
      # )
      # plan(sequential)
      pb <- progress_bar$new(format = ":current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(pdf_for_dl), width = 100, clear = FALSE)
      pb$tick(0)
      invisible(
         lapply(pdf_for_dl$path, function(file_old) {
            # drop_download(
            #    file,
            #    dir_output,
            #    overwrite = TRUE,
            #    verbose   = FALSE,
            #    progress  = FALSE
            # )
            file_new <- file.path(dir_output, basename(file_old))

            if (file_new != file_old)
               file_copy(file_old, file_new, overwrite = TRUE)

            pb$tick(1)
         })
      )
   }
}

##  Read data from the pdf files and consoldiate -------------------------------

read_pdf <- function(dir_output) {

   # iterate over all pdf files
   # TODO: Add checking for number of pages in PDF
   confirm_df    <- tibble()
   confirm_files <- list.files(dir_output, "*.pdf", full.names = TRUE)
   pb            <- progress_bar$new(format = ":current of :total PDFs [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = length(confirm_files), width = 100, clear = FALSE)
   pb$tick(0)
   .log_info("Consolidating metadata into a dataframe.")
   for (file in confirm_files) {
      df <- pdftools::pdf_data(file)

      # get type; confirmatory or duplicate
      form <- pdf_section(df[[1]], seq(15, 62), seq(750, 765)) %>%
         mutate(
            text = stri_replace_last_fixed(text, ",", "")
         )
      if (nrow(form) == 0)
         form <- pdf_section(df[[1]], seq(30, 70), seq(550, 584)) %>%
            mutate(
               text = stri_replace_last_fixed(text, ",", "")
            ) %>%
            slice(1)

      # different ways of processing based on form type
      if (form$text == "LAB-F-374") {
         # get pii data
         raw_data <- pdf_section(df[[1]], seq(165, 317), seq(170, 180)) %>%
            bind_rows(pdf_section(df[[1]], seq(165, 700), seq(105, 140)))

         # get data of original result
         orig_lab <- pdf_section(df[[1]], seq(35, 80), seq(240, 250)) %>%
            bind_rows(pdf_section(df[[1]], seq(200, 400), seq(240, 250))) %>%
            bind_rows(pdf_section(df[[1]], seq(400, 600), seq(240, 250)))

         signatory_2           <- list()
         signatory_2[["name"]] <- pdf_section(df[[1]], seq(40, 500), seq(395, 410))
         signatory_2[["prc"]]  <- pdf_section(df[[1]], seq(125, 500), seq(411, 425))

         signatory_3           <- list()
         signatory_3[["name"]] <- pdf_section(df[[1]], seq(40, 500), seq(475, 490))
         signatory_3[["prc"]]  <- pdf_section(df[[1]], seq(125, 500), seq(490, 511))

         # consolidate
         pdf_info <- tibble(
            FORM                  = form[1,]$text,
            LABCODE               = raw_data[3,]$text,
            FULLNAME              = raw_data[1,]$text,
            SOURCE                = raw_data[4,]$text,
            REMARKS               = "Duplicate",
            REVIEWED_BY_NAME      = signatory_2[["name"]][1,]$text,
            REVIEWED_BY_PRC       = signatory_2[["prc"]][1,]$text,
            NOTED_BY_NAME         = signatory_3[["name"]][1,]$text,
            NOTED_BY_PRC          = signatory_3[["prc"]][1,]$text,
            SPECIMEN_RECEIPT_DATE = raw_data[2,]$text,
            NHSSS_LABCODE         = orig_lab[1,]$text,
            NHSSS_DXLAB           = orig_lab[2,]$text,
            NHSSS_DATE_CONFIRM    = orig_lab[3,]$text,
            FILENAME              = file %>% basename()
         )
      } else {
         # get pii data
         name       <- pdf_section(df[[1]], seq(77, 365), seq(110, 150))
         name_addtl <- pdf_section(df[[1]], seq(77, 365), seq(150, 170))

         if (stri_count_fixed(name[4, "text"], "-") == 2 & StrIsNumeric(StrLeft(name[4, "text"], 4))) {
            name[2, "text"] <- name[3, "text"]
            name[3, "text"] <- name[4, "text"]
            name[4, "text"] <- name_addtl[1, "text"]
            name_addtl      <- data.frame()
         }

         # if source is multi-line, append
         if (nrow(name_addtl) > 0)
            name[4, "text"] <- paste(name[4, "text"], name_addtl[1, "text"])

         # get laboratory data
         lab     <- pdf_section(df[[1]], seq(481, 769), seq(110, 155))
         confirm <- pdf_section(df[[1]], seq(107, 169), seq(315, 340))
         release <- pdf_section(df[[1]], seq(420, 750), seq(720, 728)) %>%
            mutate(
               text = stri_replace_first_fixed(text, "DATE RELEASED: ", "")
            )

         # get test data
         t1             <- list()
         t1[["kit"]]    <- pdf_section(df[[1]], seq(170, 330), seq(211, 250))
         t1[["date"]]   <- pdf_section(df[[1]], seq(100, 150), seq(235, 250))
         t1[["lot_no"]] <- pdf_section(df[[1]], seq(330, 380), seq(211, 250))
         t1[["result"]] <- pdf_section(df[[1]], seq(450, 600), seq(230, 250))

         t2             <- list()
         t2[["kit"]]    <- pdf_section(df[[1]], seq(170, 330), seq(251, 290))
         t2[["date"]]   <- pdf_section(df[[1]], seq(100, 150), seq(280, 295))
         t2[["lot_no"]] <- pdf_section(df[[1]], seq(330, 380), seq(251, 290))
         t2[["result"]] <- pdf_section(df[[1]], seq(450, 600), seq(280, 300))

         t3             <- list()
         t3[["kit"]]    <- pdf_section(df[[1]], seq(170, 330), seq(291, 330))
         t3[["date"]]   <- pdf_section(df[[1]], seq(100, 150), seq(315, 330))
         t3[["lot_no"]] <- pdf_section(df[[1]], seq(330, 380), seq(291, 330))
         t3[["result"]] <- pdf_section(df[[1]], seq(448, 600), seq(300, 320))

         # get result and remarks
         if (stri_detect_fixed(toupper(paste0(collapse = ",", df[[1]]$text)), "GEENIUS")) {
            result  <- pdf_section(df[[1]], seq(155, 500), seq(355, 375))
            remarks <- pdf_section(df[[1]], seq(155, 500), seq(375, 405)) %>%
               summarise(text = paste0(collapse = " ", text))
         } else {
            result  <- pdf_section(df[[1]], seq(155, 500), seq(335, 355))
            remarks <- pdf_section(df[[1]], seq(155, 500), seq(355, 385)) %>%
               summarise(text = paste0(collapse = " ", text))
         }

         # signatories
         signatory_1           <- list()
         signatory_1[["name"]] <- pdf_section(df[[1]], seq(40, 150), seq(680, 690))
         signatory_1[["prc"]]  <- pdf_section(df[[1]], seq(120, 150), seq(695, 705))

         signatory_2           <- list()
         signatory_2[["name"]] <- pdf_section(df[[1]], seq(220, 370), seq(680, 690))
         signatory_2[["prc"]]  <- pdf_section(df[[1]], seq(320, 350), seq(695, 705))

         signatory_3           <- list()
         signatory_3[["name"]] <- pdf_section(df[[1]], seq(430, 560), seq(680, 690))
         signatory_3[["prc"]]  <- pdf_section(df[[1]], seq(515, 545), seq(695, 705))

         # consolidate
         pdf_info <- tibble(
            FORM                  = form[1,]$text,
            LABCODE               = lab[2,]$text,
            FULLNAME              = name[1,]$text,
            AGE_SEX               = name[2,]$text,
            BDATE                 = name[3,]$text,
            SOURCE                = name[4,]$text,
            FINAL_INTERPRETATION  = result[1,]$text,
            REMARKS               = remarks[1,]$text,
            ANALYZED_BY_NAME      = signatory_1[["name"]][1,]$text,
            ANALYZED_BY_PRC       = signatory_1[["prc"]][1,]$text,
            REVIEWED_BY_NAME      = signatory_2[["name"]][1,]$text,
            REVIEWED_BY_PRC       = signatory_2[["prc"]][1,]$text,
            NOTED_BY_NAME         = signatory_3[["name"]][1,]$text,
            NOTED_BY_PRC          = signatory_3[["prc"]][1,]$text,
            DATE_CONFIRM          = confirm[1,]$text,
            DATE_RELEASE          = release[1,]$text,
            SPECIMEN_RECEIPT_DATE = lab[1,]$text,
            SPECIMEN_TYPE         = lab[4,]$text,
            T1_KIT                = t1[["kit"]][1,]$text,
            T1_LOT_NO             = t1[["lot_no"]][1,]$text,
            T1_DATE               = t1[["date"]][1,]$text,
            T1_RESULT             = t1[["result"]][1,]$text,
            T2_KIT                = t2[["kit"]][1,]$text,
            T2_LOT_NO             = t2[["lot_no"]][1,]$text,
            T2_DATE               = t2[["date"]][1,]$text,
            T2_RESULT             = t2[["result"]][1,]$text,
            T3_KIT                = t3[["kit"]][1,]$text,
            T3_LOT_NO             = t3[["lot_no"]][1,]$text,
            T3_DATE               = t3[["date"]][1,]$text,
            T3_RESULT             = t3[["result"]][1,]$text,
            FILENAME              = file %>% basename()
         )
      }

      confirm_df <- bind_rows(confirm_df, pdf_info)
      pb$tick(1)
   }

   return(confirm_df)
}

##  Perform cleaning on the consolidated df ------------------------------------

clean_data <- function(confirm_df) {
   confirm_df %<>%
      # convert major text data into uppercase
      mutate_at(
         .vars = vars(
            LABCODE,
            FULLNAME,
            SOURCE,
            FINAL_INTERPRETATION,
            ANALYZED_BY_NAME,
            REVIEWED_BY_NAME,
            NOTED_BY_NAME,
            T1_KIT,
            T2_KIT,
            T3_KIT
         ),
         ~toupper(.)
      ) %>%
      # convert into dates those in m/dd/yyyy format
      mutate_at(
         .vars = vars(DATE_CONFIRM, T1_DATE, T2_DATE, T3_DATE),
         ~if_else(
            condition = !is.na(.),
            true      = as.Date(., format = "%m/%d/%Y"),
            false     = NA_Date_
         )
      ) %>%
      mutate(
         # extract sex & age from AGE_SEX (comnbined text from pdf)
         AGE    = substr(
            AGE_SEX,
            1,
            stri_locate_first_fixed(AGE_SEX, "Y") - 1
         ),
         SEX    = substr(
            AGE_SEX,
            stri_locate_first_fixed(AGE_SEX, "/") + 1,
            nchar(AGE_SEX)
         ) %>% str_squish(),
         .after = AGE_SEX
      ) %>%
      mutate(
         # Remove extra text from Duplicates forms
         FULLNAME              = stri_replace_all_fixed(FULLNAME, "ALREADY HAS A PREVIOUS", ""),
         FULLNAME              = stri_replace_all_fixed(FULLNAME, "ALREADY HAS A", ""),
         FULLNAME              = stri_replace_all_fixed(FULLNAME, "ALREADY HAS", ""),
         FULLNAME              = stri_replace_all_fixed(FULLNAME, "ALREADY", ""),
         FULLNAME              = str_squish(FULLNAME),

         # birthdates are in yyyy-mm-dd format
         BDATE                 = if_else(
            condition = !is.na(BDATE),
            true      = as.Date(BDATE, format = "%Y-%m-%d"),
            false     = NA_Date_
         ),

         # specimen receipt date has different format depending on form
         SPECIMEN_RECEIPT_DATE = case_when(
            FORM == "LAB-F-374" ~ as.POSIXct(SPECIMEN_RECEIPT_DATE, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
            FORM == "LAB-F-331" ~ as.POSIXct(SPECIMEN_RECEIPT_DATE, format = "%m/%d/%Y %I:%M %p", tz = "UTC"),
            TRUE ~ NA_POSIXct_
         ),

         # date release has a special format
         DATE_RELEASE          = if_else(
            condition = !is.na(DATE_RELEASE),
            true      = as.POSIXct(DATE_RELEASE, format = "%d %B %Y %I:%M:%S %p"),
            false     = NA_POSIXct_
         ),

         # final result
         FINAL_INTERPRETATION  = if_else(
            condition = FORM == "LAB-F-331" & T1_RESULT == "NONREACTIVE",
            true      = "NEGATIVE",
            false     = FINAL_INTERPRETATION
         ),

         # date confirm is non-standard for the duplicates results
         NHSSS_DATE_CONFIRM    = case_when(
            stri_detect_fixed(NHSSS_DATE_CONFIRM, "/") ~ as.POSIXct(NHSSS_DATE_CONFIRM, format = "%m/%d/%Y", tz = "UTC"),
            stri_detect_fixed(NHSSS_DATE_CONFIRM, "-") ~ as.POSIXct(NHSSS_DATE_CONFIRM, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
            TRUE ~ NA_POSIXct_
         ),
      ) %>%
      select(-AGE_SEX) %>%
      distinct(FILENAME, .keep_all = TRUE)
   return(confirm_df)
}


##  Add OHASIS Conversions -----------------------------------------------------

match_ohasis <- function(confirm_df) {
   .log_info("Matching with OHASIS variables.")
   var_pairs <- ""
   for (var in names(nhsss$harp_dx$corr$pdf_saccl)) {
      confirm_df %<>%
         left_join(
            y  = nhsss$harp_dx$corr$pdf_saccl[[var]],
            by = var
         )

      key            <- names(nhsss$harp_dx$corr$pdf_saccl[[var]])[1]
      val            <- names(nhsss$harp_dx$corr$pdf_saccl[[var]])[2]
      var_pairs[key] <- val
   }
   var_pairs                                  <- var_pairs[-1]
   nhsss$harp_dx$steps$y1_pdf_saccl$var_pairs <- var_pairs

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
      # initialize checking layer

      if ("pdf_results" %in% names(nhsss$harp_dx$corr)) {
         # un-paired results
         check[["unpaired"]] <- confirm_df %>%
            left_join(
               y  = nhsss$harp_dx$corr$pdf_results %>% select(-FINAL_INTERPRETATION, -SPECIMEN_RECEIPT_DATE),
               by = "LABCODE"
            ) %>%
            filter(is.na(FILENAME_FORM)) %>%
            select(
               FORM,
               LABCODE,
               FULLNAME,
               AGE,
               BDATE,
               SOURCE,
               SPECIMEN_RECEIPT_DATE,
               FINAL_INTERPRETATION,
               REMARKS,
               DATE_CONFIRM,
               FILENAME_FORM
            )

         check[["pdf_results"]] <- confirm_df %>%
            left_join(
               y  = nhsss$harp_dx$corr$pdf_results %>% select(-FINAL_INTERPRETATION, -SPECIMEN_RECEIPT_DATE),
               by = "LABCODE"
            ) %>%
            filter(is.na(FILENAME_PDF)) %>%
            select(
               FORM,
               LABCODE,
               FULLNAME,
               AGE,
               BDATE,
               SOURCE,
               SPECIMEN_RECEIPT_DATE,
               FINAL_INTERPRETATION,
               REMARKS,
               DATE_CONFIRM,
               FILENAME_PDF
            ) %>%
            mutate(
               NEXTCLOUD_FILE = glue("{LABCODE}.pdf")
            )

         # un-encoded data
         check[["unencoded"]] <- confirm_df %>%
            left_join(
               y  = nhsss$harp_dx$corr$pdf_results %>% select(-FINAL_INTERPRETATION, -SPECIMEN_RECEIPT_DATE),
               by = "LABCODE"
            ) %>%
            filter(is.na(REC_ID)) %>%
            select(
               FORM,
               LABCODE,
               FULLNAME,
               AGE,
               BDATE,
               SOURCE,
               SPECIMEN_RECEIPT_DATE,
               FINAL_INTERPRETATION,
               REMARKS,
               DATE_CONFIRM,
               REC_ID
            )
      }

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
   pdf_for_ul <- confirm_df
   if ("pdf_results" %in% names(nhsss$harp_dx$corr))
      pdf_for_ul %<>%
         filter(
            !(FILENAME %in% list.files(dir_output))
         )


   dir_nc <- "E:/2022.11 confirmatory"
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
      confirm_pdf      <- read_pdf(dir_output)
      confirm_pdf_orig <- confirm_pdf
      confirm_pdf      <- clean_data(confirm_pdf) %>%
         match_ohasis()

      check <- get_checks(confirm_pdf)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "pdf_saccl", ohasis$ym))
}