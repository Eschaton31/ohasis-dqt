##  Load data from vl masterlist -----------------------------------------------

vlml              <- new.env()
vlml$config       <- read_sheet("1Yj-qP7sA8k-X0L9UHoXNl-TmBkPMfLkUIOlWNGJdENo", "eb_vl_ml")
vlml$px_id        <- lapply(sheet_names("1jaXjBjfWy6QsL4wcxFT6_LXYnEw5Unx-hAZUq2WIWiI"), function(sheet) {
   data <- read_sheet("1jaXjBjfWy6QsL4wcxFT6_LXYnEw5Unx-hAZUq2WIWiI", sheet)
   return(data)
})
names(vlml$px_id) <- sheet_names("1jaXjBjfWy6QsL4wcxFT6_LXYnEw5Unx-hAZUq2WIWiI")

local(envir = vlml, {
   params    <- list()
   params$mo <- input(prompt = "What is the reporting month?", max.char = 2)
   params$mo <- stri_pad_left(params$mo, width = 2, pad = "0")
   params$yr <- input(prompt = "What is the reporting year?", max.char = 4)
   params$yr <- stri_pad_left(params$yr, width = 4, pad = "0")
   params$ym <- paste0(params$yr, ".", params$mo)
   config %<>% filter(yr.mo == params$ym)
})

local(envir = vlml, {
   files <- file.path(Sys.getenv("HARP_VL"), "..", "ml", params$ym) %>%
      dir_info() %>%
      mutate(
         filename  = basename(path),
         file_type = tools::file_ext(path)
      )
   files <- split(files, ~file_type)
   data  <- list()
   refs  <- list()
})

##  Import data from .docx files -----------------------------------------------

.log_info("Importing .docx masterlists.")
docx_list  <- list()
docx_files <- list.files(dir_output, ".docx", full.names = TRUE)
docx_files <- docx_files[!stri_detect_regex(docx_files, "~")]
if (length(docx_files) > 0) {
   for (docx in docx_files) {
      # extract hub name from file
      hub <- basename(docx) %>% StrLeft(3)

      # special consideration processing
      if (hub == "smd")
         df <- docx_extract_tbl(docxtractr::read_docx(docx), header = FALSE) %>%
            slice(-1) %>%
            rename(
               id                = 1,
               px_code           = 2,
               confirmatory_code = 3,
               uic               = 4,
               vl_result         = 5,
               vl_date           = 6,
               remarks           = 7
            )
      else if (hub == "mtl")
         df <- docx_extract_tbl(docxtractr::read_docx(docx), preserve = TRUE) %>%
            separate(
               col  = "Patient.Code.UIC.SACCL.Code",
               sep  = "\n",
               into = c("px_code", "uic", "confirmatory_code")
            )
      else if (hub == "fps")
         df <- docx_extract_tbl(docxtractr::read_docx(docx), tbl_number = docx_tbl_count(docxtractr::read_docx(docx)))
      else
         df <- docx_extract_tbl(docxtractr::read_docx(docx))

      # standardize dataset
      df %<>%
         # clean strings
         mutate_if(
            .predicate = is.character,
            ~str_squish(.) %>% if_else(. == "", NA_character_, .)
         ) %>%
         # remove empty vectors
         remove_empty(which = c("cols", "rows")) %>%
         # special renaming to standardize
         rename_all(
            ~case_when(
               . == "CONFIRMATORY.CODE" ~ "confirmatory_code",
               . == "CONFIMATORY.CODE" ~ "confirmatory_code",
               . == "PATIENT.CODE" ~ "px_code",
               . == "UIC" ~ "uic",
               . == "VL.RESULT" ~ "vl_result",
               . == "VIRAL.LOAD.RESULT" ~ "vl_result",
               . == "VL.Test.Result" ~ "vl_result",
               . == "RESULT" ~ "vl_result",
               . == "Result" ~ "vl_result",
               . == "VL.TEST.RESULTS" ~ "vl_result",
               . == "DATE.PERFORMED" ~ "vl_date",
               . == "Date" ~ "vl_date",
               . == "DATE" ~ "vl_date",
               . == "DATE.DONE" ~ "vl_date",
               . == "DATE..DONE" ~ "vl_date",
               . == "DATE.TAKEN" ~ "vl_date",
               . == "Date.Performed.Result" ~ "vl_date",
               . == "REMARKS" ~ "remarks",
               . == "NAME.OF............PATIENT" ~ "name",
               . == "NAME.OF..PATIENT" ~ "name",
               . == "Name" ~ "name",
               . == "ADDRESS" ~ "curr_addr",
               . == "Age" ~ "age",
               . == "AGE" ~ "age",
               . == "SEX" ~ "sex",
               . == "Sex" ~ "sex",
               TRUE ~ .
            )
         ) %>%
         mutate(
            hub     = hub,
            .before = 1
         ) %>%
         mutate(
            src_file = basename(docx),
         )

      # consolidate into a datafram
      docx_list[[hub]] <- df
   }


   # consolidate and clean per column data
   .log_info("Cleaning consolidated tables.")
   docx_df <- bind_rows(docx_list)
   if (!("remarks" %in% names(docx_df)))
      docx_df$remarks <- NA_character_

   docx_df %<>%
      mutate(
         # remove spaces first
         vl_date     = stri_replace_all_fixed(vl_date, ".", ""),
         vl_date     = stri_replace_all_fixed(vl_date, ",", " , "),
         vl_date     = str_squish(vl_date),
         vl_date     = stri_replace_all_fixed(vl_date, " ,", ","),

         # do 2 passes
         # vl_date_2   = case_when(
         #    hub == "ace" ~ as.Date(vl_date, tryFormats = c("%Y/%m/%d", "%Y-%m/%d")),
         #    hub == "btn" & !stri_detect_fixed(vl_date, ",") ~ as.Date(vl_date, "%B%e%Y"),
         #    hub == "btn" & stri_detect_regex(vl_date, "^[A-Z][A-Z][A-Z] [0-9]") ~ as.Date(vl_date, "%b %e, %Y"),
         #    hub == "btn" & stri_detect_fixed(vl_date, ",") ~ as.Date(vl_date, "%B %e, %Y"),
         #    hub == "btn" & stri_detect_regex(vl_date, "\\.[0-9]+,") ~ as.Date(vl_date, "%b.%e,%Y"),
         #    hub == "btn" & stri_detect_regex(vl_date, "\\.[0-9]+[0-9]+") ~ as.Date(vl_date, "%b.%E,%Y"),
         #    hub == "con" ~ as.Date(vl_date, "%m/%d/%Y"),
         #    hub == "fps" ~ as.Date(vl_date, "%m/%d/%Y"),
         #    hub == "smd" ~ as.Date(vl_date, "%m/%d/%Y"),
         #    hub == "mtl" ~ as.Date(vl_date, "%m/%d/%Y"),
         #    hub == "asm" ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
         #    hub == "amp" ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%y"),
         # ),

         # vl_result
         use_remarks = case_when(
            hub == "fps" & is.na(vl_result) ~ 1,
            TRUE ~ 0
         ),
         vl_result   = if_else(
            condition = use_remarks == 1,
            true      = remarks,
            false     = vl_result,
            missing   = vl_result
         ),
         remarks     = if_else(
            condition = use_remarks == 1,
            true      = NA_character_,
            false     = remarks,
            missing   = remarks
         ),

         # uic
         uic         = case_when(
            hub == "ace" ~ substr(uic, stri_locate_first_fixed(uic, ".") + 1, nchar(uic)),
            TRUE ~ uic
         ),

         # tag data for dropping
         drop        = case_when(
            vl_date == "Not Done" ~ 1,
            vl_date == "Not done" ~ 1,
            vl_date == "Date Performed" ~ 1,
            vl_result == "Not Done" ~ 1,
            vl_result == "Not done" ~ 1,
            vl_result == "NO DATA" ~ 1,
            TRUE ~ 0
         )
      ) %>%
      filter(drop == 0) %>%
      remove_empty(which = c("cols", "rows"))
} else {
   docx_df <- data.frame()
}

##  Import data from .pdf files ------------------------------------------------

.log_info("Importing .pdf masterlists.")
pdf_list  <- list()
pdf_files <- list.files(dir_output, ".pdf", full.names = TRUE)
pdf_files <- pdf_files[!stri_detect_regex(pdf_files, "~")]
if (length(pdf_files) > 0) {
   for (pdf in pdf_files) {
      # extract hub name from file
      hub <- basename(pdf) %>% StrLeft(3)

      if (hub == "btn") {
         lst <- tabulizer::extract_tables(file = pdf, method = "lattice")

         for (i in seq_len(length(lst))) {
            names(lst)[i] <- glue("tab_{i}")
            lst[[i]]      <- as.data.frame(lst[[i]])
         }

         df        <- bind_rows(lst)
         names(df) <- unlist(df[1,])
         df        <- df[-1,]
      }

      # standardize dataset
      df %<>%
         # clean strings
         mutate_if(
            .predicate = is.character,
            ~str_squish(.) %>% if_else(. == "", NA_character_, .)
         ) %>%
         # remove empty vectors
         remove_empty(which = c("cols", "rows")) %>%
         # special renaming to standardize
         rename_all(
            ~case_when(
               . == "CONFIRMATORY\rCODE" ~ "confirmatory_code",
               . == "PATIENT\rCODE" ~ "px_code",
               . == "UIC" ~ "uic",
               . == "RESULT" ~ "vl_result",
               . == "DATE" ~ "vl_date",
               . == "REMARKS" ~ "remarks",
               TRUE ~ .
            )
         ) %>%
         mutate(
            hub     = hub,
            .before = 1
         ) %>%
         mutate(
            src_file = basename(pdf),
         )

      # consolidate into a datafram
      pdf_list[[hub]] <- df
   }

   # consolidate and clean per column data
   .log_info("Cleaning consolidated tables.")
   pdf_df <- bind_rows(pdf_list) %>%
      mutate(
         # remove spaces first
         vl_date     = stri_replace_all_fixed(vl_date, ".", ""),
         vl_date     = stri_replace_all_fixed(vl_date, ",", " , "),
         vl_date     = str_squish(vl_date),
         vl_date     = stri_replace_all_fixed(vl_date, " ,", ","),

         # do 2 passes
         # vl_date_2   = case_when(
         #    hub == "btn" & !stri_detect_fixed(vl_date, ",") ~ as.Date(vl_date, "%B%e%Y"),
         #    hub == "btn" & stri_detect_regex(vl_date, "^[A-Z][A-Z][A-Z] [0-9]") ~ as.Date(vl_date, "%b %e, %Y"),
         #    hub == "btn" & stri_detect_fixed(vl_date, ",") ~ as.Date(vl_date, "%B %e, %Y"),
         #    hub == "btn" & stri_detect_regex(vl_date, "\\.[0-9]+,") ~ as.Date(vl_date, "%b.%e,%Y"),
         #    hub == "btn" & stri_detect_regex(vl_date, "\\.[0-9]+[0-9]+") ~ as.Date(vl_date, "%b.%E,%Y"),
         # ),

         # vl_result
         use_remarks = case_when(
            hub == "fps" & is.na(vl_result) ~ 1,
            TRUE ~ 0
         ),
         vl_result   = if_else(
            condition = use_remarks == 1,
            true      = remarks,
            false     = vl_result,
            missing   = vl_result
         ),
         remarks     = if_else(
            condition = use_remarks == 1,
            true      = NA_character_,
            false     = remarks,
            missing   = remarks
         ),

         # uic
         uic         = case_when(
            hub == "ace" ~ substr(uic, stri_locate_first_fixed(uic, ".") + 1, nchar(uic)),
            TRUE ~ uic
         ),

         # tag data for dropping
         drop        = case_when(
            vl_date == "Not Done" ~ 1,
            vl_date == "Not done" ~ 1,
            vl_date == "Date Performed" ~ 1,
            vl_result == "Not Done" ~ 1,
            vl_result == "Not done" ~ 1,
            vl_result == "NO DATA" ~ 1,
            TRUE ~ 0
         )
      ) %>%
      filter(drop == 0) %>%
      remove_empty(which = c("cols", "rows"))
} else {
   pdf_df <- data.frame()
}

##  Import data from .xlsx files -----------------------------------------------

.log_info("Importing .xls* masterlists.")

local(envir = vlml, {
   refs$xlsx <- files$xlsx %>%
      left_join(
         y  = config,
         by = "filename"
      ) %>%
      rowwise() %>%
      mutate(
         sheets = paste(collapse = ", ", excel_sheets(path))
      ) %>%
      ungroup()

   data$xlsx <- lapply(seq_len(nrow(refs$xlsx)), function(i, ref) {
      hub_code  <- ref[i,]$hub_code
      password  <- ifelse(!is.na(ref[i,]$password), ref[i,]$password, "")
      start_row <- ifelse(!is.na(ref[i,]$start_row), ref[i,]$start_row, 4)
      start_col <- ifelse(!is.na(ref[i,]$start_col), ref[i,]$start_col, 1)

      if (password == "") {
         data <- read_xlsx(ref[i,]$path, col_types = "text", skip = start_row - 1) %>%
            remove_empty() %>%
            mutate(hub = hub_code)
      }
   }, ref = refs$xlsx)
   data$xlsx <- bind_rows(data$xlsx) %>%
      mutate(
         drop = if_else(
            `Confirmatory code` == "DOH-ABC-12345",
            1,
            0,
            0
         )
      ) %>%
      filter(drop == 0)
})

##  Match w/ OHASIS Patient IDs ------------------------------------------------

local(envir = vlml, {
   conso <- bind_rows(data) %>%
      rename_all(
         ~case_when(
            . == "Patient code" ~ "px_code",
            . == "Confirmatory code" ~ "confirmatory_code",
            . == "Full Name" ~ "name",
            . == "UIC" ~ "uic",
            . == "Sex" ~ "sex",
            . == "ART Start Date" ~ "artstart_date",
            . == "Latest_Visit" ~ "latest_ffupdate",
            . == "Latest_Regimen" ~ "latest_regimen",
            . == "Outcome" ~ "outcome",
            . == "Viral load date" ~ "vl_date",
            . == "Viral load result" ~ "vl_result",
            . == "If baseline viral load test, put Y" ~ "baseline_vl",
            . == "Remarks" ~ "remarks",
            TRUE ~ .
         )
      ) %>%
      mutate_at(
         .vars = vars(latest_ffupdate, artstart_date, vl_date),
         ~case_when(
            StrIsNumeric(.) ~ excel_numeric_to_date(as.numeric(.)),
            TRUE ~ as.Date(., "%Y-%m-%d")
         )
      ) %>%
      mutate(
         sex        = StrLeft(sex, 1),
         hub        = tolower(hub),
         PATIENT_ID = NA_character_
      )

   for (i in seq_len(length(px_id))) {
      merge_ids <- names(px_id[[i]])
      merge_ids <- merge_ids[merge_ids != "PATIENT_ID"]
      curr_pass <- as.symbol(glue("id_pass_{i}"))

      if (length(setdiff(merge_ids, names(conso))) == 0)
         conso %<>%
            left_join(
               y  = px_id[[i]] %>%
                  as.data.frame() %>%
                  rename(
                     !!curr_pass := PATIENT_ID
                  ),
               by = merge_ids
            ) %>%
            mutate(
               PATIENT_ID = if_else(
                  condition = !is.na(!!curr_pass) & is.na(PATIENT_ID),
                  true      = !!curr_pass,
                  false     = PATIENT_ID,
                  missing   = PATIENT_ID
               )
            ) %>%
            select(-starts_with("id_pass"))
   }
})

if (!("birthdate" %in% names(docx_df)) && nrow(docx_df) > 0)
   docx_df$birthdate <- NA_character_

for (var in c("birthdate", "name", "birthdate"))
   if (!(var %in% names(pdf_df)) && nrow(pdf_df > 0))
      pdf_df[var] <- NA_character_

# add ids
pdf_df %<>% mutate(PATIENT_ID = NA_character_, row_id = row_number())
docx_df %<>% mutate(PATIENT_ID = NA_character_, row_id = row_number())
xlsx_df %<>% mutate(PATIENT_ID = NA_character_, row_id = row_number())
for (i in seq_len(length(nhsss$harp_vl$corr$PATIENT_ID))) {
   merge_ids <- names(nhsss$harp_vl$corr$PATIENT_ID[[i]])
   merge_ids <- merge_ids[merge_ids != "PATIENT_ID"]
   curr_pass <- as.symbol(glue("id_pass_{i}"))

   if (nrow(pdf_df) > 0)
      pdf_df %<>%
         left_join(
            y  = nhsss$harp_vl$corr$PATIENT_ID[[i]] %>%
               as.data.frame() %>%
               rename(
                  !!curr_pass := PATIENT_ID
               ),
            by = merge_ids
         ) %>%
         mutate(
            PATIENT_ID = if_else(
               condition = !is.na(!!curr_pass) & is.na(PATIENT_ID),
               true      = !!curr_pass,
               false     = PATIENT_ID,
               missing   = PATIENT_ID
            )
         ) %>%
         select(-starts_with("id_pass"))

   if (nrow(docx_df) > 0)
      docx_df %<>%
         left_join(
            y  = nhsss$harp_vl$corr$PATIENT_ID[[i]] %>%
               as.data.frame() %>%
               rename(
                  !!curr_pass := PATIENT_ID
               ),
            by = merge_ids
         ) %>%
         mutate(
            PATIENT_ID = if_else(
               condition = !is.na(!!curr_pass) & is.na(PATIENT_ID),
               true      = !!curr_pass,
               false     = PATIENT_ID,
               missing   = PATIENT_ID
            )
         ) %>%
         select(-starts_with("id_pass"))

   xlsx_df %<>%
      left_join(
         y  = nhsss$harp_vl$corr$PATIENT_ID[[i]] %>%
            as.data.frame() %>%
            rename(
               !!curr_pass := PATIENT_ID
            ),
         by = merge_ids
      ) %>%
      mutate(
         PATIENT_ID = if_else(
            condition = !is.na(!!curr_pass) & is.na(PATIENT_ID),
            true      = !!curr_pass,
            false     = PATIENT_ID,
            missing   = PATIENT_ID
         )
      ) %>%
      select(-starts_with("id_pass"))
}

# dedup (rows must be unique
pdf_df %<>% distinct(row_id, .keep_all = TRUE)
xlsx_df %<>% distinct(row_id, .keep_all = TRUE)
xlsx_df %<>% distinct(row_id, .keep_all = TRUE) %>%
   rename_all(
      ~case_when(
         stri_detect_fixed(., "Virally Supressed (Yes/No)") ~ "vl_result_alt",
         TRUE ~ .
      )
   )

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `vl_ml` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check <- list()
if (update == "1") {
   # check for new columns in .docx data
   .log_info("Checking for un-accounted variables in .docx files.")
   docxnames <- c('hub', 'uic', 'confirmatory_code', 'px_code', 'vl_result', 'vl_date', 'remarks', 'name', 'curr_addr', 'age', 'sex', 'id', 'vl_date_2', 'use_remarks', 'drop', 'row_id', 'PATIENT_ID', 'src_file', 'src_sheet')
   newnames  <- setdiff(names(docx_df), docxnames)
   if (length(newnames) > 0) {
      nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["docx_newvars"]] <- docx_df %>%
         filter_at(
            .vars           = vars(newnames),
            .vars_predicate = any_vars(!is.na(.))
         )
   }

   # check for new columns in .pdf data
   .log_info("Checking for un-accounted variables in .pdf files.")
   pdfnames <- c('hub', 'uic', 'confirmatory_code', 'px_code', 'vl_result', 'vl_date', 'remarks', 'name', 'curr_addr', 'age', 'sex', 'id', 'vl_date_2', 'use_remarks', 'drop', 'row_id', 'PATIENT_ID', 'src_file', 'src_sheet')
   newnames <- setdiff(names(pdf_df), pdfnames)
   if (length(newnames) > 0) {
      nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["pdf_newvars"]] <- pdf_df %>%
         filter_at(
            .vars           = vars(newnames),
            .vars_predicate = any_vars(!is.na(.))
         )
   }

   # check for new columns in .xlsx data
   .log_info("Checking for un-accounted variables in .xlsx files.")
   xlsxnames <- c('hub', 'cd4_date', 'uic', 'confirmatory_code', 'px_code', 'vl_date', 'vl_result', 'remarks', 'vl_result_alt', 'vl_date_alt', 'artstart_date', 'age', 'birthdate', 'sex', 'curr_addr', 'contact', 'latest_regimen', 'artstart_ddate', 'philhealth_no', 'latest_ffupdate', 'latest_nextpickup', 'cd4_result', 'vl_suppressed', 'actions_taken', 'name', 'px_code_alt', 'date_request', 'vl_code', 'date_receive', 'id', 'baseline_vl_result', 'outcome', 'vl_cat', 'source', 'drop', 'vl_date_2', 'row_id', 'status', 'PATIENT_ID', 'src_file', 'src_sheet', 'status', 'baseline_cd4_result', "hiv_test_result", "year")
   newnames  <- setdiff(names(xlsx_df), xlsxnames)
   if (length(newnames) > 0) {
      nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["xlsx_newvars"]] <- xlsx_df %>%
         filter_at(
            .vars           = vars(newnames),
            .vars_predicate = any_vars(!is.na(.))
         )
   }

   # missing vl data
   .log_info("Finding data w/o VL info.")
   nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["vl_data_missing"]] <- bind_rows(docx_df, pdf_df, xlsx_df) %>%
      filter(
         is.na(vl_date) | is.na(vl_result)
      )

   # no patient id yes
   .log_info("Checking for those w/ missing PATIENT_IDs.")
   nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["PATIENT_ID"]] <- bind_rows(docx_df, pdf_df, xlsx_df) %>%
      filter(
         is.na(PATIENT_ID)
      )

   # colnames
   .log_info("Checking for those w/ missing PATIENT_IDs.")
   docx_cols <- data.frame()
   for (i in seq_len(length(docx_list)))
      docx_cols <- bind_rows(
         docx_cols,
         data.frame(
            hub  = names(docx_list)[i],
            cols = paste(collapse = ", ", names(docx_list[[i]]))
         )
      )
   nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["docx_cols"]] <- docx_cols

   pdf_cols <- data.frame()
   for (i in seq_len(length(pdf_list)))
      pdf_cols <- bind_rows(
         pdf_cols,
         data.frame(
            hub  = names(pdf_list)[i],
            cols = paste(collapse = ", ", names(pdf_list[[i]]))
         )
      )
   nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["pdf_cols"]] <- pdf_cols

   xlsx_cols <- data.frame()
   for (i in seq_len(length(xlsx_list)))
      xlsx_cols <- bind_rows(
         xlsx_cols,
         data.frame(
            hub  = names(xlsx_list)[i],
            cols = paste(collapse = ", ", names(xlsx_list[[i]]))
         )
      )
   nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[["xlsx_cols"]] <- xlsx_cols
}

for (i in names(nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check))
   if (sum(dim(nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[[i]])) == 0)
      nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$check[[i]] <- NULL

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- glue("vl_ml_{ml_report}")
if (!is.empty(nhsss$harp_vl[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_vl[[data_name]]$check,
      drive_path  = paste0(nhsss$harp_vl$gdrive$path$report, "Validation/"),
      surv_name   = "HARP VL"
   )

# assign to global environment
nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$docx_df   <- docx_df
nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$pdf_df    <- pdf_df
nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$xlsx_df   <- xlsx_df
nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$docx_list <- docx_list
nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$pdf_list  <- pdf_list
nhsss$harp_vl[[glue("vl_ml_{ml_report}")]]$xlsx_list <- xlsx_list

##  Save data ------------------------------------------------------------------

save <- input(
   prompt  = "Create `.dta` file?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
if (save == "1")
   write_dta(bind_rows(docx_df, pdf_df, xlsx_df), glue(r"({Sys.getenv("HARP_VL")}/{format(Sys.time(), "%Y%m%d")}_vl_ml_{ml_report}.dta)"))

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))