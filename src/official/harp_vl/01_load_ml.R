##  Load data from vl masterlist -----------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

ml_qr     <- input(prompt = "What is the reporting quarter?", max.char = 1)
ml_yr     <- input(prompt = "What is the reporting year?", max.char = 4)
ml_yr     <- ml_yr %>% stri_pad_left(width = 4, pad = "0")
ml_report <- glue("{ml_yr}-Q{ml_qr}")

##  Conduct prelim steps on the cloud instances --------------------------------

# check if directories exist
.log_info("Checking drive/cloud directories.")
dir_output       <- file.path("data", ohasis$ym, "harp_vl", "vl_ml", ml_report)
dir_cloud_base   <- glue("/HARP Cloud/Hub Masterlist/VIRAL LOAD") %>% stri_replace_all_fixed(" ", "%20")
dir_cloud_report <- glue("{dir_cloud_base}/{ml_report}")
check_dir(dir_output)

# copy to local system
download <- input(
   prompt  = "Download VL masterlist files?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
if (download == "1") {
   .log_info("Downloading raw ml files.")
   download_files(
	  paths      = paste0(dir_cloud_report, "/", list_files(dir_cloud_report, full_info = TRUE)$file),
	  target_dir = dir_output
   )
}

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
xlsx_list  <- list()
xlsx_files <- list.files(dir_output, glue(".xls*"), full.names = TRUE)
xlsx_files <- xlsx_files[!stri_detect_regex(xlsx_files, "~")]
for (xlsx in xlsx_files) {
   # extract hub name from file
   hub <- basename(xlsx) %>% StrLeft(3)

   # get xlsx configurations
   # password
   try({
	  password <- nhsss$harp_vl$corr$config_xlsx$password %>%
		 rename(hubname = hub) %>%
		 filter(hubname == hub, !is.na(!!ml_report)) %>%
		 as.data.frame() %>%
		 select(ml_report)

	  if (nrow(password) > 0)
		 password <- as.character(password)
	  else
		 password <- NULL
   })

   # password
   try({
	  start_row <- nhsss$harp_vl$corr$config_xlsx$start_row %>%
		 rename(hubname = hub) %>%
		 filter(hubname == hub, !is.na(!!ml_report)) %>%
		 as.data.frame() %>%
		 select(ml_report)

	  if (nrow(start_row) > 0)
		 start_row <- as.numeric(start_row)
	  else
		 start_row <- 0
   })

   # load workbooks
   if (hub == "vcm") {
	  df <- read_xlsx(
		 xlsx,
		 sheet     = 1,
		 skip      = 1,
		 col_types = "text"
	  ) %>%
		 mutate_at(
			.vars = vars(starts_with("Date of Birth"), starts_with("DATE")),
			~case_when(
			   stri_detect_fixed(., "/") ~ as.Date(., "%m/%d/%Y"),
			   stri_detect_fixed(., "-") ~ as.Date(., "%Y-%m-%d"),
			   StrIsNumeric(.) ~ excel_numeric_to_date(as.numeric(.))
			) %>% as.character()
		 ) %>%
		 rename_all(
			~case_when(
			   . == "DATE...11" ~ "vl_date",
			   . == "DATE...14" ~ "cd4_date",
			   TRUE ~ .
			)
		 ) %>%
		 mutate(
			src_file  = basename(xlsx),
			src_sheet = sheets[1],
		 )

	  sheets <- excel_sheets(xlsx)
   } else {
	  if (!is.null(password) && !is.na(password)) {
		 wb <- XLConnect::loadWorkbook(xlsx, password = password)
	  } else {
		 wb <- XLConnect::loadWorkbook(xlsx)
	  }
	  # import all sheets if many
	  sheets <- XLConnect::getSheets(wb)
   }

   if (hub == "slh")
	  sheets <- sheets[1]

   if (hub == "r1m")
	  sheets <- sheets[sheets == "VIRAL LOAD"]

   if (hub == "pgt" && "VIRAL LOAD" %in% sheets)
	  sheets <- sheets[sheets == "VIRAL LOAD"]

   if (hub == "bmc")
	  sheets <- sheets[sheets == "VIRAL LOAD"]

   if (length(sheets) > 1) {

	  # tag puts all their data into one sheet
	  if (hub == "bic") {
		 tmp_list                    <- list()
		 tmp_list[[glue("{hub}_1")]] <- read_xlsx(
			xlsx,
			sheet = 1,
			skip  = 3
		 ) %>%
			pivot_longer(
			   cols      = c(starts_with("Result"), starts_with("Date")),
			   names_to  = "vl_var",
			   values_to = "vl_data"
			) %>%
			mutate(
			   var     = case_when(
				  stri_detect_fixed(vl_var, "Date") ~ "vl_date",
				  stri_detect_fixed(vl_var, "Result") ~ "vl_result",
			   ),
			   var_num = substr(vl_var, stri_locate_last_fixed(vl_var, "...") + 3, nchar(vl_var)) %>% as.numeric(),
			   var_num = case_when(
				  stri_detect_fixed(vl_var, "Date") ~ var_num + 1,
				  stri_detect_fixed(vl_var, "Result") ~ var_num,
			   ),
			) %>%
			pivot_wider(
			   id_cols     = c(UIC, `Patient's Name`, `SACCLE Code`, `Patient Code`, Address, var_num),
			   names_from  = var,
			   values_from = vl_data
			) %>%
			mutate(
			   src_file  = basename(xlsx),
			   src_sheet = sheets[1],
			   vl_date   = case_when(
				  stri_detect_fixed(vl_date, "/") ~ as.Date(vl_date, "%m/%d/%Y"),
				  StrIsNumeric(vl_date) ~ excel_numeric_to_date(as.numeric(vl_date))
			   ),
			   vl_date   = as.character(vl_date)
			) %>%
			filter(!is.na(vl_date) | !is.na(vl_result)) %>%
			select(-var_num)

	  } else if (hub == "tag") {
		 tmp_list                    <- list()
		 tmp_list[[glue("{hub}_1")]] <- XLConnect::readWorksheet(
			wb,
			sheets[1],
			colTypes = XLC$DATA_TYPE.STRING,
			startRow = 4,
			startCol = 2,
			endCol   = 6
		 ) %>%
			mutate(
			   src_file  = basename(xlsx),
			   src_sheet = sheets[1]
			)
		 tmp_list[[glue("{hub}_2")]] <- XLConnect::readWorksheet(
			wb,
			sheets[1],
			colTypes = XLC$DATA_TYPE.STRING,
			startRow = 4,
			startCol = 9,
			endCol   = 13
		 ) %>%
			mutate(
			   src_file  = basename(xlsx),
			   src_sheet = sheets[1]
			)
	  } else if (hub == "tph") {
		 tmp_list <- list()
		 for (i in seq_len(length(sheets))) {
			tmp_list[[glue("{hub}_{i}_1")]] <- XLConnect::readWorksheet(
			   wb,
			   sheets[i],
			   colTypes = XLC$DATA_TYPE.STRING,
			   startRow = 3,
			   startCol = 1,
			   endCol   = 5
			) %>%
			   mutate(
				  src_file  = basename(xlsx),
				  src_sheet = sheets[i]
			   )
			tmp_list[[glue("{hub}_{i}_2")]] <- XLConnect::readWorksheet(
			   wb,
			   sheets[i],
			   colTypes = XLC$DATA_TYPE.STRING,
			   startRow = 3,
			   startCol = 9,
			   endCol   = 13
			) %>%
			   mutate(
				  src_file  = basename(xlsx),
				  src_sheet = sheets[i]
			   )

			if (nrow(tmp_list[[glue("{hub}_{i}_1")]]) > 0)
			   tmp_list[[glue("{hub}_{i}_1")]] %<>% rename(Date = 1)

			if (nrow(tmp_list[[glue("{hub}_{i}_2")]]) > 0)
			   tmp_list[[glue("{hub}_{i}_2")]] %<>% rename(Date = 1)
		 }
	  } else if (hub == "bcr") {
		 tmp_list <- list()
		 for (i in seq_len(length(sheets)))
			tmp_list[[glue("{hub}_{i}")]] <- XLConnect::readWorksheet(
			   wb,
			   sheets[i],
			   colTypes = XLC$DATA_TYPE.STRING,
			   startRow = start_row,
			   header   = FALSE
			) %>%
			   rename(
				  id        = 1,
				  bcr       = 2,
				  year      = 3,
				  initials  = 4,
				  vl_date   = 5,
				  vl_result = 6
			   ) %>%
			   mutate(
				  src_file  = basename(xlsx),
				  src_sheet = sheets[i],
				  px_code   = if_else(
					 condition = stri_detect_fixed(year, "B"),
					 true      = paste0(year, "-", initials),
					 false     = paste0("B", year, "-", initials),
				  )
			   ) %>%
			   select(-bcr, -year, -initials)
	  } else {
		 tmp_list <- list()
		 for (i in seq_len(length(sheets)))
			tmp_list[[glue("{hub}_{i}")]] <- XLConnect::readWorksheet(
			   wb,
			   sheets[i],
			   colTypes = XLC$DATA_TYPE.STRING,
			   startRow = start_row
			) %>%
			   rename_all(
				  ~if_else(
					 condition = . == "Date.Recieved",
					 true      = "Date.Received",
					 false     = .,
					 missing   = .
				  )
			   ) %>%
			   mutate(
				  src_file  = basename(xlsx),
				  src_sheet = sheets[i]
			   )
	  }

	  df <- bind_rows(tmp_list)
   } else if (hub == "nmm") {
	  tmp_list                    <- list()
	  tmp_list[[glue("{hub}_1")]] <- XLConnect::readWorksheet(
		 wb,
		 sheets[1],
		 colTypes = XLC$DATA_TYPE.STRING,
		 startRow = 2,
		 startCol = 1,
		 endCol   = 10
	  ) %>%
		 mutate(
			src_file  = basename(xlsx),
			src_sheet = sheets[1]
		 )
	  tmp_list[[glue("{hub}_2")]] <- XLConnect::readWorksheet(
		 wb,
		 sheets[1],
		 colTypes = XLC$DATA_TYPE.STRING,
		 startRow = 2,
		 startCol = 12,
		 endCol   = 17
	  ) %>%
		 mutate(
			src_file  = basename(xlsx),
			src_sheet = sheets[1]
		 )
	  df                          <- bind_rows(tmp_list)
   } else if (hub == "mrl" && ml_report == "2021-Q3") {
	  df <- XLConnect::readWorksheet(
		 wb,
		 sheets[1],
		 colTypes = XLC$DATA_TYPE.STRING
	  ) %>%
		 remove_empty(c("cols", "rows")) %>%
		 select(-starts_with("VIRAL.LOAD.STATUS")) %>%
		 pivot_longer(
			cols      = ends_with("LOAD"),
			names_to  = "var_num",
			values_to = "vl_data"
		 ) %>%
		 filter(!is.na(vl_data)) %>%
		 mutate(
			var_num   = case_when(
			   var_num == "FIRST.VIRAL.LOAD" ~ 1,
			   var_num == "SECOND.VIRAL.LOAD" ~ 2,
			   var_num == "THIRD.VIRAL.LOAD" ~ 3,
			),
			vl_result = substr(vl_data, 1, stri_locate_last_fixed(vl_data, " ") - 1),
			vl_date   = substr(vl_data, stri_locate_last_fixed(vl_data, " ") + 1, nchar(vl_data)),
			src_file  = basename(xlsx),
			src_sheet = sheets[1],
			UIC       = stri_replace_all_fixed(UIC, "-", "")
		 ) %>%
		 rename(uic = UIC) %>%
		 select(-var_num, -vl_data)

   }  else if (hub != "vcm") {
	  df <- XLConnect::readWorksheet(
		 wb,
		 sheets[1],
		 colTypes = XLC$DATA_TYPE.STRING,
		 startRow = start_row
	  ) %>%
		 mutate(
			src_file  = basename(xlsx),
			src_sheet = sheets[1]
		 )
   }

   # free up memory used
   rm(wb)
   xlcFreeMemory()

   # tly
   if (hub == "tly") {
	  df %<>%
		 rename_all(
			~case_when(
			   stri_detect_fixed(toupper(.), "DATE") & stri_detect_fixed(toupper(.), "2022") ~ "vl_date-2022",
			   stri_detect_fixed(toupper(.), "DATE") & stri_detect_fixed(toupper(.), "2021") ~ "vl_date-2021",
			   stri_detect_fixed(toupper(.), "RESULT") & stri_detect_fixed(toupper(.), "2022") ~ "vl_result-2022",
			   stri_detect_fixed(toupper(.), "RESULT") & stri_detect_fixed(toupper(.), "2021") ~ "vl_result-2021",
			   stri_detect_fixed(toupper(.), "Full.Name") ~ "name",
			   TRUE ~ .
			)
		 ) %>%
		 as_tibble() %>%
		 pivot_longer(
			cols      = starts_with("vl_"),
			names_to  = "var_num",
			values_to = "vl_data"
		 ) %>%
		 separate(
			col  = var_num,
			sep  = "-",
			into = c("piece", "year")
		 ) %>%
		 pivot_wider(
			id_cols     = c(Full.Name, UIC, Confirmatory.Code, Patient.Code, src_file, src_sheet, year),
			names_from  = piece,
			values_from = vl_data
		 )
   }
   if (hub == "drh") {
	  df %<>%
		 rename_all(
			~case_when(
			   . == "Viral.load" ~ "vl_data_1",
			   . == "LATEST.Viral.Load" ~ "vl_data_2",
			   . == "LATEST.Viral.Load.1" ~ "vl_data_3",
			   . == "SACCL.CODE.Rhivda.CODE" ~ "confirmatory_code",
			   . == "BDAY" ~ "birthdate",
			   . == "RHWC.CODE" ~ "px_code",
			   TRUE ~ .
			)
		 ) %>%
		 pivot_longer(
			cols      = starts_with("vl_"),
			names_to  = "var_num",
			values_to = "vl_data"
		 ) %>%
		 mutate(
			vl_data = stri_replace_last_fixed(vl_data, "-", "_")
		 ) %>%
		 separate(
			col  = vl_data,
			sep  = "_",
			into = c("vl_result", "vl_date")
		 ) %>%
		 select(
			confirmatory_code,
			UIC,
			px_code,
			birthdate,
			vl_date,
			vl_result,
			src_file,
			src_sheet
		 )
   }
   if (hub == "scp") {
	  df %<>%
		 rename_all(
			~case_when(
			   . == "VL.TEST.RESULT" ~ "vl_result",
			   . == "DATE.PERFORMED" ~ "vl_date",
			   . == "PATIENTS.CODE" ~ "px_code",
			   TRUE ~ .
			)
		 )
   }

   # special run
   if (hub %in% c("sjd", "mar")) {
	  if (!("No.." %in% names(df)))
		 df$`No..` <- NA_character_

	  if (!("HUB.NO." %in% names(df)))
		 df$`HUB.NO.` <- NA_character_

	  df %<>%
		 pivot_longer(
			cols      = starts_with("Col"),
			names_to  = "var_num",
			values_to = "vl_data"
		 ) %>%
		 filter(!is.na(vl_data)) %>%
		 mutate(
			var_num   = case_when(
			   var_num == "X.1ST" ~ 1,
			   var_num == "X.2ND" ~ 2,
			   var_num == "X.3RD" ~ 3,
			   var_num == "X.4TH" ~ 4,
			   var_num == "X.5TH" ~ 5,
			   var_num == "X1st" ~ 1,
			   var_num == "X2nd" ~ 2,
			   var_num == "X3rd" ~ 3,
			   var_num == "X4th" ~ 4,
			   var_num == "X5th" ~ 5,
			   var_num == "Col4" ~ 1,
			   var_num == "Col5" ~ 2,
			   var_num == "Col6" ~ 3,
			   var_num == "Col7" ~ 4,
			   var_num == "Col8" ~ 5,
			),
			vl_date   = substr(vl_data, 1, stri_locate_first_fixed(vl_data, " ") - 1),
			vl_result = substr(vl_data, stri_locate_first_fixed(vl_data, " ") + 1, nchar(vl_data)),
			px_code   = if_else(
			   condition = !is.na(`No..`),
			   true      = `No..`,
			   false     = `HUB.NO.`
			)
		 ) %>%
		 select(-var_num, -vl_data) %>%
		 remove_empty(c("cols", "rows"))
   }

   if (hub == "bmc")
	  df %<>%
		 remove_empty(c("cols", "rows")) %>%
		 pivot_longer(
			cols      = c(starts_with("DATE"), starts_with("RESULT")),
			names_to  = "vl_var",
			values_to = "vl_data"
		 ) %>%
		 rename(PATIENT_ID = eHARP.Codes) %>%
		 mutate(
			var     = case_when(
			   stri_detect_fixed(vl_var, "DATE") ~ "vl_date",
			   stri_detect_fixed(vl_var, "RESULT") ~ "vl_result",
			),
			var_num = substr(vl_var, stri_locate_last_fixed(vl_var, "...") + 3, nchar(vl_var)) %>% as.numeric(),
			var_num = case_when(
			   vl_var == "DATE" ~ 1,
			   vl_var == "DATE.1" ~ 2,
			   vl_var == "RESULT" ~ 1,
			   vl_var == "RESULT.1" ~ 2,
			),
		 ) %>%
		 pivot_wider(
			id_cols     = c(PATIENT_ID, Patient.Code, SACCL, src_file, src_sheet, var_num),
			names_from  = var,
			values_from = vl_data
		 ) %>%
		 filter(!is.na(vl_date) | !is.na(vl_result)) %>%
		 select(-var_num)

   # hubs w/ many extra rows
   if (hub == "ccc")
	  df %<>%
		 filter(!is.na(UIC)) %>%
		 select(
			any_of(
			   c(
				  "hub",
				  "Client.Enrollment",
				  "uic",
				  "SACCL..Code..rHIVda.Code",
				  "Contact..",
				  "Hospital.Patient.Code",
				  "X.PhilHealth.Number",
				  "City.Municipality",
				  "Sex",
				  "Date.of.Birth..Day.Month.Year.",
				  "Age",
				  "Date.Started.on.ARV..Day.Month.Year.",
				  "ARV",
				  "Last.Date.of.ARV.Refill..Day.Month.Year.",
				  "Date.of.Next.Refill..Day.Month.Year.",
				  "CD4.Count",
				  "Viral.Load.Results",
				  "X.Virally.Supressed..Yes.No..",
				  "Actions.Taken",
				  "src_file",
				  "src_sheet"
			   )
			)
		 )
   if (hub == "chm")
	  df %<>%
		 filter(!is.na(UIC)) %>%
		 select(
			any_of(
			   c(
				  "hub",
				  "uic",
				  "X.Patient.s.Name",
				  "SACCL",
				  "Hospital.Patient.Code",
				  "City.Municipality",
				  "Sex",
				  "CD4.Count",
				  "Viral.Load.Results",
				  "Date.tested..Month.Day.Year.",
				  "src_file",
				  "src_sheet"
			   )
			)
		 )
   if (hub == "mey")
	  df %<>%
		 select(
			any_of(
			   c(
				  "hub",
				  "Patient.Code",
				  "SACCL.No.",
				  "Unique.Identifier.Code",
				  "Viral.Load",
				  "Date.Viral.Load.was.taken",
				  "Remarks",
				  "Name",
				  "CD4.count",
				  "ARV.Regimen",
				  "ART.Enrollment",
				  "src_file",
				  "src_sheet"
			   )
			)
		 )
   if (hub == "pjg")
	  df %<>%
		 select(
			any_of(
			   c(
				  "hub",
				  "UIC",
				  "PATIENT.CODE",
				  "CONFIRMATORY.SACCL.RHIVDA",
				  "X.Patient.s.Code",
				  "SACCL.rHIVda.No.",
				  "Unique.Identifier.Code",
				  "Viral.Load",
				  "Viral.Load.",
				  "Date.Viral.Load.was.taken",
				  "src_file",
				  "src_sheet"
			   )
			)
		 )
   if (hub == "ocp")
	  df %<>%
		 select(
			-any_of(
			   c(
				  "CD4.COUNT..........",
				  "DATE.DONE"
			   )
			)
		 )

   # # standardize dataset
   df %<>%
	  # clean strings
	  mutate_if(
		 .predicate = is.character,
		 ~str_squish(.) %>% if_else(. == "", NA_character_, .)
	  ) %>%
	  # remove empty vectors
	  remove_empty(which = c("cols", "rows")) %>%
	  mutate(
		 hub     = hub,
		 .before = 1
	  )

   if (hub == "mmc" && "Result" %in% names(df))
	  df %<>%
		 rename(vl_result = Result)

   for_rename <- intersect(nhsss$harp_vl$corr$config_xlsx$colnames$oldname, names(df))
   for (i in seq_len(nrow(nhsss$harp_vl$corr$config_xlsx$colnames))) {
	  old <- nhsss$harp_vl$corr$config_xlsx$colnames[i,]$oldname %>% as.character()
	  new <- nhsss$harp_vl$corr$config_xlsx$colnames[i,]$newname %>% as.character()

	  df %<>%
		 rename_all(
			~if_else(
			   condition = . == old,
			   true      = new,
			   false     = .,
			   missing   = .
			)
		 )
   }

   # special dates of tph
   if (hub %in% c("tph", "nmm"))
	  for (i in 2:nrow(df)) {
		 df[i, "vl_date"] <- ifelse(is.na(df[i, "vl_date"]), df[i - 1, "vl_date"], df[i, "vl_date"])
	  }

   if (hub == "mmc" && "SACCL.number" %in% names(df))
	  df %<>%
		 mutate(
			px_code = if_else(
			   condition = is.na(confirmatory_code) & !is.na(SACCL.number),
			   true      = SACCL.number,
			   false     = confirmatory_code,
			   missing   = confirmatory_code
			)
		 ) %>%
		 select(-SACCL.number)

   if (hub == "mnl" && "px_code_alt" %in% names(df))
	  df %<>%
		 mutate(
			px_code = if_else(
			   condition = !is.na(px_code) & !is.na(px_code_alt),
			   true      = paste(px_code_alt, px_code),
			   false     = px_code,
			   missing   = px_code
			)
		 )


   if (hub == "mrl" && !("vl_date" %in% names(df)))
	  df %<>%
		 mutate(
			vl_date   = substr(vl_result, stri_locate_last_fixed(vl_result, " ") + 1, nchar(vl_result)),
			vl_result = substr(vl_result, 1, stri_locate_last_fixed(vl_result, " ") - 1),
		 )

   if (hub == "jlg" && "px_code_alt" %in% names(df))
	  df %<>%
		 mutate(
			px_code = if_else(
			   condition = is.na(px_code) & !is.na(px_code_alt),
			   true      = px_code_alt,
			   false     = px_code,
			   missing   = px_code
			)
		 )

   # consolidate into a datafram
   xlsx_list[[hub]] <- df
   # vl_names         <- c(vl_names, names(df))
}

# consolidate and clean per column data
.log_info("Cleaning consolidated tables.")

xlsx_df <- bind_rows(xlsx_list)
for (var in c("baseline_vl_result", "vl_result_alt", "vl_date_alt", "px_code_alt", "SACCL..from.Masterlist."))
   if (!(var %in% names(xlsx_df)))
	  xlsx_df[var] <- NA_character_

xlsx_df %<>%
   filter_at(
	  .vars           = vars(vl_date, vl_result, vl_date_alt, vl_result_alt),
	  .vars_predicate = any_vars(!is.na(.))
   ) %>%
   mutate(
	  drop = case_when(
		 uic == "UIC" ~ 1,
		 vl_date == "SEPTEMBER" ~ 1,
		 vl_date == "N/A" ~ 1,
		 vl_result == "N/A" ~ 1,
		 vl_date == "Not Done" ~ 1,
		 vl_date == "Not done" ~ 1,
		 vl_date == "Date Performed" ~ 1,
		 vl_result == "Not Done" ~ 1,
		 vl_result == "Not done" ~ 1,
		 vl_result == "NO DATA" ~ 1,
		 px_code == "ACEMC-" &
			is.na(vl_date) &
			is.na(vl_result) ~ 1,
		 TRUE ~ 0
	  )
   ) %>%
   filter(drop == 0) %>%
   mutate(
	  # dates
	  vl_date           = str_squish(vl_date),
	  vl_date           = if_else(
		 condition = is.na(vl_date),
		 true      = str_squish(vl_date_alt),
		 false     = vl_date,
		 missing   = vl_date
	  ),
	  # vl_date_2         = case_when(
	  #    hub %in% c("agn", "blb", "bul", "jbl", "jlg", "mrh", "pgt", "prm", "r1m", "rdj", "rit", "slh", "tag", "tph", "mrs", "psy", "bic", "spm", "nmm") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "bkd" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "bkd" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "chm" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "chm" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "gcg" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "gcg" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), tryFormats = c("%Y-%m-%d", "%m-%d-%Y")),
	  #    hub == "gch" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "gch" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), tryFormats = c("%Y-%m-%d", "%m-%d-%Y")),
	  #    hub == "ggt" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "ggt" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), tryFormats = c("%Y-%m-%d", "%m-%d-%Y")),
	  #    hub == "ccc" ~ as.Date(substr(vl_result, 2, 11), "%m/%d/%Y"),
	  #    hub == "asm" ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "amp" ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%y"),
	  #    hub == "lkc" ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "jcp" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "jcp" & stri_detect_regex(vl_date, "[A-Z]") ~ as.Date(vl_date, "%B %e, %Y"),
	  #    hub == "jcp" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), tryFormats = c("%Y-%m-%d", "%m-%d-%Y")),
	  #    hub == "mey" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "mey" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "mmc" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "mmc" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "pjg" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "pjg" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "mnl" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "mnl" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "pmc" & stri_detect_fixed(vl_date, ".") ~ as.Date(substr(vl_date, 1, 10), "%m.%d.%Y"),
	  #    hub == "pmc" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "cdh" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "cdh" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "adh" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "adh" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  #    hub == "mrl" ~ as.Date(substr(vl_result, stri_locate_last_fixed(vl_result, " ") + 1, nchar(vl_result)), "%m/%d/%Y"),
	  #    hub == "ocp" & stri_detect_fixed(vl_date, "/") ~ as.Date(substr(vl_date, stri_locate_last_fixed(vl_date, " ") + 1, nchar(vl_date)), "%m/%d/%Y"),
	  #    hub == "ocp" & stri_detect_fixed(vl_date, "-") ~ as.Date(substr(vl_date, 1, 10), "%Y-%m-%d"),
	  # ),
	  # vl_date_2         = case_when(
	  #    hub %in% c("gch", "ggt") & is.na(vl_date_2) ~ as.Date(vl_date, "%m-%d-%Y"),
	  #    hub == "asm" & is.na(vl_date_2) ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%Y"),
	  #    hub == "amp" & is.na(vl_date_2) ~ as.Date(substr(vl_date, 1, 10), "%m/%d/%y"),
	  #    hub == "bul" &
	  #       StrIsNumeric(vl_date) &
	  #       is.na(vl_date_2) ~ excel_numeric_to_date(as.numeric(vl_date)),
	  #    hub == "bul" & vl_date == "43678" ~ as.Date("2019-08-01"),
	  #    TRUE ~ vl_date_2
	  # ),

	  # result
	  vl_result         = case_when(
		 hub == "nmm" & !is.na(vl_result_alt) ~ vl_result_alt,
		 hub == "prm" & is.na(vl_result) ~ Col6,
		 hub == "r1m" & is.na(vl_result) ~ baseline_vl_result,
		 hub == "jbl" & is.na(vl_result) ~ hiv_test_result,
		 TRUE ~ vl_result
	  ),

	  # confirmatory_code
	  confirmatory_code = case_when(
		 hub == "r1m" & is.na(confirmatory_code) ~ `SACCL..from.Masterlist.`,
		 TRUE ~ confirmatory_code
	  ),

	  # px_code
	  px_code           = case_when(
		 hub == "jcp" & !is.na(px_code_alt) ~ px_code_alt,
		 TRUE ~ px_code
	  ),

	  # name
	  name              = toupper(name),

	  # uic
	  uic               = stri_replace_all_fixed(uic, "-", "")
   ) %>%
   remove_empty(which = c("cols", "rows"))

for (i in seq_len(nrow(nhsss$harp_vl$corr$config_xlsx$colremove))) {
   remove <- nhsss$harp_vl$corr$config_xlsx$colremove[i,]$colname %>% as.character()

   if (remove %in% names(xlsx_df))
	  xlsx_df[, remove] <- NULL
}

##  Match w/ OHASIS Patient IDs ------------------------------------------------


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
   xlsxnames <- c('hub', 'cd4_date', 'uic', 'confirmatory_code', 'px_code', 'vl_date', 'vl_result', 'remarks', 'vl_result_alt', 'vl_date_alt', 'artstart_date', 'age', 'birthdate', 'sex', 'curr_addr', 'contact', 'latest_regimen', 'artstart_ddate', 'philhealth_no', 'latest_ffupdate', 'latest_nextpickup', 'cd4_result', 'vl_suppressed', 'actions_taken', 'name', 'px_code_alt', 'date_request', 'vl_code', 'date_receive', 'id', 'baseline_vl_result', 'outcome', 'vl_cat', 'source', 'drop', 'vl_date_2', 'row_id', 'status', 'PATIENT_ID', 'src_file', 'src_sheet', 'status', 'baseline_cd4_result')
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