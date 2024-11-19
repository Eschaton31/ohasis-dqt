##  Set configurations ---------------------------------------------------------

# list of current vars for code cleanup
vlml    <- new.env()
currEnv <- ls()[ls() != "currEnv"]

local(envir = vlml, {
   params    <- list()
   params$mo <- input(prompt = "What is the reporting month?", max.char = 2)
   params$mo <- stri_pad_left(params$mo, width = 2, pad = "0")
   params$yr <- input(prompt = "What is the reporting year?", max.char = 4)
   params$yr <- stri_pad_left(params$yr, width = 4, pad = "0")
   params$ym <- paste0(params$yr, ".", params$mo)
})

##  Download reference datasets ------------------------------------------------

local(envir = vlml, {
   local_gs4_quiet()
   config       <- read_sheet("1Yj-qP7sA8k-X0L9UHoXNl-TmBkPMfLkUIOlWNGJdENo", "eb_vl_ml") %>%
      filter(yr.mo == params$ym)
   px_id        <- lapply(sheet_names("1jaXjBjfWy6QsL4wcxFT6_LXYnEw5Unx-hAZUq2WIWiI"), function(sheet) {
      data <- read_sheet("1jaXjBjfWy6QsL4wcxFT6_LXYnEw5Unx-hAZUq2WIWiI", sheet)
      return(data)
   })
   names(px_id) <- sheet_names("1jaXjBjfWy6QsL4wcxFT6_LXYnEw5Unx-hAZUq2WIWiI")
})

##  Set directories ------------------------------------------------------------

local(envir = vlml, {
   dir         <- list()
   dir$dropbox <- file.path(Sys.getenv("DRIVE_DROPBOX"), "File requests/HARP Form Submission/VL Masterlist", params$ym)
   dir$local   <- file.path(Sys.getenv("HARP_VL"), "..", "ml", params$ym)
   invisible(lapply(dir, check_dir))
})

##  Get submitted masterlists---------------------------------------------------

local(envir = vlml, {
   files <- dir_info(dir$dropbox) %>%
      mutate(
         filename  = basename(path),
         file_type = tools::file_ext(path)
      )
   files <- split(files, ~file_type)
   data  <- list()
   refs  <- list()
   check <- list()
})

##  Import data from .pdf files ------------------------------------------------

log_info("Importing .pdf masterlists.")
local(envir = vlml, {
   if ("pdf" %in% names(files)) {
      refs$pdf <- files$pdf %>%
         left_join(
            y  = config,
            by = "filename"
         )

      data$pdf <- lapply(seq_len(nrow(refs$pdf)), function(i, ref) {
         hub_code <- ref[i,]$hub_code

         pdf         <- tabulizer::extract_tables(file = ref[i,]$path, method = "lattice")
         data        <- lapply(seq_len(length(pdf)), function(i) as.data.frame(pdf[[i]]))
         data        <- bind_rows(data)
         names(data) <- unlist(data[1,])
         data        <- data[-1,]
         if (hub_code == "VRH") {
            data %<>%
               rename_all(
                  ~case_when(
                     . == "SACCL CODE" ~ "confirmatory_code",
                     . == "UIC" ~ "uic",
                     . == "PATIENT CODE" ~ "px_code",
                     . == "LATEST VL DATE" ~ "vl_date",
                     . == "LATEST VL RESULT" ~ "vl_result",
                     TRUE ~ .
                  )
               ) %>%
               mutate(
                  uic       = gsub("/", "", uic),
                  vl_date_2 = as.character(as.Date(vl_date, "%d-%b-%y")),
                  row_id    = paste(sep = "_", "pdf", hub_code, ref[i,]$path, row_number())
               )
         }

         data <- data %>%
            mutate(hub = hub_code)

      }, ref = refs$pdf)
      data$pdf <- bind_rows(data$pdf)
   }
})

##  Import data from .xlsx files -----------------------------------------------

log_info("Importing .xls* masterlists.")
local(envir = vlml, {
   refs$xlsx <- files$xlsx %>%
      left_join(
         y  = config,
         by = "filename"
      ) %>%
      mutate(sheets = NA_character_)

   for (i in seq_len(nrow(refs$xlsx))) {
      password <- refs$xlsx[i,]$password
      file     <- refs$xlsx[i,]$path
      if (!is.na(password)) {
         refs$xlsx[i,]$sheets <- paste(collapse = ", ", XLConnect::getSheets(XLConnect::loadWorkbook(file, password = password)))
      } else {
         refs$xlsx[i,]$sheets <- paste(collapse = ", ", excel_sheets(file))
      }
   }
   XLConnect::xlcFreeMemory()
   rm(i, password, file)

   if (nrow(refs$xlsx %>% filter(is.na(import_sheets)))) {
      log_warn("VL MLs not yet included in config.")
      check$no_config <- filter(refs$xlsx, is.na(import_sheets))
   }

   data$xlsx <- lapply(seq_len(nrow(refs$xlsx)), function(i, ref) {
      hub_code  <- ref[i,]$hub_code
      password  <- ifelse(!is.na(ref[i,]$password), ref[i,]$password, "")
      start_row <- ifelse(!is.na(ref[i,]$start_row), ref[i,]$start_row, 4)
      start_col <- ifelse(!is.na(ref[i,]$start_col), ref[i,]$start_col, 1)
      sheets    <- ifelse(!is.na(ref[i,]$import_sheets), ref[i,]$import_sheets, '')
      sheets    <- ifelse(sheets == "", 1, strsplit(sheets, ", ")[[1]])

      if (password == "") {
         data <- lapply(sheets, function(sheet) {
            data <- read_xlsx(ref[i,]$path, col_types = "text", skip = start_row - 1, sheet = sheet, .name_repair = "unique_quiet") %>%
               remove_empty(c("rows", "cols")) %>%
               mutate(
                  row_id = paste(sep = "_", "xlsx", hub_code, ref[i,]$path, sheet, row_number())
               )
            return(data)
         })
         data %<>%
            bind_rows() %>%
            mutate(hub = hub_code)
      } else {
         wb <- XLConnect::loadWorkbook(ref[i,]$path, password = password)

         data <- lapply(sheets, function(sheet) {
            data <- wb %>%
               XLConnect::readWorksheet(
                  sheet,
                  colTypes = XLC$DATA_TYPE.STRING,
                  startRow = start_row,
                  startCol = start_col,
                  header   = TRUE
               ) %>%
               remove_empty(c("rows", "cols")) %>%
               mutate(
                  row_id = paste(sep = "_", "xlsx", hub_code, sheet, row_number())
               )
            return(data)
         })
         data %<>%
            bind_rows() %>%
            mutate(hub = hub_code)

         rm(wb)
         XLConnect::xlcFreeMemory()
      }

      data %<>%
         rename_all(
            ~case_when(
               . == "Patient code" ~ "px_code",
               . == "Patient Code" ~ "px_code",
               . == "FACILITY CODE" ~ "px_code",
               . == "FACILITY.CODE" ~ "px_code",
               . == "Patient.code" ~ "px_code",
               . == "Confirmatory code" ~ "confirmatory_code",
               . == "Confirmatory Code" ~ "confirmatory_code",
               . == "Confirmatory.code" ~ "confirmatory_code",
               . == "CONFIRAMATORY CODE" ~ "confirmatory_code",
               . == "CONFIRAMATORY.CODE" ~ "confirmatory_code",
               . == "CONFIRMATORY CODE" ~ "confirmatory_code",
               . == "CONFIRMATORY.CODE" ~ "confirmatory_code",
               . == "SACCL CODE" ~ "confirmatory_code",
               . == "SACCL Code" ~ "confirmatory_code",
               . == "Full Name" ~ "name",
               . == "Full name" ~ "name",
               . == "Full.Name" ~ "name",
               . == "UIC" ~ "uic",
               . == "Sex" ~ "sex",
               . == "ART Start Date" ~ "artstart_date",
               . == "ART.Start.Date" ~ "artstart_date",
               . == "Latest_Visit" ~ "latest_ffupdate",
               . == "Latest_Regimen" ~ "latest_regimen",
               . == "Latest Regimen" ~ "latest_regimen",
               . == "Outcome" ~ "outcome",
               . == "DATE OF VIRAL LOAD" ~ "vl_date",
               . == "Viral.load.date" ~ "vl_date",
               . == "Latest VL Date" ~ "vl_date",
               . == "Viral load date" ~ "vl_date",
               . == "Viral Load Date" ~ "vl_date",
               . == "Viral Load date" ~ "vl_date",
               . == "VIRAL LOAD DATE TESTED" ~ "vl_date",
               . == "Date.of.Viral.Load.Test" ~ "vl_date",
               . == "Viral load result" ~ "vl_result",
               . == "Viral Load Result" ~ "vl_result",
               . == "Viral.load.result" ~ "vl_result",
               . == "Latest VL Result" ~ "vl_result",
               . == "Latest VL Result" ~ "vl_result",
               . == "Viral.Load.Result." ~ "vl_result",
               . == "RESULT" ~ "vl_result",
               . == "If baseline viral load test, put Y" ~ "baseline_vl",
               . == "If.baseline.viral.load.test..put.Y" ~ "baseline_vl",
               . == "Remarks" ~ "remarks",
               . == "REMARKS" ~ "remarks",
               . == "Remarks." ~ "remarks",
               . == "Birth date" ~ "birthdate",
               . == "Birth Date" ~ "birthdate",
               TRUE ~ .
            )
         )

      return(data)
   }, ref = refs$xlsx)
   data$xlsx <- bind_rows(data$xlsx) %>%
      mutate(
         drop = case_when(
            confirmatory_code == "DOH-ABC-12345" ~ 1,
            uic == "DONE UPDATING OHASIS" ~ 1,
            TRUE ~ 0
         ),
      ) %>%
      filter(drop == 0)

   if ("First Name" %in% names(data$xlsx)) {
      data$xlsx %<>%
         mutate(
            name = if_else(
               is.na(name) & !is.na(`First Name`),
               paste0(
                  if_else(
                     condition = is.na(`Last Name`),
                     true      = "",
                     false     = `Last Name`
                  ), ", ",
                  if_else(
                     condition = is.na(`First Name`),
                     true      = "",
                     false     = `First Name`
                  ), " ",
                  if_else(
                     condition = is.na(`Middle Name`),
                     true      = "",
                     false     = `Middle Name`
                  ), " ",
                  if ("Name Ext." %in% names(data$xlsx)) {
                     if_else(
                        condition = is.na(`Name Ext.`),
                        true      = "",
                        false     = `Name Ext.`
                     )
                  }
               ),
               name,
               name
            ) %>% str_squish()
         )
   }
})

##  Match w/ OHASIS Patient IDs ------------------------------------------------

local(envir = vlml, {
   conso <- bind_rows(data) %>%
      mutate(vl_date_2 = vl_date) %>%
      mutate_at(
         .vars = vars(name, confirmatory_code, uic, px_code),
         ~str_squish(toupper(.))
      ) %>%
      mutate(
         confirmatory_code = if_else(str_squish(confirmatory_code) == "", NA_character_, confirmatory_code, confirmatory_code),
         uic               = stri_replace_all_fixed(uic, "-", ""),
         sex               = str_left(sex, 1),
         hub               = tolower(hub),
         PATIENT_ID        = NA_character_
      ) %>%
      relocate(hub, px_code, confirmatory_code, name, uic, .before = 1)

   for (var in c("latest_ffupdate", "artstart_date", "vl_date_2", "birthdate")) {
      if (var %in% names(conso))
         conso %<>%
            mutate(
               date_num = as.numeric(!!as.name(var))
            ) %>%
            rowwise() %>%
            mutate(
               first_dash = stri_locate_first_fixed(!!as.name(var), "-")[1, 1]
            ) %>%
            ungroup() %>%
            mutate(
               !!as.name(var) := case_when(
                  grepl(paste(collapse = "|", toupper(month.abb[1:12])), !!as.name(var)) ~ as.Date(!!as.name(var), "%d-%b-%y"),
                  !is.na(date_num) ~ excel_numeric_to_date(date_num),
                  !!as.name(var) %in% c("NULL", "N/A", "NA") ~ NA_Date_,
                  !!as.name(var) == "03/182022" ~ as.Date("2022-03-18"),
                  !!as.name(var) == "1--2-2021" ~ as.Date("2021-01-02"),
                  !!as.name(var) == "12/072022" ~ as.Date("2022-12-07"),
                  first_dash %in% c(2, 3) ~ as.Date(!!as.name(var), "%m-%d-%Y"),
                  grepl("/", !!as.name(var)) ~ as.Date(!!as.name(var), "%m/%d/%Y"),
                  grepl("\\.", !!as.name(var)) ~ as.Date(!!as.name(var), "%m.%d.%Y"),
                  TRUE ~ as.Date(!!as.name(var), "%Y-%m-%d")
               )
            ) %>%
            select(-first_dash, -date_num)
   }
   rm(var)
   if (nrow(conso %>% filter(is.na(vl_date_2), !is.na(vl_date)))) {
      log_warn("Records w/ unconverted dates still exist.")
      check$date_format <- filter(conso, is.na(vl_date_2), !is.na(vl_date))
   }

   if ("birthdate" %in% names(conso))
      conso %<>% mutate(birthdate = as.character(birthdate))

   for (i in seq_len(length(px_id))) {
      merge_ids <- names(px_id[[i]])
      merge_ids <- merge_ids[merge_ids != "PATIENT_ID"]
      merge_ids <- intersect(merge_ids, names(conso))
      log_info("Merge IDs: {green(stri_c(collapse = ', ', merge_ids))}")
      curr_pass <- as.symbol(glue("id_pass_{i}"))

      if (length(merge_ids) > 0)
         conso %<>%
            left_join(
               y  = px_id[[i]] %>%
                  as.data.frame() %>%
                  distinct(select(., merge_ids), .keep_all = TRUE) %>%
                  rename(
                     !!curr_pass := PATIENT_ID
                  ),
               by = merge_ids
            ) %>%
            mutate(
               PATIENT_ID = coalesce(PATIENT_ID, !!curr_pass)
            ) %>%
            select(-starts_with("id_pass"))
   }
   rm(i, merge_ids, curr_pass)
   conso %<>% distinct(row_id, .keep_all = TRUE)

   if (nrow(conso %>% filter(is.na(PATIENT_ID)))) {
      log_warn("Records w/o Patient IDs still exist.")
      check$no_pid <- filter(conso, is.na(PATIENT_ID))
   }
})

##  Slightly clean results -----------------------------------------------------
local(envir = vlml, {
   conso %<>%
      process_vl("vl_result", "vl_result_2") %>%
      mutate(
         drop = if_else(
            is.na(vl_date) & is.na(vl_result),
            1,
            drop,
            drop
         )
      ) %>%
      as_tibble()

   conso %>%
      filter(
         VL_DROP == 0,
         VL_ERROR == 0,
         is.na(vl_result)
      ) %>%
      distinct(vl_result, vl_result_2)

   check$res_nodate <- filter(conso, is.na(vl_date), !is.na(vl_result))
})

##  Save data ------------------------------------------------------------------

save <- input(
   prompt  = "Create `.dta` file?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
if (save == "1")
   local(envir = vlml, {
      conso %>%
         select(
            -drop,
            -vl_date,
            -any_of(c("Last Name", "First Name", "Middle Name", "Name Ext.", "Latest Regimen")),
            -contains("."),
            -contains("-"),
            -contains("/"),
            -matches("^[0-9]")
         ) %>%
         rename(
            vl_date          = vl_date_2,
            LAB_VIRAL_RESULT = vl_result,
            vl_result        = vl_result_2,
         ) %>%
         mutate_if(
            .predicate = is.Date,
            ~as.character(.)
         ) %>%
         select(
            -contains(" "),
            -contains("."),
            -contains("\r\n"),
         ) %>%
         mutate(vlml2022 = 1) %>%
         write_dta(file.path(Sys.getenv("HARP_VL"), paste0(format(Sys.time(), "%Y%m%d"), "_vl_ml_", params$yr, "-", params$mo, ".dta")))
   })

log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))