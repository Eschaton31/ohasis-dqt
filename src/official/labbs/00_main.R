labbs <- new.env()

##  Set reporting parameters to be used for the project ------------------------

# set paths and reporting period
local(envir = labbs, {
   params        <- list()
   params$gdrive <- as_id("17-JYnMzUvTOa59u72FkETmGC8vncD8WZ")
   params$mo     <- input(prompt = "What is the reporting month?", max.char = 2)
   params$mo     <- stri_pad_left(params$mo, width = 2, pad = "0")
   params$yr     <- input(prompt = "What is the reporting year?", max.char = 4)
   params$yr     <- stri_pad_left(params$yr, width = 4, pad = "0")
   params$ym     <- paste0(params$yr, ".", params$mo)
})

##  Set configurations and parameters for the files and reports ----------------

local(envir = labbs, {
   # get submissions
   params$gdrive_ym <- drive_ls(params$gdrive, params$ym)$id

   # configs
   config      <- list()
   config$src  <- as_id("1TNxJrnV_yjvvKW3Pb9eNuFfJphyl6CPyxuAOQPDo8sc")
   config$cols <- read_sheet(config$src, "cols")

   paths                <- drive_ls(params$gdrive_ym)
   params$gdrive_submit <- filter(paths, name == "Submissions")$id
   params$gdrive_valid  <- filter(paths, name == "Validations")$id
   rm(paths)

   dirs             <- list()
   dirs$local_files <- file.path(Sys.getenv("LABBS"), params$ym)

   reports <- list(
      faci_list = "List of Facilities",
      hiv       = "HIV",
      bbhiv     = "BLOODHIV",
      hepb      = "Hepatitis B",
      bbhepb    = "BLOODHepB",
      hepc      = "Hepatitis C",
      bbhepc    = "BLOODHepC",
      syp       = "Syphilis",
      bbsyp     = "BLOODSyph",
      gono      = "Gonorrhea"
   )

   files <- drive_ls(labbs$params$gdrive_submit) %>%
      mutate(local = file.path(dirs$local_files, name))
})

##  Get and process files for the reporting period -----------------------------

# download files into local directory
local(envir = labbs, {
   log_warn("Downloading submissions from GDrive.")
   invisible(apply(files, 1, function(row) {
      log_info("Downloading {green(row$name)}.")
      local_drive_quiet()
      drive_download(as_id(row$id), row$local, overwrite = TRUE)
   }))
   log_success("Done downloading!")
})

# import excel data and process per sheet
labbs$data        <- lapply(seq_along(labbs$reports), function(i, env) {
   # import each region as lists of all region with submissions on the specific
   # diseases. sheet_data is initially a list object
   log_warn("Sheet: {red(env$reports[[i]])}")
   sheet_data <- lapply(env$files$local, labbs_sheet, names(env$reports)[[i]], env$reports[[i]], env)

   # bind all datasets into a single dataframe, each sheet will have all data
   # for that specific disease across all regions
   sheet_data <- bind_rows(sheet_data)

   log_success("Done!")
   return(sheet_data)
}, labbs)
names(labbs$data) <- names(labbs$reports)

# clean facilities reference list
labbs$data$faci_list %<>%
   distinct(region, province, muncity, faci_name, faci_type, faci_status, pubpriv, .keep_all = TRUE) %>%
   mutate_if(
      .predicate = is.character,
      ~toupper(str_squish(.))
   )

# convert the bound datasets into long format;
# each row will be a combination of indicator, per month, per disease, per facility;
# facilities list sheet (first sheet) will be excluded from conversion
labbs$long <- lapply(labbs$data[2:10], function(data) {
   long <- data %>%
      pivot_longer(
         cols     = starts_with("x"),
         names_to = "month_var"
      ) %>%
      mutate(
         # extract variable name from grouping variable
         var   = substr(month_var, 5, 1000),

         # extract month from grouping variable
         month = substr(month_var, 2, 3),

         # tag data to be dropped
         drop  = case_when(
            faci_name == "#ERROR!" ~ 1,
            faci_name == "" ~ 1,
            TRUE ~ 0
         )
      ) %>%
      filter(drop == 0) %>%
      mutate_at(
         .vars = vars(faci_name, region, province, var),
         ~toupper(str_squish(.))
      ) %>%
      # only keep columns that are relevant for analysis
      select(
         any_of(c(
            "reg_faci_id",
            "faci_name",
            "region",
            "province",
            "muncity",
            "labbs_disease",
            "pubpriv",
            "faci_status",
            "var",
            "month",
            "value"
         ))
      ) %>%
      # keep only rows that start with num; these are the indicator columns
      filter(grepl("^NUM", var)) %>%
      mutate(
         # tag per quarter
         qr    = case_when(
            month %in% c("01", "02", "03") ~ "Q1",
            month %in% c("04", "05", "06") ~ "Q2",
            month %in% c("07", "08", "09") ~ "Q3",
            month %in% c("10", "11", "12") ~ "Q4",
         ),

         # initial cleaning for non-numeric data that were accidentally included
         # in the final consolidation sheets
         value = str_squish(value),
         value = gsub("\\.0$", "", value),
         value = case_when(
            value == "c" ~ NA_character_,
            value == "TRUE" ~ NA_character_,
            value == "FALSE" ~ NA_character_,
            value == ":" ~ NA_character_,
            grepl("^[a-zA-Z]+$", value) ~ NA_character_,
            TRUE ~ value
         ),
         value = as.numeric(value)
      )
})

labbs$agg    <- list()
labbs$agg$qr <- lapply(labbs$long, labbs_agg, c("faci_name", "region", "province", "qr"), labbs$config)
labbs$agg$yr <- lapply(labbs$long, labbs_agg, c("faci_name", "region", "province"), labbs$config)

labbs$check$qr <- lapply(labbs$agg$qr, function(data) {
   col_preg  <- data %>% get_names("_PREG$")
   review_df <- data %>%
      mutate(with_issue = 0)

   for (i in seq_along(col_preg)) {
      preg <- col_preg[i] %>% as.name()
      f    <- gsub("_PREG$", "_F", preg) %>% as.name()

      review_df %<>%
         mutate(
            with_issue = case_when(
               {{ preg }} > {{ f }} ~ 1,
               TRUE ~ with_issue
            )
         )
   }

   return(review_df %>% filter(with_issue == 1))
})

labbs_review <- bind_rows(labbs$long) %>%
   distinct(region, province, faci_name)

labbs$data$faci_list %>%
   select(region, province, muncity, faci_name, faci_type, faci_status, pubpriv) %>%
   write_sheet("1TNxJrnV_yjvvKW3Pb9eNuFfJphyl6CPyxuAOQPDo8sc", "for_review_tebers")

ohasis$ref_faci %>%
   select(
      FACI_ID,
      SUB_FACI_ID,
      FACI_NAME,
      EMAIL,
      MOBILE,
      LANDLINE,
      FACI_PSGC_REG,
      FACI_PSGC_PROV,
      FACI_PSGC_MUNC,
      FACI_NAME_REG,
      FACI_NAME_PROV,
      FACI_NAME_MUNC,
      FACI_ADDR
   ) %>%
   write_sheet("1TNxJrnV_yjvvKW3Pb9eNuFfJphyl6CPyxuAOQPDo8sc", "ref_faci")