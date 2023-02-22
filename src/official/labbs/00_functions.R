labbs_sheet <- function(xl, disease, sheet_name) {
   # extract region from filename
   # i.e., 20230222_labbs_region-NCR_2022-12.xlsx
   filename <- strsplit(basename(xl), "_")[[1]]
   region   <- gsub("region-", "", filename[3])
   log_info("Processing {green(basename(xl))}.")

   data <- tibble()
   if (sheet_name %in% excel_sheets(xl)) {
      # read excel file into a dataframe, copy over data for merged cells
      # NOTE: Do not use the first row as headers
      data <- read.xlsx(xl, sheet = sheet_name, fillMergedCells = TRUE, skipEmptyCols = FALSE, skipEmptyRows = FALSE, colNames = FALSE) %>%
         as_tibble() %>%
         mutate(row_id = row_number())

      # extract values of the first three columns into separate vectors;
      # this is done to check which columns the facilities places the "Name of Facility";
      # this is also the basis for which row/s will be used as reference for colnames;
      val_col1 <- (data %>%
         distinct(X1) %>%
         mutate_all(~str_trim(toupper(.))))$X1
      val_col2 <- (data %>%
         distinct(X2) %>%
         mutate_all(~str_trim(toupper(.))))$X2
      val_col3 <- (data %>%
         distinct(X3) %>%
         mutate_all(~str_trim(toupper(.))))$X3

      # find out which columns the markers first appear on;
      # type of facility was included as some regions were noted to have deleted
      # the "Name of Facility" column
      col_of_name <- NA
      if ("NAME OF FACILITY" %in% val_col1) col_of_name <- 1
      if ("NAME OF FACILITY" %in% val_col2) col_of_name <- 2
      if ("NAME OF FACILITY" %in% val_col3) col_of_name <- 3

      col_of_type <- NA
      if ("TYPE OF FACILITY" %in% val_col1) col_of_type <- 1
      if ("TYPE OF FACILITY" %in% val_col2) col_of_type <- 2
      if ("TYPE OF FACILITY" %in% val_col3) col_of_type <- 3

      col_of_bb <- NA
      if ("NAME OF BLOODBANK" %in% val_col1) col_of_bb <- 1
      if ("NAME OF BLOODBANK" %in% val_col2) col_of_bb <- 2
      if ("NAME OF BLOODBANK" %in% val_col3) col_of_bb <- 3

      col_faci_name <- coalesce(col_of_bb, col_of_name, col_of_type)

      # extract row number for colnames ref
      row_name  <- data %>%
         filter(str_trim(toupper(.[[col_faci_name]])) %in% c("NAME OF BLOODBANK", "NAME OF FACILITY", "TYPE OF FACILITY"))
      row_name  <- as.integer(row_name[1, 'row_id'])
      row_total <- nrow(data)

      # remove all rows that came before the colnames row
      data %<>%
         slice(seq(row_name, row_total))

      # tag which version of the labbs form they are using;
      # > 4 rows = old
      # > 3 rows = new
      form_version <- as.character(data[3, 3])
      if (form_version == "*Please indicate the corresponding number")
         data %<>%
            filter(row_number() != 3)

      # extract names from each row
      row1 <- toupper(as.character(str_squish(data[1,])))
      row2 <- toupper(as.character(str_squish(data[2,])))
      row3 <- toupper(as.character(str_squish(data[3,])))

      # clean the names to be used; convert to snake-case
      row1 <- case_when(
         row1 == "NAME OF FACILITY" ~ "faci_name",
         row1 == "TYPE OF FACILITY" ~ "faci_type",
         row1 == "OWNERSHIP" ~ "ownership",
         row1 == "MUN/CITY" ~ "muncity",
         row1 == "PROVINCE" ~ "province",
         row1 == "REMARKS" ~ "remarks",
         !is.na(match(row1, toupper(month.name))) ~ stri_pad_left(match(row1, toupper(month.name)), 2, "0"),
         TRUE ~ row1
      )

      # read the column renaming from labbs_config, limit to the columns for the
      # current sheet
      sheet_cols <- filter(labbs$config$cols, sheet == disease)
      for (i in seq_len(nrow(sheet_cols))) {
         xl_col <- as.character(sheet_cols[i, "xl_col"])
         r_col  <- as.character(sheet_cols[i, "r_col"])
         row2   <- ifelse(row2 == xl_col, r_col, row2)
      }
      row2 <- case_when(
         row2 == "NAME OF FACILITY" ~ "faci_name",
         row2 == "TYPE OF FACILITY" ~ "faci_type",
         row2 == "OWNERSHIP" ~ "ownership",
         row2 == "MUN/CITY" ~ "muncity",
         row2 == "PROVINCE" ~ "province",
         row2 == "REMARKS" ~ "remarks",
         TRUE ~ row2
      )
      log_warn("row2")
      print(unique(row2))

      row3 <- case_when(
         grepl("^OFFICIAL NAME", row3) ~ NA_character_,
         grepl("^1 - HOSPITAL LABORATOR", row3) ~ NA_character_,
         grepl("^INDICATE MUNICIPALITY", row3) ~ "muncity",
         grepl("^INDICATE PROVINCE OF THE FACILITY", row3) ~ "province",
         grepl("^INDICATE WHETHER FACIITY CLOSED", row3) ~ "faci_status",
         row3 == "1 - GOVERNMENT; 2 - PRIVATE" ~ "pubpriv",
         row3 == "MALE" ~ "m",
         row3 == "M" ~ "m",
         row3 == "FEMALE (TOTAL FEMALES)" ~ "f",
         row3 == "FEMALE &#10;(TOTAL FEMALES)" ~ "f",
         row3 == "TOTAL F" ~ "f",
         row3 == "PREGNANTA" ~ "preg",
         row3 == "PREGNANT" ~ "preg",
         row3 == "TOTALB" ~ "total",
         row3 == "TOTAL" ~ "total",
         TRUE ~ row3
      )

      # concatenate all rows into one name each, then assign to the dataset
      final_names <- paste(row1, row2, row3, sep = "_")
      final_names <- case_when(
         final_names == "1_2_3" ~ "last_col",
         final_names == "5_6_8" ~ "last_col",
         grepl("^faci_name", final_names) ~ "faci_name",
         grepl("^faci_type", final_names) ~ "faci_type",
         grepl("^ownership", final_names) ~ "pubpriv",
         grepl("^province", final_names) ~ "province",
         grepl("^muncity", final_names) ~ "muncity",
         grepl("^remarks", final_names) ~ "faci_status",
         TRUE ~ final_names
      )
      names(data) <- final_names

      # final cleaning of dataset and conversion to standard format
      data %<>%
         clean_names() %>%
         mutate_all(~as.character(.))

      # remove header rows
      data %<>%
         filter(row_number() > 3)

      # generate a labbs facility id for those regions who removed the first column
      if (col_faci_name == 1)
         data %<>%
            mutate(
               reg_faci_id = row_number(),
               .before     = 1
            )

      # final cleaning and ordering of columns
      data %<>%
         rename(
            reg_faci_id = 1,
            faci_name   = 2,
            faci_type   = 3
         ) %>%
         filter(!is.na(reg_faci_id), faci_name != "0") %>%
         mutate(region = region) %>%
         relocate(any_of(c("region", "province", "muncity")), .after = reg_faci_id) %>%
         remove_empty("cols")

   }
   return(data)
}
