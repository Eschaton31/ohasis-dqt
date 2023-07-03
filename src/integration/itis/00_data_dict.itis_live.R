tb_dummy <- function(sheet, file) {
   sample <- tibble()
   if (!(sheet %in% c("Reference", "Facility"))) {
      log_info(sheet)
      data <- read_xlsx(file, sheet, skip = 4, .name_repair = "unique_quiet") %>%
         slice(-1, -2) %>%
         mutate(
            data_col   = str_extract(`Data Element`, "[^\\(]*(?=\\))"),
            data_type  = case_when(
               Type %in% c("int", "bigint", "float") ~ "numeric",
               Type == "date" ~ "Date",
               Type == "datetime" ~ "POSIXct",
               Type %in% c("varchar", "char", "text", "longtext") ~ "character",
               is.na(Type) ~ "character"
            ),
            data_dummy = NA_character_
         ) %>%
         rowwise() %>%
         filter(!is.na(data_col)) %>%
         mutate(
            data_dummy = case_when(
               data_type == "numeric" ~ stri_rand_shuffle(stri_rand_strings(1, Size, "[0-9]")),
               data_type == "Date" ~ format(Sys.time(), "%Y-%m-%d"),
               data_type == "POSIXct" ~ format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
               data_type == "character" ~ stri_rand_shuffle(stri_rand_strings(1, Size, "[a-zA-Z]")),
            )
         ) %>%
         ungroup()

      rows <- seq_len(nrow(data))
      for (i in rows) {
         col_key  <- as.character(data[i, "data_col"])
         col_type <- as.character(data[i, "data_type"])
         col_size <- as.character(data[i, "Size"])
         for (i in 1:10) {
            col_val            <- case_when(
               col_type == "numeric" ~ suppress_warnings(stri_rand_strings(1, col_size, "[0-9]"), "NAs introduced"),
               col_type == "Date" ~ format(Sys.time(), "%Y-%m-%d"),
               col_type == "POSIXct" ~ format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
               col_type == "character" ~ suppress_warnings(stri_rand_strings(1, col_size, "[a-zA-Z]"), "NAs introduced"),
            )
            sample[i, col_key] <- suppress_warnings(eval(parse(text = glue("as.{col_type}('{col_val}')"))), "NAs introduced")
         }
      }
   }
   return(sample)
}

map_file   <- "C:/Users/Administrator/Downloads/Integrated Tuberculosis Information System v2 - Data Dictionary.xlsx"
map_tables <- excel_sheets(map_file)

itis               <- list()
itis$tables        <- lapply(map_tables, tb_dummy, file = map_file)
names(itis$tables) <- str_squish(map_tables)
