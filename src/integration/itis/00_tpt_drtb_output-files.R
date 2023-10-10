################################################################################
# Project name: > Data Exploration - DRTB+TPT Output Files
# Author(s):    > Palo, John Benedict
# Date:         > 2023-07-01
################################################################################

##  SETUP ----------------------------------------------------------------------

# ! libraries
if (!require(pacman))
   install.packages("pacman")

# load libraries
library(pacman)
p_load(
   rJava,
   XLConnect,
   dplyr,
   magrittr,
   lubridate,
   arrow,
   crayon,
   glue,
   fastLink
)

# ! objects
itis <- list(
   tpt  = list(
      xl  = "D:/Downloads/Documents/2023_TPT_Patient_List_AS_OF_JULY_01_2023.xlsx",
      pw  = "@cc3$$@2023D@T@",
      ver = "2023.07.01"
   ),
   drtb = list(
      xl  = "D:/Downloads/Documents/2023_DRTB_Patient_List_AS_OF_JULY_01_2023.xlsx",
      pw  = "@cc3$$@2023D@T@",
      ver = "2023.07.01"
   )
)

# ! functions
load_itis_output <- function(config) {
   # read workbook using password
   wb <- XLConnect::loadWorkbook(config$xl, password = config$pw)

   # read all sheets
   sheets      <- XLConnect::getSheets(wb)
   data        <- lapply(sheets, function(sheet) {
      log_info("Processing {green(sheet)}.")
      sheet_data <- XLConnect::readWorksheet(wb, sheet, colTypes = XLC$DATA_TYPE.STRING)
      return(sheet_data)
   })
   names(data) <- sheets

   # free up memory reserved to java
   rm(wb)
   XLConnect::xlcFreeMemory()

   # initial cleaning of data
   data <- bind_rows(data) %>%
      rename_all(
         ~str_replace_all(., "\\.$", "") %>%
            str_replace_all("\\.", "_") %>%
            toupper() %>%
            str_squish()
      ) %>%
      rename(
         ITIS_PATIENT_ID = CASE_ID_NUMBER
      ) %>%
      mutate_at(
         .vars = vars(contains("_DATE"), contains("DATE_")),
         ~as.Date(substr(., 1, 10), "%Y-%m-%d")
      ) %>%
      mutate(
         # remove excel formatting that puts a single quote "'" in front of
         # numbers formatted as text
         ITIS_PATIENT_ID = str_replace(ITIS_PATIENT_ID, "^'", "")
      )

   return(data)
}

itis_cid <- function(linelist, cid_list, pid_col) {
   join_col <- deparse(substitute(pid_col))
   # finalize a central_id columns
   linelist %<>%
      select(-matches("ITIS_CENTRAL_ID", ignore.case = FALSE)) %>%
      left_join(
         y  = cid_list %>%
            select(
               ITIS_CENTRAL_ID,
               {{pid_col}} := ITIS_PATIENT_ID
            ),
         by = join_col
      ) %>%
      mutate(
         ITIS_CENTRAL_ID = if_else(
            condition = is.na(ITIS_CENTRAL_ID),
            true      = {{pid_col}},
            false     = ITIS_CENTRAL_ID
         ),
      ) %>%
      relocate(ITIS_CENTRAL_ID, .before = {{pid_col}})

   return(linelist)
}

itis_dedup_prep <- function(
   data = NULL,
   name_f = NULL,
   name_m = NULL,
   name_l = NULL,
   name_s = NULL,
   BIRTH_DATE = NULL,
   phic = NULL,
   code_px = NULL
) {
   dedup_new <- data %>%
      mutate(
         LAST          = stri_trans_general(stri_trans_toupper({{name_l}}), "latin-ascii"),
         MIDDLE        = stri_trans_general(stri_trans_toupper({{name_m}}), "latin-ascii"),
         FIRST         = stri_trans_general(stri_trans_toupper({{name_f}}), "latin-ascii"),
         SUFFIX        = stri_trans_general(stri_trans_toupper({{name_s}}), "latin-ascii"),
         PHILHEALTH_NO = stri_trans_general(stri_trans_toupper({{phic}}), "latin-ascii"),
         PATIENT_CODE  = stri_trans_general(stri_trans_toupper({{code_px}}), "latin-ascii"),
      ) %>%
      mutate(
         # get components of BIRTH_DATE
         BIRTH_YR     = year({{BIRTH_DATE}}),
         BIRTH_MO     = month({{BIRTH_DATE}}),
         BIRTH_DY     = day({{BIRTH_DATE}}),

         # variables for first 3 letters of names
         FIRST_A      = substr(FIRST, 1, 3),
         MIDDLE_A     = substr(MIDDLE, 1, 3),
         LAST_A       = substr(LAST, 1, 3),

         LAST         = coalesce(LAST, MIDDLE),
         MIDDLE       = coalesce(MIDDLE, LAST),

         # clean ids
         FIRST_SIEVE  = str_replace_all(FIRST, "[^[:alnum:]]", ""),
         MIDDLE_SIEVE = str_replace_all(MIDDLE, "[^[:alnum:]]", ""),
         LAST_SIEVE   = str_replace_all(LAST, "[^[:alnum:]]", ""),
         PHIC         = str_replace_all(PHILHEALTH_NO, "[^[:alnum:]]", ""),
         PXCODE_SIEVE = str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""),
      ) %>%
      mutate_at(
         .vars = vars(ends_with("_SIEVE", ignore.case = TRUE), PHIC),
         ~str_replace_all(., "([[:alnum:]])\\1+", "\\1")
      ) %>%
      mutate(
         # code standard names
         FIRST_NY  = suppress_warnings(nysiis(FIRST_SIEVE, stri_length(FIRST_SIEVE)), "unknown characters"),
         MIDDLE_NY = suppress_warnings(nysiis(MIDDLE_SIEVE, stri_length(MIDDLE_SIEVE)), "unknown characters"),
         LAST_NY   = suppress_warnings(nysiis(LAST_SIEVE, stri_length(LAST_SIEVE)), "unknown characters"),
      )

   return(dedup_new)
}

itis_unique <- function(refs) {
   # columns to be included in the final linelist
   cols <- c(
      "LAST_NAME",
      "FIRST_NAME",
      "MIDDLE_NAME",
      "NAME_EXTENSION",
      "BIRTH_DATE",
      "SEX",
      "CONTACT_NUMBER",
      "RELIGION",
      "NATIONALITY",
      "PHILHEALTH_NUMBER",
      "PATIENT_CODE",
      "CONTACT_NUMBER",
      "RELIGION",
      get_names(refs$patient, "PERM_"),
      get_names(refs$patient, "CURR_"),
      get_names(refs$patient, "TX_")
   )

   # arrange descendingly based on latest record
   refs$patient %<>%
      ungroup() %>%
      itis_cid(refs$registry, ITIS_PATIENT_ID) %>%
      arrange(desc(LAST_UPDATED))

   # get latest non-missing data from column
   refs$vars <- list()
   for (col in cols) {
      log_info("Getting latest data for {green(col)}.")
      col_name         <- as.name(col)
      refs$vars[[col]] <- refs$patient %>%
         select(
            ITIS_CENTRAL_ID,
            !!col_name
         ) %>%
         mutate(
            !!col_name := str_squish(toupper(!!col_name)),
            !!col_name := case_when(
               !!col_name == "XXX" ~ NA_character_,
               !!col_name == "N/A" ~ NA_character_,
               !!col_name == "NA" ~ NA_character_,
               !!col_name == "N.A" ~ NA_character_,
               !!col_name == "NULL" ~ NA_character_,
               !!col_name == "NONE" ~ NA_character_,
               !!col_name == "" ~ NA_character_,
               !!col_name == "." ~ NA_character_,
               !!col_name == "-" ~ NA_character_,
               # nchar(!!col_name) == 1 ~ NA_character_,
               TRUE ~ !!col_name
            )
         ) %>%
         filter(!is.na(!!col_name))

      # deduplicate based on central id
      refs$vars[[col]] %<>%
         distinct(ITIS_CENTRAL_ID, .keep_all = TRUE) %>%
         # rename columns for reshaping
         rename(
            DATA = 2
         ) %>%
         mutate(
            VAR = col
         ) %>%
         mutate_all(~as.character(.))
   }

   # append list of latest variables and reshape to create final dataset/linelist
   log_info("Consolidating variables.")
   refs$linelist <- bind_rows(refs$vars) %>%
      pivot_wider(
         id_cols     = ITIS_CENTRAL_ID,
         names_from  = VAR,
         values_from = DATA
      )

   # standardize for deduplication
   log_info("Dataset cleaning and preparation.")
   refs$standard <- refs$linelist %>%
      itis_dedup_prep(
         name_f     = FIRST_NAME,
         name_m     = MIDDLE_NAME,
         name_l     = LAST_NAME,
         name_s     = NAME_EXTENSION,
         BIRTH_DATE = BIRTH_DATE,
         phic       = PHILHEALTH_NUMBER,
         code_px    = PATIENT_CODE
      ) %>%
      mutate(row_id = row_number())

   refs$num_linked <- refs$registry %>%
      group_by(ITIS_CENTRAL_ID) %>%
      summarise(
         NUM_LINKED = n()
      ) %>%
      ungroup()

   return(refs)
}

itis_dupes <- function(dedup, ...) {
   # tag duplicates based on grouping
   dupes <- dedup$standard %>%
      select(row_id, ...) %>%
      filter(if_all(c(...), ~!is.na(.))) %>%
      group_by(...) %>%
      mutate(
         # generate a group id to identify groups of duplicates
         group_id   = cur_group_id(),
         dupe_count = n(),
      ) %>%
      ungroup() %>%
      filter(dupe_count > 1) %>%
      inner_join(dedup$standard %>% select(-c(...)), by = join_by(row_id)) %>%
      left_join(dedup$num_linked, join_by(ITIS_CENTRAL_ID)) %>%
      relocate(ITIS_CENTRAL_ID, group_id, row_id, .before = 1)

   dupes_wide  <- data.frame()
   dupes_pairs <- list()
   if (nrow(dupes) > 0) {
      dupes_wide <- dupes %>%
         arrange(group_id, ITIS_CENTRAL_ID) %>%
         mutate(
            FINAL_CID = first(ITIS_CENTRAL_ID),
            CID_NUM   = row_number(),
         ) %>%
         ungroup() %>%
         pivot_wider(
            id_cols     = FINAL_CID,
            names_from  = CID_NUM,
            names_glue  = "CID_{CID_NUM}",
            values_from = ITIS_CENTRAL_ID
         ) %>%
         select(-CID_1)

      cid_names   <- names(dupes_wide)[grepl("^CID_", names(dupes_wide))]
      dupes_pairs <- cid_names %>%
         lapply(function(name) {
            final_pair <- dupes_wide %>%
               select(
                  FINAL_CID,
                  LINK_CID = !!as.symbol(name)
               ) %>%
               filter(!is.na(LINK_CID))

            return(final_pair)
         }) %>%
         bind_rows()
   }

   num_pairs <- n_groups(dupes %>% group_by(group_id))
   cat(
      crayon::blue("Remaining Unmatched:"),
      dedup$standard %>%
         select(ITIS_CENTRAL_ID) %>%
         anti_join(dedup$registry) %>%
         crayon::magenta() %>%
         crayon::underline() %>%
         nrow(),
      "rows \n"
   )
   cat(
      crayon::blue("Matched Clients:"),
      crayon::underline(crayon::magenta(nrow(dupes_pairs))),
      "rows |",
      crayon::underline(crayon::magenta(num_pairs)),
      "pairs\n"
   )

   data_list <- list(
      "normal"       = dupes %>% arrange(group_id),
      "normal_pivot" = dupes_wide,
      "normal_up"    = dupes_pairs
   )
}

itis_update_registry <- function(new_dupes, registry) {
   # new data
   bind_pid <- new_dupes %>%
      select(
         ITIS_CENTRAL_ID = 1,
         ITIS_PATIENT_ID = 2,
      )

   bind_cid <- new_dupes %>%
      select(
         ITIS_CENTRAL_ID = 1,
      ) %>%
      distinct_all() %>%
      mutate(
         ITIS_PATIENT_ID = ITIS_CENTRAL_ID
      )

   # updated old cids
   new_reg <- registry %>%
      left_join(
         y  = new_dupes %>%
            select(NEW_CID = 1, ITIS_CENTRAL_ID = 2),
         by = join_by(ITIS_CENTRAL_ID)
      ) %>%
      mutate(
         ITIS_CENTRAL_ID = coalesce(NEW_CID, ITIS_CENTRAL_ID)
      ) %>%
      select(-NEW_CID)

   # final new data
   new_reg <- bind_pid %>%
      bind_rows(bind_cid) %>%
      bind_rows(new_reg) %>%
      distinct(ITIS_PATIENT_ID, .keep_all = TRUE)

   return(new_reg)
}

itis_reclink <- function(old, new) {
   data     <- list()
   data$new <- new %>%
      mutate_at(
         .vars = vars(BIRTH_YR, BIRTH_MO, BIRTH_DY),
         ~as.numeric(.)
      )
   data$old <- old %>%
      mutate_at(
         .vars = vars(BIRTH_YR, BIRTH_MO, BIRTH_DY),
         ~as.numeric(.)
      )

   # list of for review
   dedup_old <- list()

   # record linkage
   reclink_df <- fastLink(
      dfA              = data$new,
      dfB              = data$old,
      varnames         = c(
         "FIRST_NAME",
         "MIDDLE_NAME",
         "LAST_NAME",
         "BIRTH_YR",
         "BIRTH_MO",
         "BIRTH_DY"
      ),
      stringdist.match = c(
         "FIRST_NAME",
         "MIDDLE_NAME",
         "LAST_NAME"
      ),
      partial.match    = c(
         "FIRST_NAME",
         "MIDDLE_NAME",
         "LAST_NAME"
      ),
      numeric.match    = c(
         "BIRTH_YR",
         "BIRTH_MO",
         "BIRTH_DY"
      ),
      threshold.match  = 0.95,
      cut.a            = 0.90,
      cut.p            = 0.85,
      dedupe.matches   = FALSE,
      n.cores          = 4
   )

   # check if matches are founid
   if (length(reclink_df$matches$inds.a) > 0) {
      reclink_matched <- getMatches(
         dfA         = data$new,
         dfB         = data$old,
         fl.out      = reclink_df,
         combine.dfs = FALSE
      )

      reclink_review <- reclink_matched$dfA.match %>%
         mutate(
            MATCH_ID = row_number()
         ) %>%
         select(
            MATCH_ID,
            MASTER_CID       = ITIS_CENTRAL_ID,
            MASTER_FIRST     = FIRST_NAME,
            MASTER_MIDDLE    = MIDDLE_NAME,
            MASTER_LAST      = LAST_NAME,
            MASTER_SUFFIX    = NAME_EXTENSION,
            MASTER_BIRTHDATE = BIRTH_DATE,
            MASTER_PHIC      = PHILHEALTH_NUMBER,
            posterior
         ) %>%
         left_join(
            y  = reclink_matched$dfB.match %>%
               mutate(
                  MATCH_ID = row_number()
               ) %>%
               select(
                  MATCH_ID,
                  USING_CID       = ITIS_CENTRAL_ID,
                  USING_FIRST     = FIRST_NAME,
                  USING_MIDDLE    = MIDDLE_NAME,
                  USING_LAST      = LAST_NAME,
                  USING_SUFFIX    = NAME_EXTENSION,
                  USING_BIRTHDATE = BIRTH_DATE,
                  USING_PHIC      = PHILHEALTH_NUMBER,
               ),
            by = "MATCH_ID"
         ) %>%
         unite(
            col   = "MASTER_FMS",
            sep   = " ",
            MASTER_FIRST,
            MASTER_MIDDLE,
            MASTER_SUFFIX,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "MASTER_NAME",
            sep   = ", ",
            MASTER_LAST,
            MASTER_FMS,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "USING_FMS",
            sep   = " ",
            USING_FIRST,
            USING_MIDDLE,
            USING_SUFFIX,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "USING_NAME",
            sep   = ", ",
            USING_LAST,
            USING_FMS,
            na.rm = TRUE
         ) %>%
         arrange(desc(posterior)) %>%
         relocate(posterior, .before = MATCH_ID) %>%
         # Additional sift through of matches
         mutate(
            # levenshtein
            LV       = stringsim(MASTER_NAME, USING_NAME, method = 'lv'),
            # jaro-winkler
            JW       = stringsim(MASTER_NAME, USING_NAME, method = 'jw'),
            # qgram
            QGRAM    = stringsim(MASTER_NAME, USING_NAME, method = 'qgram', q = 3),
            AVG_DIST = (LV + QGRAM + JW) / 3,
         ) %>%
         # choose 60% and above match
         filter(AVG_DIST >= 0.60, !is.na(posterior))

      # assign to global env
      dedup_old$reclink <- reclink_review %>%
         mutate(
            Bene  = NA_character_,
            Gab   = NA_character_,
            Lala  = NA_character_,
            Fayye = NA_character_,
            Cate  = NA_character_,
            Joji  = NA_character_,
            Mavz  = NA_character_,
         )
   }

   data      <- bind_rows(old, new)
   group_pii <- list(
      "PxCode.Base"             = "PATIENT_CODE",
      "PxCode.Fixed"            = "PXCODE_SIEVE",
      "PxCode.Fixed"            = "PXCODE_SIEVE",
      "PxBD.Base"               = c("PATIENT_CODE", "BIRTH_DATE"),
      "PxBD.Fixed"              = c("PXCODE_SIEVE", "BIRTH_DATE"),
      "Name.Base"               = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_DATE"),
      "Name.Fixed.Perm"         = c("FIRST_NY", "LAST_NY", "BIRTH_DATE", "PERM_MUNC"),
      "Name.Partial.Perm"       = c("FIRST_A", "LAST_A", "BIRTH_DATE", "PERM_MUNC"),
      "YM.BD-Name.Base.Perm"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_MO", "PERM_MUNC"),
      "YD.BD-Name.Base.Perm"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_DY", "PERM_MUNC"),
      "MD.BD-Name.Base.Perm"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_MO", "BIRTH_DY", "PERM_MUNC"),
      "YM.BD-Name.Fixed.Perm"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_MO", "PERM_MUNC"),
      "YD.BD-Name.Fixed.Perm"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_DY", "PERM_MUNC"),
      "MD.BD-Name.Fixed.Perm"   = c("FIRST_NY", "LAST_NY", "BIRTH_MO", "BIRTH_DY", "PERM_MUNC"),
      "YM.BD-Name.Partial.Perm" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_MO", "PERM_MUNC"),
      "YD.BD-Name.Partial.Perm" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_DY", "PERM_MUNC"),
      "MD.BD-Name.Partial.Perm" = c("FIRST_A", "LAST_A", "BIRTH_MO", "BIRTH_DY", "PERM_MUNC"),
      "Name.Fixed.Curr"         = c("FIRST_NY", "LAST_NY", "BIRTH_DATE", "CURR_MUNC"),
      "Name.Partial.Curr"       = c("FIRST_A", "LAST_A", "BIRTH_DATE", "CURR_MUNC"),
      "YM.BD-Name.Base.Curr"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_MO", "CURR_MUNC"),
      "YD.BD-Name.Base.Curr"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_DY", "CURR_MUNC"),
      "MD.BD-Name.Base.Curr"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_MO", "BIRTH_DY", "CURR_MUNC"),
      "YM.BD-Name.Fixed.Curr"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_MO", "CURR_MUNC"),
      "YD.BD-Name.Fixed.Curr"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_DY", "CURR_MUNC"),
      "MD.BD-Name.Fixed.Curr"   = c("FIRST_NY", "LAST_NY", "BIRTH_MO", "BIRTH_DY", "CURR_MUNC"),
      "YM.BD-Name.Partial.Curr" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_MO", "CURR_MUNC"),
      "YD.BD-Name.Partial.Curr" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_DY", "CURR_MUNC"),
      "MD.BD-Name.Partial.Curr" = c("FIRST_A", "LAST_A", "BIRTH_MO", "BIRTH_DY", "CURR_MUNC")
   )
   for (i in seq_len(length(group_pii))) {
      dedup_name <- names(group_pii)[[i]]
      dedup_id   <- group_pii[[i]]

      # tag duplicates based on grouping
      df <- data %>%
         filter(if_all(any_of(dedup_id), ~!is.na(.))) %>%
         get_dupes(all_of(dedup_id)) %>%
         filter(dupe_count > 0)

      # if any found, include in list for review
      dedup_old[[dedup_name]] <- df
   }

   return(dedup_old)
}

itis_harp <- function(itis, harp) {
   data     <- list()
   data$new <- itis %>%
      mutate_at(
         .vars = vars(BIRTH_YR, BIRTH_MO, BIRTH_DY),
         ~as.numeric(.)
      )
   data$old <- harp %>%
      mutate_at(
         .vars = vars(BIRTH_YR, BIRTH_MO, BIRTH_DY),
         ~as.numeric(.)
      )

   # list of for review
   dedup_old <- list()

   # record linkage
   reclink_df <- fastLink(
      dfA              = data$new,
      dfB              = data$old,
      varnames         = c(
         "FIRST_NAME",
         "MIDDLE_NAME",
         "LAST_NAME",
         "BIRTH_YR",
         "BIRTH_MO",
         "BIRTH_DY"
      ),
      stringdist.match = c(
         "FIRST_NAME",
         "MIDDLE_NAME",
         "LAST_NAME"
      ),
      partial.match    = c(
         "FIRST_NAME",
         "MIDDLE_NAME",
         "LAST_NAME"
      ),
      numeric.match    = c(
         "BIRTH_YR",
         "BIRTH_MO",
         "BIRTH_DY"
      ),
      threshold.match  = 0.95,
      cut.a            = 0.90,
      cut.p            = 0.85,
      dedupe.matches   = FALSE,
      n.cores          = 4
   )

   # check if matches are founid
   if (length(reclink_df$matches$inds.a) > 0) {
      reclink_matched <- getMatches(
         dfA         = data$new,
         dfB         = data$old,
         fl.out      = reclink_df,
         combine.dfs = FALSE
      )

      reclink_review <- reclink_matched$dfA.match %>%
         mutate(
            MATCH_ID = row_number()
         ) %>%
         select(
            MATCH_ID,
            MASTER_CID       = ITIS_CENTRAL_ID,
            MASTER_FIRST     = FIRST_NAME,
            MASTER_MIDDLE    = MIDDLE_NAME,
            MASTER_LAST      = LAST_NAME,
            MASTER_SUFFIX    = NAME_EXTENSION,
            MASTER_BIRTHDATE = BIRTH_DATE,
            MASTER_PHIC      = PHILHEALTH_NUMBER,
            posterior
         ) %>%
         left_join(
            y  = reclink_matched$dfB.match %>%
               mutate(
                  MATCH_ID = row_number()
               ) %>%
               select(
                  MATCH_ID,
                  USING_CID       = HARP_CENTRAL_ID,
                  USING_FIRST     = FIRST_NAME,
                  USING_MIDDLE    = MIDDLE_NAME,
                  USING_LAST      = LAST_NAME,
                  USING_SUFFIX    = NAME_EXTENSION,
                  USING_BIRTHDATE = BIRTH_DATE,
                  USING_PHIC      = PHILHEALTH_NUMBER,
               ),
            by = "MATCH_ID"
         ) %>%
         unite(
            col   = "MASTER_FMS",
            sep   = " ",
            MASTER_FIRST,
            MASTER_MIDDLE,
            MASTER_SUFFIX,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "MASTER_NAME",
            sep   = ", ",
            MASTER_LAST,
            MASTER_FMS,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "USING_FMS",
            sep   = " ",
            USING_FIRST,
            USING_MIDDLE,
            USING_SUFFIX,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "USING_NAME",
            sep   = ", ",
            USING_LAST,
            USING_FMS,
            na.rm = TRUE
         ) %>%
         arrange(desc(posterior)) %>%
         relocate(posterior, .before = MATCH_ID) %>%
         # Additional sift through of matches
         mutate(
            # levenshtein
            LV       = stringsim(MASTER_NAME, USING_NAME, method = 'lv'),
            # jaro-winkler
            JW       = stringsim(MASTER_NAME, USING_NAME, method = 'jw'),
            # qgram
            QGRAM    = stringsim(MASTER_NAME, USING_NAME, method = 'qgram', q = 3),
            AVG_DIST = (LV + QGRAM + JW) / 3,
         ) %>%
         # choose 60% and above match
         filter(AVG_DIST >= 0.60, !is.na(posterior))

      # assign to global env
      dedup_old$reclink <- reclink_review %>%
         mutate(
            Bene  = NA_character_,
            Gab   = NA_character_,
            Lala  = NA_character_,
            Fayye = NA_character_,
            Cate  = NA_character_,
            Joji  = NA_character_,
            Mavz  = NA_character_,
         )
   }

   return(dedup_old)
}

##  Surveillance Data ----------------------------------------------------------

itis$tpt$data  <- load_itis_output(itis$tpt)
itis$drtb$data <- load_itis_output(itis$drtb)

##  Patient Master List --------------------------------------------------------

itis$refs$patient <- itis %>%
   lapply(function(surveillance) {
      data <- data.frame()
      if ("xl" %in% names(surveillance))
         data <- surveillance$data %>%
            select(
               ITIS_PATIENT_ID,
               LAST_NAME,
               FIRST_NAME,
               MIDDLE_NAME,
               NAME_EXTENSION,
               BIRTH_DATE,
               SEX,
               CONTACT_NUMBER,
               RELIGION,
               NATIONALITY,
               PHILHEALTH_NUMBER,
               PATIENT_CODE = REGISTRATION_NO,
               PERM_ADDR    = PERMANENT_ADDRESS,
               PERM_REG     = PERMANENT_ADDRESS___REGION,
               PERM_PROV    = PERMANENT_ADDRESS___PROVINCE,
               PERM_MUNC    = PERMANENT_ADDRESS___CITY_MUNICIPALITY,
               PERM_BRGY    = PERMANENT_ADDRESS___BRGY,
               CURR_REG     = CURRENT_ADDRESS___REGION,
               CURR_PROV    = CURRENT_ADDRESS___PROVINCE,
               CURR_MUNC    = CURRENT_ADDRESS___CITY_MUNICIPALITY,
               CURR_BRGY    = CURRENT_ADDRESS___BRGY,
               SCREEN_REG   = SCREENING_FACILITY___REGION,
               SCREEN_PROV  = SCREENING_FACILITY___PROVINCE_HUC,
               SCREEN_MUNC  = SCREENING_FACILITY___CITY_MUNICIPALITY,
               SCREEN_FACI  = SCREENING_FACILITY,
               TX_REG       = TREATMENT_FACILITY___REGION,
               TX_PROV      = TREATMENT_FACILITY___PROVINCE_HUC,
               TX_MUNC      = TREATMENT_FACILITY___CITY_MUNICIPALITY,
               TX_FACI      = TREATMENT_FACILITY,
               LAST_UPDATED
            ) %>%
            distinct_all() %>%
            mutate_at(
               .vars = vars(PERM_ADDR),
               ~toupper(.) %>%
                  str_squish() %>%
                  str_replace_all("\\.", " ") %>%
                  str_replace_all(",", " ") %>%
                  str_replace_all("\\bBLOCK\\b", "B") %>%
                  str_replace_all("\\bLOT\\b", "") %>%
                  str_replace_all("\\bNO\\b", "#") %>%
                  str_replace_all("\\bSTREET\\b", "ST") %>%
                  str_squish()
            )

      return(data)
   }) %>%
   bind_rows()

##  Unique linelist deduplication by id groups ---------------------------------

itis$refs$registry <- tibble(ITIS_CENTRAL_ID = NA_character_, ITIS_PATIENT_ID = NA_character_)
itis$refs          <- itis_unique(itis$refs)

# 0th pass
dupes              <- itis_dupes(itis$refs, FIRST_NAME, MIDDLE_NAME, LAST_NAME, SEX, BIRTH_DATE, PERM_BRGY)
itis$refs$registry <- itis_update_registry(dupes$normal_up, itis$refs$registry)
itis$refs          <- itis_unique(itis$refs)

# 1st pass
dupes              <- itis_dupes(itis$refs, FIRST_SIEVE, LAST_SIEVE, BIRTH_DATE, PERM_MUNC)
itis$refs$registry <- itis_update_registry(dupes$normal_up, itis$refs$registry)
itis$refs          <- itis_unique(itis$refs)

# 2nd pass
dupes              <- itis_dupes(itis$refs, FIRST_SIEVE, LAST_NY, BIRTH_DATE, PERM_MUNC)
itis$refs$registry <- itis_update_registry(dupes$normal_up, itis$refs$registry)
itis$refs          <- itis_unique(itis$refs)

# 3rd pass
dupes              <- itis_dupes(itis$refs, FIRST_SIEVE, LAST_NY, PATIENT_CODE)
itis$refs$registry <- itis_update_registry(dupes$normal_up, itis$refs$registry)
itis$refs          <- itis_unique(itis$refs)

##  Unique linelist deduplication by probabilistic record linkage --------------

itis$dedup$review <- itis_reclink(
   itis$refs$standard %>%
      inner_join(
         y  = itis$refs$registry %>% distinct(ITIS_CENTRAL_ID),
         by = join_by(ITIS_CENTRAL_ID)
      ),
   itis$refs$standard %>%
      anti_join(
         y  = itis$refs$registry,
         by = join_by(ITIS_CENTRAL_ID == ITIS_PATIENT_ID)
      )
)
invisible({
   lapply(
      names(itis$dedup$review),
      function(sheet, src) {
         local_gs4_quiet()
         if (nrow(src[[sheet]]) > 0) {
            log_info("Uploading = {green(sheet)}.")
            ss <- as_id("188U7H8EnKHrrOZBwBQORjEUAP4b5F9CnmlFuWyZPRrE")
            write_sheet(src[[sheet]], ss, sheet)
            range_autofit(ss, sheet)
         }
      },
      src = itis$dedup$review
   )
   # remove empty rows
   itis$dedup$review <- itis$dedup$review[sapply(itis$dedup$review, function(x) dim(x)[1]) > 0]

   ss <- as_id("188U7H8EnKHrrOZBwBQORjEUAP4b5F9CnmlFuWyZPRrE")
   lapply(setdiff(sheet_names(ss), names(itis$dedup$review)), function(sheet) sheet_delete(ss, sheet))
})

itis$refs$registry <- itis_update_registry(read_sheet(ss, "upload"), itis$refs$registry)
itis$refs          <- itis_unique(itis$refs)
itis$refs$standard %>%
   anti_join(
      y  = itis$refs$registry,
      by = join_by(ITIS_CENTRAL_ID == ITIS_PATIENT_ID)
   ) %>%
   slice(1:500) %>%
   # slice(1:1000) %>%
   arrange(LAST_NAME, FIRST_NAME) %>%
   select(ITIS_CENTRAL_ID) %>%
   write_clip()

dupes <- itis_dupes(itis$refs, FIRST_NY, PERM_ADDR, PATIENT_CODE)
dupes <- itis_dupes(itis$refs, FIRST_NY, TX_FACI, PATIENT_CODE)
dupes$normal

itis$tpt$data %>% filter(REGISTRATION_NO == "PHNT-1307-780-I23-0002")
itis$tpt$data %>% filter(ITIS_PATIENT_ID %in% c('230503111953942556000000001951', '230116210110RQO79L000000001951'))

##  Match w/ HIV Datasets ------------------------------------------------------

itis$refs$patient %>%
   group_by(ITIS_CENTRAL_ID) %>%
   mutate(
      dupe_count = n()
   ) %>%
   ungroup() %>%
   filter(dupe_count > 2)

check <- itis$refs$patient %>%
   filter(ITIS_PATIENT_ID %in% c(
      '230413135310944757000000040611', '230413092838944757000000040612', '230411134938943956000000001111', '230411134136943956000000001111',
      '230608132626947845000000033461', '230512133003947845000000033461', '230428084634268000000016791', '230411100545268000000016791', '230204060810942776000000007591', '230125182315942776000000007591', '230518091939943832000000007061', '230517130722943832000000007061', '230612215434946625000000022141', '230313160341946625000000022141', '230613130039946960000000029201', '230220141213946960000000029201', '230423113616944279000000011461', '230416214237944279000000011461', '230219225224T19ENC000000035001', '230119110922QHP1CJ000000035001', '230403204601950400000000035521', '230403201746950400000000035521', '230203161959999706000000008161', '230117113231999706000000008161', '230316103125950401000000001201', '230309125550950401000000001201', '230509141006713000000000291', '230306120844713000000000291', '230422031026943402000000000591', '230315152018948719000000000591', '230503111953942556000000001951', '230116210110RQO79L000000001951', '230512095403TOFQJR000000040641', '230512093742HI8WXM000000040641', '230525163207994801000000057791', '230525163805994801000000057791', '230525164136994801000000057791', '230525164538994801000000057791',
      '230508135958GUWCPI000000106651', '2305100937245ZNIJO000000106651', '230616131636WSVJ31000000106651',
      '230511145924948280000000049101', '230511150729948280000000049101', '230526155747948280000000049101',
      '230329171507964620000000101681', '230329171832964620000000101681'
   )) %>%
   arrange(PATIENT_CODE, LAST_NAME, FIRST_NAME, BIRTH_DATE) %>%
   select(ITIS_PATIENT_ID) %>%
   left_join(
      y  = itis$tpt$data,
      by = join_by(ITIS_PATIENT_ID)
   ) %>%
   mutate(GROUP_CHECK = NA_integer_, .before = 1) %>%
   relocate(
      ITIS_PATIENT_ID,
      LAST_NAME,
      FIRST_NAME,
      MIDDLE_NAME,
      NAME_EXTENSION,
      BIRTH_DATE,
      SEX,
      CONTACT_NUMBER,
      RELIGION,
      NATIONALITY,
      PHILHEALTH_NUMBER,
      ENCODED_BY,
      USERLEVEL_OF_ENCODER,
      LAST_UPDATED,
      .after = GROUP_CHECK
   ) %>%
   filter(!is.na(LAST_NAME))

odd  <- 1
even <- 2
for (i in 1:22) {
   check[odd:even, "GROUP_CHECK"] <- i

   i    <- i + 1
   odd  <- odd + 1
   even <- even + 2
}

check %>%
   write_xlsx("H:/20230702_itis-tpt_reference_dupes.xlsx")


##  match with tx data
tx      <- read_dta(hs_data("harp_tx", "reg", 2023, 6))
check   <- itis_harp(
   itis$refs$linelist %>%
      anti_join(
         y = id_itis %>% select(ITIS_CENTRAL_ID = ITIS_PATIENT_ID)
      ) %>%
      mutate(
         BIRTH_YR = year(BIRTH_DATE),
         BIRTH_MO = month(BIRTH_DATE),
         BIRTH_DY = day(BIRTH_DATE),
      ),
   tx %>%
      rename(
         FIRST_NAME  = first,
         MIDDLE_NAME = middle,
         LAST_NAME   = last
      ) %>%
      mutate(
         BIRTH_YR = year(birthdate),
         BIRTH_MO = month(birthdate),
         BIRTH_DY = day(birthdate),
      )
)
check   <- quick_reclink(
   itis$refs$linelist %>%
      anti_join(
         y = id_itis %>% select(ITIS_CENTRAL_ID = ITIS_PATIENT_ID)
      ) %>%
      mutate_at(
         .vars = vars(FIRST_NAME, MIDDLE_NAME, LAST_NAME, NAME_EXTENSION),
         ~coalesce(clean_pii(.), "")
      ) %>%
      mutate(
         # name
         name = str_squish(stri_c(LAST_NAME, ", ", FIRST_NAME, " ", MIDDLE_NAME, " ", NAME_EXTENSION)),
      ),
   tx %>%
      rename(
         FIRST_NAME     = first,
         MIDDLE_NAME    = middle,
         LAST_NAME      = last,
         NAME_EXTENSION = suffix,
         BIRTH_DATE     = birthdate
      ) %>%
      mutate(
         name = str_squish(stri_c(LAST_NAME, ", ", FIRST_NAME, " ", MIDDLE_NAME, " ", NAME_EXTENSION)),
      ),
   "ITIS_CENTRAL_ID",
   "CENTRAL_ID",
   # c("name", "BIRTH_DATE"),
   "name",
   "name"
)
lw_conn <- ohasis$conn("lw")
id_itis <- dbxSelect(lw_conn, "SELECT PATIENT_ID, ITIS_PATIENT_ID FROM ohasis_warehouse.id_itis")
dbDisconnect(lw_conn)
for_match <- itis$refs$linelist %>%
   mutate(
      CENTRAL_ID        = ITIS_CENTRAL_ID,
      BIRTH_DATE        = as.Date(BIRTH_DATE),
      philsys_id        = NA_character_,
      uic               = str_c(
         "XXXX99",
         stri_pad_left(month(BIRTH_DATE), 2, "0"),
         stri_pad_left(day(BIRTH_DATE), 2, "0"),
         stri_pad_left(year(BIRTH_DATE), 4, "0")
      ),
      confirmatory_code = NA_character_,
   ) %>%
   anti_join(
      y = id_itis %>% select(ITIS_CENTRAL_ID = ITIS_PATIENT_ID)
   ) %>%
   dedup_prep(
      FIRST_NAME,
      MIDDLE_NAME,
      LAST_NAME,
      NAME_EXTENSION,
      uic,
      BIRTH_DATE,
      confirmatory_code,
      PATIENT_CODE,
      PHILHEALTH_NUMBER,
      philsys_id
   ) %>%
   select(-CENTRAL_ID) %>%
   mutate_at(
      .vars = vars(FIRST_NAME, MIDDLE_NAME, LAST_NAME, NAME_EXTENSION),
      ~coalesce(clean_pii(.), "")
   ) %>%
   mutate(
      MASTER_NAME = str_squish(stri_c(LAST_NAME, ", ", FIRST_NAME, " ", MIDDLE_NAME, " ", NAME_EXTENSION)),
   )
ref_match <- tx %>%
   dedup_prep(
      first,
      middle,
      last,
      suffix,
      uic,
      birthdate,
      confirmatory_code,
      px_code,
      philhealth_no,
      philsys_id
   ) %>%
   rename(
      FIRST_NAME     = first,
      MIDDLE_NAME    = middle,
      LAST_NAME      = last,
      NAME_EXTENSION = suffix,
      BIRTH_DATE     = birthdate
   ) %>%
   mutate(
      USING_NAME = str_squish(stri_c(LAST_NAME, ", ", FIRST_NAME, " ", MIDDLE_NAME, " ", NAME_EXTENSION)),
   )

ref_match %>%
   inner_join(
      y = for_match,
      join_by(FIRST_NY, LAST_NY, BIRTH_MO, BIRTH_DY)
   ) %>%
   select(
      CENTRAL_ID,
      MASTER_NAME,
      BIRTH_DATE.x,
      ITIS_CENTRAL_ID,
      USING_NAME,
      BIRTH_DATE.y
   ) %>%
   # Additional sift through of matches
   mutate(
      # levenshtein
      LV       = stringsim(MASTER_NAME, USING_NAME, method = 'lv'),
      # jaro-winkler
      JW       = stringsim(MASTER_NAME, USING_NAME, method = 'jw'),
      # qgram
      QGRAM    = stringsim(MASTER_NAME, USING_NAME, method = 'qgram', q = 3),
      AVG_DIST = (LV + QGRAM + JW) / 3,
   ) %>%
   # choose 60% and above match
   filter(AVG_DIST >= 0.85) %>%
   select(
      CENTRAL_ID,
      ITIS_CENTRAL_ID,
   ) %>%
   write_clip()

itis$tpt$data %>%
   rename_all(
      ~str_replace_all(., "__", "_") %>%
         str_replace_all("LABORATORY", "LAB") %>%
         str_replace_all("FACILITY", "FACI") %>%
         str_replace_all("CITY_MUNICIPALITY", "MUNCITY")
   ) %>%
   format_stata() %>%
   write_dta("H:/_R/library/itis_tpt/20230701_itis-tpt_2023-06.dta")

itis$drtb$data %>%
   rename_all(
      ~str_replace_all(., "__", "_") %>%
         str_replace_all("LABORATORY", "LAB") %>%
         str_replace_all("FACILITY", "FACI") %>%
         str_replace_all("CITY_MUNICIPALITY", "MUNCITY")
   ) %>%
   format_stata() %>%
   write_dta("H:/_R/library/itis_tbtx/20230701_itis-drtb_2023-06.dta")

drtb_sql <- r"(
SELECT caseID, scrngLocation, gender, age, classification, screening_date, registrationDate, registrationCode, starttxDate, screeningUnit, treatmentUnit, sourceofReferral, tbContact, transin, category, tb_kind, patientType, bact_status, outcome, outcome_reason, outcome_comment, outcomeDate, pictDate, no_hiv_reason, no_art_reason, validated, txPartner, socialClass, birthDate, nationality, drType, regimenCode, screening_date, study_participation, AES_DECRYPT(last_name,'grtjw48957hfsd63fj3je0rwpdkqdjdw23948fsqfu') AS dec_lastname, AES_DECRYPT(first_name,'grtjw48957hfsd63fj3je0rwpdkqdjdw23948fsqfu') AS dec_firstname, AES_DECRYPT(middle_name,'grtjw48957hfsd63fj3je0rwpdkqdjdw23948fsqfu') AS dec_middlename, birthPlace, religion, occupation, philHealth, AES_DECRYPT(street_address,'grtjw48957hfsd63fj3je0rwpdkqdjdw23948fsqfu') AS dec_address, screeningCode, specific_source, presumptive_drtb, endtxDate, continuationTx, ipt_type, gLocation, civilStatus, name_suffix, datetime_created, diagnosis_date, mode_of_screening, txPartner, tx_supporter_location, dat, tb_case.userID, tb_case.dateEncoded FROM `tb_case` JOIN `patient` ON tb_case.patientID = patient.patientID WHERE (registrationDate BETWEEN '2023-06-01' AND '2023-06-30') AND registrationCode != '' AND outcome != 'NULL' AND outcome != '0' AND tb_kind = 1 AND validated = '1'
)"