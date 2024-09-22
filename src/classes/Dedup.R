Dedup <- R6Class(
   "Dedup",
   public  = list(
      master     = list(
         data = tibble(),
         id   = character()
      ),
      using      = list(
         data = tibble(),
         id   = character()
      ),
      match      = list(),
      review     = list(),

      setMaster  = function(data, id) {
         self$master$data <- data
         self$master$id   <- id
      },

      setUsing   = function(data, id) {
         self$using$data <- data
         self$using$id   <- id
      },

      preparePii = function() {
         self$match$master <- self$master$data %>%
            rename_all(private$renameColumns) %>%
            select(
               CENTRAL_ID,
               all_of(self$master$id),
               all_of(private$requiredColumns)
            ) %>%
            private$auxColumns(self$master$id)
         self$match$using  <- self$using$data %>%
            rename_all(private$renameColumns) %>%
            select(
               CENTRAL_ID,
               all_of(self$using$id),
               all_of(private$requiredColumns)
            ) %>%
            private$auxColumns(self$using$id)

         invisible(self)
      },

      reclink    = function() {
         reclink_df <- fastLink(
            dfA              = self$match$master,
            dfB              = self$match$using,
            varnames         = c(
               "FIRST",
               "MIDDLE",
               "LAST",
               "SUFFIX",
               "BIRTH_YR",
               "BIRTH_MO",
               "BIRTH_DY"
            ),
            stringdist.match = c(
               "FIRST",
               "MIDDLE",
               "LAST"
            ),
            partial.match    = c(
               "FIRST",
               "LAST"
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
            n.cores          = 6
         )

         if (length(reclink_df$matches$inds.a) > 0) {
            reclink_matched <- getMatches(
               dfA         = self$match$master,
               dfB         = self$match$using,
               fl.out      = reclink_df,
               combine.dfs = FALSE
            )

            reclink_review <- reclink_matched$dfA.match %>%
               mutate(
                  MATCH_ID = row_number()
               ) %>%
               select(
                  posterior,
                  MATCH_ID,
                  MASTER_CID          = CENTRAL_ID,
                  MASTER_FIRST        = FIRST,
                  MASTER_MIDDLE       = MIDDLE,
                  MASTER_LAST         = LAST,
                  MASTER_SUFFIX       = SUFFIX,
                  MASTER_BIRTHDATE    = BIRTHDATE,
                  MASTER_CONFIRMATORY = CONFIRMATORY_CODE,
                  MASTER_UIC          = UIC,
                  MASTER_PXCODE       = PATIENT_CODE
               ) %>%
               left_join(
                  y  = reclink_matched$dfB.match %>%
                     mutate(
                        MATCH_ID = row_number()
                     ) %>%
                     select(
                        MATCH_ID,
                        USING_CID          = CENTRAL_ID,
                        USING_FIRST        = FIRST,
                        USING_MIDDLE       = MIDDLE,
                        USING_LAST         = LAST,
                        USING_SUFFIX       = SUFFIX,
                        USING_BIRTHDATE    = BIRTHDATE,
                        USING_CONFIRMATORY = CONFIRMATORY_CODE,
                        USING_UIC          = UIC,
                        USING_PXCODE       = PATIENT_CODE,
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
            self$review$reclink <- reclink_review %>%
               mutate(
                  Bene  = NA_character_,
                  Gab   = NA_character_,
                  Lala  = NA_character_,
                  Fayye = NA_character_,
                  # ) %>%
                  # anti_join(
                  #    y  = non_dupes %>%
                  #       select(USING_CID = PATIENT_ID, MASTER_CID = NON_PAIR_ID),
                  #    by = join_by(USING_CID, MASTER_CID)
               )
         }

         log_success("Done.")

         invisible(self)
      }
   ),

   private = list(
      requiredColumns = c(
         "FIRST",
         "MIDDLE",
         "LAST",
         "SUFFIX",
         "BIRTHDATE",
         "SEX",
         "UIC",
         "CONFIRMATORY_CODE",
         "PATIENT_CODE",
         "PHILHEALTH_NO",
         "PHILSYS_ID",
         "CLIENT_MOBILE",
         "CLIENT_EMAIL"
      ),

      renameColumns   = function(name) {
         new <- case_when(
            name == "labcode2" ~ "CONFIRMATORY_CODE",
            name == "confirmatory_code" ~ "CONFIRMATORY_CODE",
            name == "uic" ~ "UIC",
            name == "px_code" ~ "PATIENT_CODE",
            name == "patient_code" ~ "PATIENT_CODE",
            name == "CLIENT_CODE" ~ "PATIENT_CODE",
            name == "firstname" ~ "FIRST",
            name == "first" ~ "FIRST",
            name == "fname" ~ "FIRST",
            name == "middle" ~ "MIDDLE",
            name == "mname" ~ "MIDDLE",
            name == "last" ~ "LAST",
            name == "lname" ~ "LAST",
            name == "suffix" ~ "SUFFIX",
            name == "name_suffix" ~ "SUFFIX",
            name == "bdate" ~ "BIRTHDATE",
            name == "birthdate" ~ "BIRTHDATE",
            name == "DATE_OF_BIRTH" ~ "BIRTHDATE",
            name == "philhealth" ~ "PHILHEALTH_NO",
            name == "philhealth_no" ~ "PHILHEALTH_NO",
            name == "philhealth_num" ~ "PHILHEALTH_NO",
            name == "philsys" ~ "PHILSYS_ID",
            name == "philsys_id" ~ "PHILSYS_ID",
            name == "sex" ~ "SEX",
            name == "sex_at_birtH" ~ "SEX",
            name == "client_mobile" ~ "CLIENT_MOBILE",
            name == "mobile" ~ "CLIENT_MOBILE",
            name == "mobile_no" ~ "CLIENT_MOBILE",
            name == "email" ~ "CLIENT_EMAIL",
            name == "email_address" ~ "CLIENT_EMAIL",
            TRUE ~ name
         )

         return(new)
      },

      ensureColumns   = function(data) {
         missing_cols <- setdiff(private$requireColumns, names(data))
         if (length(missing_cols) > 0) {
            for (col in missing_cols) {
               data %<>%
                  mutate(
                     NEW_COL = if (col == "BIRTHDATE") NEW_COL = NA_Date_ else NA_character_,
                  ) %>%
                  rename_at(
                     .vars = vars(NEW_COL),
                     ~col
                  )
            }
         }

         return(data)
      },

      auxColumns      = function(data, id_col) {
         dedup_new <- data %>%
            mutate_if(is.character, toupper) %>%
            mutate_if(is.character, str_squish) %>%
            mutate_if(is.character, ~stri_trans_general(., "latin-ascii")) %>%
            mutate_if(is.character, clean_pii) %>%
            mutate(
               # get components of birthdate
               BIRTH_YR         = as.numeric(year(BIRTHDATE)),
               BIRTH_MO         = as.numeric(month(BIRTHDATE)),
               BIRTH_DY         = as.numeric(day(BIRTHDATE)),

               # extract parent info from uic
               UIC_MOM          = substr(UIC, 1, 2),
               UIC_DAD          = substr(UIC, 3, 4),
               UIC_ORDER        = substr(UIC, 5, 6),

               # variables for first 3 letters of names
               FIRST_A          = substr(FIRST, 1, 3),
               MIDDLE_A         = substr(MIDDLE, 1, 3),
               LAST_A           = substr(LAST, 1, 3),

               LAST             = coalesce(LAST, MIDDLE),
               MIDDLE           = coalesce(MIDDLE, LAST),

               # clean ids
               CONFIRM_SIEVE    = CONFIRMATORY_CODE,
               PXCODE_SIEVE     = PATIENT_CODE,
               FIRST_SIEVE      = FIRST,
               MIDDLE_SIEVE     = MIDDLE,
               LAST_SIEVE       = LAST,
               PHILHEALTH_SIEVE = PHILHEALTH_NO,
               PHILSYS_SIEVE    = PHILSYS_ID,
            ) %>%
            mutate_at(
               .vars = vars(ends_with("_SIEVE", ignore.case = TRUE)),
               ~str_replace_all(., "[^[:alnum:]]", "")
            ) %>%
            mutate_at(
               .vars = vars(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE),
               ~str_replace_all(., "([[:alnum:]])\\1+", "\\1")
            ) %>%
            mutate(
               # code standard names
               FIRST_NY  = suppress_warnings(nysiis(FIRST_SIEVE, stri_length(FIRST_SIEVE)), "unknown characters"),
               MIDDLE_NY = suppress_warnings(nysiis(MIDDLE_SIEVE, stri_length(MIDDLE_SIEVE)), "unknown characters"),
               LAST_NY   = suppress_warnings(nysiis(LAST_SIEVE, stri_length(LAST_SIEVE)), "unknown characters"),
            )

         log_info("Splitting UIC.")
         # genearte UIC w/o 1 parent, 2 combinations
         dedup_new_uic <- dedup_new %>%
            filter(!is.na(UIC)) %>%
            rename(
               ROW_ID = id_col
            ) %>%
            select(
               ROW_ID,
               UIC_MOM,
               UIC_DAD
            ) %>%
            pivot_longer(
               cols      = c(UIC_MOM, UIC_DAD),
               names_to  = 'UIC',
               values_to = 'FIRST_TWO'
            ) %>%
            arrange(ROW_ID, FIRST_TWO) %>%
            group_by(ROW_ID) %>%
            mutate(UIC = row_number()) %>%
            ungroup() %>%
            pivot_wider(
               id_cols      = ROW_ID,
               names_from   = UIC,
               names_prefix = 'UIC_',
               values_from  = FIRST_TWO
            ) %>%
            rename(
               !!id_col := ROW_ID
            )

         log_info("Sorting UIC.")
         dedup_new %<>%
            left_join(
               y  = dedup_new_uic,
               by = id_col
            ) %>%
            mutate(
               UIC_SORT = stri_c(UIC_1, UIC_2, substr(UIC, 5, 14))
            )

         log_info("Sorting Names.")
         dedup_new_names <- dedup_new %>%
            rename(
               ROW_ID = id_col
            ) %>%
            select(
               ROW_ID,
               NAME_1 = FIRST_SIEVE,
               NAME_2 = MIDDLE_SIEVE,
               NAME_3 = LAST_SIEVE
            ) %>%
            filter(if_any(c(NAME_1, NAME_2, NAME_3), ~!is.na(.))) %>%
            pivot_longer(
               cols      = c(NAME_1, NAME_2, NAME_3),
               names_to  = "name",
               values_to = "value",
            ) %>%
            mutate(
               value = if_else(nchar(value) == 1, NA_character_, value, value)
            ) %>%
            filter(!is.na(value)) %>%
            arrange(ROW_ID, value) %>%
            group_by(ROW_ID) %>%
            summarise(
               NAMESORT_FIRST = first(value),
               NAMESORT_LAST  = last(value),
            ) %>%
            ungroup() %>%
            rename(
               !!id_col := ROW_ID
            )

         dedup_new %<>%
            left_join(
               y  = dedup_new_names,
               by = id_col
            )

         return(dedup_new)
      }
   )
)

try <- Dedup$new()
try$setMaster(dx %>% select(-first) %>% filter(year == 2024, month == 6), "idnum")
try$setUsing(dx %>% select(-first) %>% filter(year <= 2023), "idnum")
try$preparePii()
try$reclink()
