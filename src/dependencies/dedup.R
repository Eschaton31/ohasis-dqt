##  Deduplication functions used in processing data ----------------------------

dedup_prep <- function(
   data = NULL,
   name_f = NULL,
   name_m = NULL,
   name_l = NULL,
   name_s = NULL,
   uic = NULL,
   birthdate = NULL,
   code_confirm = NULL,
   code_px = NULL,
   phic = NULL,
   philsys = NULL
) {
   dedup_new <- data %>%
      mutate(
         LAST              = stri_trans_general(stri_trans_toupper({{name_l}}), "latin-ascii"),
         MIDDLE            = stri_trans_general(stri_trans_toupper({{name_m}}), "latin-ascii"),
         FIRST             = stri_trans_general(stri_trans_toupper({{name_f}}), "latin-ascii"),
         SUFFIX            = stri_trans_general(stri_trans_toupper({{name_s}}), "latin-ascii"),
         UIC               = stri_trans_general(stri_trans_toupper({{uic}}), "latin-ascii"),
         CONFIRMATORY_CODE = stri_trans_general(stri_trans_toupper({{code_confirm}}), "latin-ascii"),
         PATIENT_CODE      = stri_trans_general(stri_trans_toupper({{code_px}}), "latin-ascii"),

         # get components of birthdate
         BIRTH_YR          = year({{birthdate}}),
         BIRTH_MO          = month({{birthdate}}),
         BIRTH_DY          = day({{birthdate}}),

         # extract parent info from uic
         UIC_MOM           = if_else(!is.na(UIC), substr(UIC, 1, 2), NA_character_),
         UIC_DAD           = if_else(!is.na(UIC), substr(UIC, 3, 4), NA_character_),

         # variables for first 3 letters of names
         FIRST_A           = if_else(!is.na(FIRST), substr(FIRST, 1, 3), NA_character_),
         MIDDLE_A          = if_else(!is.na(MIDDLE), substr(MIDDLE, 1, 3), NA_character_),
         LAST_A            = if_else(!is.na(LAST), substr(LAST, 1, 3), NA_character_),

         LAST              = if_else(is.na(LAST), MIDDLE, LAST),
         MIDDLE            = if_else(is.na(MIDDLE), LAST, MIDDLE),

         # clean ids
         CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
         PXCODE_SIEVE      = if_else(!is.na(PATIENT_CODE), str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""), NA_character_),
         FIRST_S           = if_else(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_),
         MIDDLE_S          = if_else(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_),
         LAST_S            = if_else(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_),
         PHIC              = if_else(!is.na({{phic}}), str_replace_all({{phic}}, "[^[:alnum:]]", ""), NA_character_),
         PHILSYS           = if_else(!is.na({{philsys}}), str_replace_all({{philsys}}, "[^[:alnum:]]", ""), NA_character_),

         # code standard names
         FIRST_NY          = suppress_warnings(if_else(!is.na(FIRST_S), nysiis(FIRST_S, stri_length(FIRST_S)), NA_character_), "unknown characters"),
         MIDDLE_NY         = suppress_warnings(if_else(!is.na(MIDDLE_S), nysiis(MIDDLE_S, stri_length(MIDDLE_S)), NA_character_), "unknown characters"),
         LAST_NY           = suppress_warnings(if_else(!is.na(LAST_S), nysiis(LAST_S, stri_length(LAST_S)), NA_character_), "unknown characters"),
      )


   # genearte UIC w/o 1 parent, 2 combinations
   dedup_new_uic <- dedup_new %>%
      filter(!is.na(UIC)) %>%
      select(
         CENTRAL_ID,
         UIC_MOM,
         UIC_DAD
      ) %>%
      pivot_longer(
         cols      = starts_with('UIC'),
         names_to  = 'UIC',
         values_to = 'FIRST_TWO'
      ) %>%
      arrange(CENTRAL_ID, FIRST_TWO) %>%
      group_by(CENTRAL_ID) %>%
      mutate(UIC = row_number()) %>%
      ungroup() %>%
      pivot_wider(
         id_cols      = CENTRAL_ID,
         names_from   = UIC,
         names_prefix = 'UIC_',
         values_from  = FIRST_TWO
      )

   dedup_new %<>%
      left_join(
         y  = dedup_new_uic,
         by = 'CENTRAL_ID'
      ) %>%
      mutate(
         UIC_SORT = if_else(
            condition = !is.na(UIC),
            true      = paste0(UIC_1, UIC_2, substr(UIC, 5, 14)),
            false     = NA_character_
         )
      )

   return(dedup_new)
}

upload_dupes <- function(data) {
   db_conn <- dbConnect(
      RMariaDB::MariaDB(),
      user     = 'ohasis',
      password = 't1rh0uGCyN2sz6zk',
      host     = '192.168.193.232',
      port     = '3307',
      timeout  = -1,
      'ohasis_interim'
   )

   pb <- progress_bar$new(format = ":current of :total PIDs | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(data), width = 100, clear = FALSE)
   pb$tick(0)
   for (i in seq_len(nrow(data))) {
      cid <- data[i, 1] %>% as.character()
      pid <- data[i, 2] %>% as.character()
      ts  <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

      num_pid <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM registry WHERE PATIENT_ID = ?",
            params = pid
         )
      )
      num_cid <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM registry WHERE CENTRAL_ID = ?",
            params = pid
         )
      )
      num_pcid <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM registry WHERE PATIENT_ID = ?",
            params = cid
         )
      )

      if (num_pcid == 0) {
         dbExecute(
            db_conn,
            "INSERT IGNORE INTO registry (CENTRAL_ID, PATIENT_ID, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?);",
            params = list(cid, cid, "1300000001", ts)
         )
      }

      if (num_pid == 0) {
         dbExecute(
            db_conn,
            "INSERT IGNORE INTO registry (CENTRAL_ID, PATIENT_ID, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?);",
            params = list(cid, pid, "1300000001", ts)
         )
      } else {
         dbExecute(
            db_conn,
            "UPDATE registry SET CENTRAL_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE PATIENT_ID = ?;",
            params = list(cid, "1300000001", ts, pid)
         )
      }

      if (num_cid > 0) {
         dbExecute(
            db_conn,
            "UPDATE registry SET CENTRAL_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE CENTRAL_ID = ?;",
            params = list(cid, "1300000001", ts, pid)
         )
      }

      pb$tick(1)
   }

   dbDisconnect(db_conn)
}

dedup_by <- function(ss, sheet, col_start, col_end, row_start, row_end) {
   # get sheet properties
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.get",
      params   = list(spreadsheetId = ss)
   )
   val <- googlesheets4::request_make(req)
   res <- httr::content(val)
   for (i in seq_len(length(res$sheets))) {
      sheet_name <- res$sheets[[i]]$properties$title

      if (sheet_name == sheet)
         sheet_id <- res$sheets[[i]]$properties$sheetId
   }

   # write to sheet
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            setDataValidation = list(
               rule  = list(
                  condition    = list(
                     type   = 'ONE_OF_LIST',
                     values = list(
                        list(userEnteredValue = "Y"),
                        list(userEnteredValue = "N")
                     )
                  ),
                  showCustomUi = TRUE
               ),
               range = list(
                  sheetId          = sheet_id,
                  startRowIndex    = row_start,
                  endRowIndex      = row_end,
                  startColumnIndex = col_start,
                  endColumnIndex   = col_end
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)

   # conditional formatting
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            deleteConditionalFormatRule = list(
               index   = 0,
               sheetId = sheet_id
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            deleteConditionalFormatRule = list(
               index   = 0,
               sheetId = sheet_id
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            addConditionalFormatRule = list(
               rule = list(
                  ranges      = list(
                     sheetId          = sheet_id,
                     startRowIndex    = row_start,
                     endRowIndex      = row_end,
                     startColumnIndex = col_start,
                     endColumnIndex   = col_end
                  ),
                  booleanRule = list(
                     condition = list(
                        type   = "CUSTOM_FORMULA",
                        values = list(userEnteredValue = r"(=X2 = "Y")")
                     ),
                     format    = list(
                        backgroundColorStyle = list(
                           rgbColor = list(
                              red   = 97 / 255,
                              green = 226 / 255,
                              blue  = 148 / 255
                           )
                        )
                     )
                  )
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            addConditionalFormatRule = list(
               rule = list(
                  ranges      = list(
                     sheetId          = sheet_id,
                     startRowIndex    = row_start,
                     endRowIndex      = row_end,
                     startColumnIndex = col_start,
                     endColumnIndex   = col_end
                  ),
                  booleanRule = list(
                     condition = list(
                        type   = "CUSTOM_FORMULA",
                        values = list(userEnteredValue = r"(=X2 = "N")")
                     ),
                     format    = list(
                        backgroundColorStyle = list(
                           rgbColor = list(
                              red   = 189 / 255,
                              green = 147 / 255,
                              blue  = 216 / 255
                           )
                        )
                     )
                  )
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)
}