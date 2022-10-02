#* @filter cors
cors <- function(req, res) {
   .log_info("Adding CORS.")
   res$setHeader("Access-Control-Allow-Origin", "*")

   if (req$REQUEST_METHOD == "OPTIONS") {
      res$setHeader("Access-Control-Allow-Methods", "*")
      res$setHeader("Access-Control-Allow-Headers", req$HTTP_ACCESS_CONTROL_REQUEST_HEADERS)
      res$status <- 200
      return(list())
   } else {
      plumber::forward()
   }
}

#* @filter auth
auth <- function(req, res) {
   .log_info("Authenticating token.")
   # decrypt hashed token
   token <- rawToChar(base64enc::base64decode(stri_replace_all_fixed(req$HTTP_AUTHORIZATION, "Basic ", "")))

   # print(token)
   # check auth if in allowed list
   conn <- dbConnect(
      RMariaDB::MariaDB(),
      user     = Sys.getenv("DB_USER"),
      password = Sys.getenv("DB_PASS"),
      host     = '127.0.0.1',
      port     = Sys.getenv("DB_PORT"),
      timeout  = -1,
      'ohasis_api'
   )
   auth <- dbGetQuery(conn, "SELECT * FROM api_tokens WHERE token = ? AND active = 1", list(token))
   dbDisconnect(conn)

   # return error if not
   if (nrow(auth) == 0) {
      res$status <- 422
      return(list(error = "Authentication required!"))
   } else {
      plumber::forward()
   }
}

#* Return a list of clients
#* @post /client
function(req, res) {
   .log_info("Constructing query.")
   # get list of cols with non-empty params
   cols <- list()
   for (col in names(req$body))
      if (length(req$body[[col]]$value) > 0) cols[[col]] <- rawToChar(req$body[[col]]$value)

   # get faci_id to limit if non-central
   faci_id <- cols[['FACI_ID']]
   cols    <- within(cols, rm(FACI_ID))

   # return error if no search params sent
   if (length(cols) == 0) {
      res$status == 422
      return(list(error = "No parameters sent."))
   } else {
      # initiate connection
      conn <- dbConnect(
         RMariaDB::MariaDB(),
         user     = Sys.getenv("DB_USER"),
         password = Sys.getenv("DB_PASS"),
         host     = '127.0.0.1',
         port     = Sys.getenv("DB_PORT"),
         timeout  = -1,
         'ohasis_api'
      )

      # construct query
      query <- r"(
   SELECT r.CENTRAL_ID,
                   p.PATIENT_ID,
                   p.CONFIRMATORY_CODE,
                   p.UIC,
                   p.PATIENT_CODE,
                   p.PHILHEALTH_NO,
                   p.PHILSYS_ID,
                   p.FIRST,
                   p.MIDDLE,
                   p.LAST,
                   p.SUFFIX,
                   p.BIRTHDATE,
                   p.SEX,
                   p.PRIME,
                   p.SNAPSHOT,
                   p.DELETED_AT
   FROM px_pii AS p
            LEFT JOIN registry AS r ON p.PATIENT_ID = r.PATIENT_ID
      )"

      # add queries
      where_txt <- ""
      for (col in names(cols))
         where_txt[col] <- paste0(col, " REGEXP '", cols[col], "'")

      rows <- paste0("WHERE ", stri_replace_first_regex(paste(collapse = " AND ", where_txt), "^ AND ", ""))

      query <- paste(query, rows)

      # if non-central, limit to facility
      if (faci_id != '130000') query <- paste0(query, " AND FACI_ID ='", faci_id, "'")

      query <- paste0(query, "
GROUP BY IFNULL(r.CENTRAL_ID, p.PATIENT_ID)
      ")
      .log_info("Downloading data.")
      data <- dbGetQuery(conn, query)
      dbDisconnect(conn)

      # clean data
      # cols <- c(
      #    "FIRST",
      #    "MIDDLE",
      #    "LAST",
      #    "SUFFIX",
      #    "UIC",
      #    "CONFIRMATORY_CODE",
      #    "PATIENT_CODE",
      #    "BIRTHDATE",
      #    "SEX"
      # )
      #
      # .log_info("Finalizing data.")
      # # clean cids
      # clean <- data %>%
      #    mutate(
      #       CENTRAL_ID = if_else(
      #          condition = is.na(CENTRAL_ID),
      #          true      = PATIENT_ID,
      #          false     = CENTRAL_ID
      #       ),
      #    ) %>%
      #    distinct(CENTRAL_ID, .keep_all = TRUE)

      # get latest in group from result query
      # clean <- list()
      # for (col in cols) {
      #    col_name     <- as.name(col)
      #    clean[[col]] <- data %>%
      #       filter(is.na(DELETED_AT), !is.na(!!col_name)) %>%
      #       arrange(desc(PRIME), desc(SNAPSHOT)) %>%
      #       distinct(
      #          CENTRAL_ID,
      #          !!col_name
      #       ) %>%
      #       rename(
      #          DATA = col_name
      #       ) %>%
      #       mutate(
      #          VAR = col
      #       ) %>%
      #       mutate_all(~as.character(.)) %>%
      #       distinct(CENTRAL_ID, .keep_all = TRUE)
      # }
      #
      # clean <- bind_rows(clean) %>%
      #    pivot_wider(
      #       id_cols     = CENTRAL_ID,
      #       names_from  = VAR,
      #       values_from = DATA
      #    )
      #
      # # create fcol if no data in group
      # for (col in cols)
      #    if (!(col %in% names(clean)))
      #       clean[, col] <- NA_character_

      .log_info("Creating fullname.")
      # create fullname data
      clean <- data %>%
         mutate(
            FULLNAME = paste0(
               if_else(
                  condition = is.na(LAST),
                  true      = "",
                  false     = LAST
               ), ", ",
               if_else(
                  condition = is.na(FIRST),
                  true      = "",
                  false     = FIRST
               ), " ",
               if_else(
                  condition = is.na(MIDDLE),
                  true      = "",
                  false     = MIDDLE
               ), " ",
               if_else(
                  condition = is.na(SUFFIX),
                  true      = "",
                  false     = SUFFIX
               )
            ),
            FULLNAME = str_squish(FULLNAME),
         ) %>%
         select(
            -FIRST,
            -MIDDLE,
            -LAST,
            -SUFFIX
         )

      .log_info("Sending data.")
      return(clean)
   }
}