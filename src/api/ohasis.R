#* @filter cors
cors <- function(req, res) {

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

#* Return a list of clients
#* @param faci_id The facility id of the requestor
#* @param search The search string
#* @post /client
function(faci_id, search) {
   # initiate connection
   conn <- dbConnect(
      RMariaDB::MariaDB(),
      user     = Sys.getenv("LW_USER"),
      password = Sys.getenv("LW_PASS"),
      host     = Sys.getenv("LW_HOST"),
      port     = Sys.getenv("LW_PORT"),
      timeout  = -1
   )

   # construct query
   query <- r"(
SELECT DISTINCT i.CENTRAL_ID,
                p.PATIENT_ID,
                p.CONFIRMATORY_CODE,
                p.UIC,
                p.PATIENT_CODE,
                p.FIRST,
                p.MIDDLE,
                p.LAST,
                p.SUFFIX,
                p.BIRTHDATE,
                p.SEX,
                p.SNAPSHOT,
                p.DELETED_AT
FROM ohasis_lake.px_pii AS p
         LEFT JOIN ohasis_warehouse.id_registry AS i ON p.PATIENT_ID = i.PATIENT_ID
   )"

   # add queries
   where     <- strsplit(search, ' ')[[1]]
   where_txt <- ""
   for (i in seq_len(length(where)))
      where_txt[i] <- paste0("CONCAT_WS(' ', p.UIC, p.FIRST, p.LAST) REGEXP '", where[i], "'")

   rows <- paste0("WHERE ", paste(collapse = " AND ", where_txt))

   query <- paste(query, rows)

   if (faci_id != '130000') query <- paste0(query, " AND FACI_ID ='", faci_id, "'")

   data <- dbGetQuery(conn, query)
   dbDisconnect(conn)

   # clean data
   cols <- c(
      "FIRST",
      "MIDDLE",
      "LAST",
      "SUFFIX",
      "UIC",
      "CONFIRMATORY_CODE",
      "PATIENT_CODE",
      "BIRTHDATE",
      "SEX"
   )

   clean <- list()
   for (col in cols) {
      col_name     <- as.name(col)
      clean[[col]] <- data %>%
         filter(is.na(DELETED_AT), !is.na(!!col_name)) %>%
         arrange(desc(SNAPSHOT)) %>%
         distinct(
            CENTRAL_ID,
            !!col_name
         ) %>%
         rename(
            DATA = col_name
         ) %>%
         mutate(
            VAR = col
         ) %>%
         mutate_all(~as.character(.)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE)
   }

   clean <- bind_rows(clean) %>%
      pivot_wider(
         id_cols     = CENTRAL_ID,
         names_from  = VAR,
         values_from = DATA
      )

   return(clean)
}