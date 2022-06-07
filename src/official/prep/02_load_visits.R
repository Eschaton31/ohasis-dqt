##  Get earliest visit data ----------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")
db_conn <- ohasis$conn("db")
db_name <- "ohasis_warehouse"

# check if prep starts to be re-processed
update <- input(
   prompt  = "Do you want to re-process the PrEP Dispensed Start Dates?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
update <- StrLeft(update, 1) %>% toupper()

# if Yes, re-process
if (update == "1") {
   # download the data
   query <- glue(r"(
SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY VISIT_DATE) AS VISIT_NUM
      FROM (
               SELECT CASE
                          WHEN reg.CENTRAL_ID IS NULL THEN rec.PATIENT_ID
                          WHEN reg.CENTRAL_ID IS NOT NULL THEN reg.CENTRAL_ID
                          END AS CENTRAL_ID,
                      rec.*
               FROM ohasis_warehouse.form_prep rec
                        LEFT JOIN ohasis_warehouse.id_registry reg ON rec.PATIENT_ID = reg.PATIENT_ID
               WHERE PREP_RECORD = 'PrEP' AND VISIT_DATE < '{ohasis$next_date}'
           ) AS data) AS prepstart
WHERE VISIT_NUM = 1;
   )")
   .log_info("Downloading dataset.")
   rs   <- dbSendQuery(lw_conn, query)
   data <- dbFetch(rs)
   dbClearResult(rs)

   # update lake
   .log_info("Clearing old data.")
   table_space <- Id(schema = "ohasis_warehouse", table = "prepdisp_first")
   if (dbExistsTable(lw_conn, table_space))
      dbxDelete(lw_conn, table_space, batch_size = 1000)

   .log_info("Payload = {red(formatC(nrow(data), big.mark = ','))} rows.")
   ohasis$upsert(lw_conn, "warehouse", "prepdisp_first", data, c("CENTRAL_ID", "REC_ID"))
   .log_success("Done!")
}

# check if prep starts to be re-processed
update <- input(
   prompt  = "Do you want to re-process the PrEP Start Dates?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
update <- StrLeft(update, 1) %>% toupper()

# if Yes, re-process
if (update == "1") {
   # download the data
   query <- glue(r"(
SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY VISIT_DATE, LATEST_NEXT_DATE) AS VISIT_NUM
      FROM (
               SELECT CASE
                          WHEN reg.CENTRAL_ID IS NULL THEN rec.PATIENT_ID
                          WHEN reg.CENTRAL_ID IS NOT NULL THEN reg.CENTRAL_ID
                          END AS CENTRAL_ID,
                      rec.*
               FROM ohasis_warehouse.form_prep rec
                        LEFT JOIN ohasis_warehouse.id_registry reg ON rec.PATIENT_ID = reg.PATIENT_ID
               WHERE VISIT_DATE < '{ohasis$next_date}'
           ) AS data) AS prepstart
WHERE VISIT_NUM = 1;
   )")
   .log_info("Downloading dataset.")
   rs   <- dbSendQuery(lw_conn, query)
   data <- dbFetch(rs)
   dbClearResult(rs)

   # update lake
   .log_info("Clearing old data.")
   table_space <- Id(schema = "ohasis_warehouse", table = "prep_first")
   if (dbExistsTable(lw_conn, table_space))
      dbRemoveTable(lw_conn, table_space)

   .log_info("Payload = {red(formatC(nrow(data), big.mark = ','))} rows.")
   ohasis$upsert(lw_conn, "warehouse", "prep_first", data, c("CENTRAL_ID", "REC_ID"))
   .log_success("Done!")
}

##  Get latest visit data ------------------------------------------------------

# check if prep starts to be re-processed
update <- input(
   prompt  = "Do you want to re-process the PrEP Dispensed Latest Dates?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
update <- StrLeft(update, 1) %>% toupper()

# if Yes, re-process
if (update == "1") {
   # download the data
   query <- glue(r"(
SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY VISIT_DATE DESC) AS VISIT_NUM
      FROM (
               SELECT CASE
                          WHEN reg.CENTRAL_ID IS NULL THEN rec.PATIENT_ID
                          WHEN reg.CENTRAL_ID IS NOT NULL THEN reg.CENTRAL_ID
                          END AS CENTRAL_ID,
                      rec.*
               FROM ohasis_warehouse.form_prep rec
                        LEFT JOIN ohasis_warehouse.id_registry reg ON rec.PATIENT_ID = reg.PATIENT_ID
               WHERE PREP_RECORD = 'PrEP' AND VISIT_DATE < '{ohasis$next_date}'
           ) AS data) AS prepstart
WHERE VISIT_NUM = 1;
   )")
   .log_info("Downloading dataset.")
   rs   <- dbSendQuery(lw_conn, query)
   data <- dbFetch(rs)
   dbClearResult(rs)

   # update lake
   .log_info("Clearing old data.")
   table_space <- Id(schema = "ohasis_warehouse", table = "prepdisp_last")
   if (dbExistsTable(lw_conn, table_space))
      dbxDelete(lw_conn, table_space, batch_size = 1000)

   .log_info("Payload = {red(formatC(nrow(data), big.mark = ','))} rows.")
   ohasis$upsert(lw_conn, "warehouse", "prepdisp_last", data, c("CENTRAL_ID", "REC_ID"))
   .log_success("Done!")
}

# check if prep starts to be re-processed
update <- input(
   prompt  = "Do you want to re-process the PrEP Latest Dates?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
update <- StrLeft(update, 1) %>% toupper()

# if Yes, re-process
if (update == "1") {
   # download the data
   query <- glue(r"(
SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY YEAR(VISIT_DATE) DESC, MONTH(VISIT_DATE) DESC, LATEST_NEXT_DATE DESC) AS VISIT_NUM
      FROM (
               SELECT CASE
                          WHEN reg.CENTRAL_ID IS NULL THEN rec.PATIENT_ID
                          WHEN reg.CENTRAL_ID IS NOT NULL THEN reg.CENTRAL_ID
                          END AS CENTRAL_ID,
                      rec.*
               FROM ohasis_warehouse.form_prep rec
                        LEFT JOIN ohasis_warehouse.id_registry reg ON rec.PATIENT_ID = reg.PATIENT_ID
               WHERE VISIT_DATE < '{ohasis$next_date}'
           ) AS data) AS prepstart
WHERE VISIT_NUM = 1;
   )")
   .log_info("Downloading dataset.")
   rs   <- dbSendQuery(lw_conn, query)
   data <- dbFetch(rs)
   dbClearResult(rs)

   # update lake
   .log_info("Clearing old data.")
   table_space <- Id(schema = "ohasis_warehouse", table = "prep_last")
   if (dbExistsTable(lw_conn, table_space))
      dbxDelete(lw_conn, table_space, batch_size = 1000)

   .log_info("Payload = {red(formatC(nrow(data), big.mark = ','))} rows.")
   ohasis$upsert(lw_conn, "warehouse", "prep_last", data, c("CENTRAL_ID", "REC_ID"))
   .log_success("Done!")
}

.log_info("Closing connections.")
dbDisconnect(lw_conn)
dbDisconnect(db_conn)
.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))

# TODO: Add option to process locally
