yr <- "2024"
mo <- "09"

min <- "2024-07-01"
max <- as.character(end_ym(yr, mo))

conn <- connect("ohasis-lw")

tpt_all <- QB$new(conn)$from("ohasis_warehouse.form_art_bc")
tpt_all$where("VISIT_DATE", "<=", max)
tpt_all$select(PATIENT_ID,
               REC_ID,
               VISIT_DATE,
               TB_IPT_STATUS,
               TB_IPT_OUTCOME,
               TB_IPT_OUTCOME_OTHER,
               TB_IPT_START_DATE,
               TB_IPT_END_DATE)
tpt_all$where(function(query = QB$new(conn)) {
   query$whereNotNull("TB_IPT_STATUS", "or")$
      whereNotNull("TB_IPT_OUTCOME", "or")$
      whereNotNull("TB_IPT_OUTCOME_OTHER", "or")$
      whereNotNull("TB_IPT_START_DATE", "or")$
      whereNotNull("TB_IPT_END_DATE", "or")
   query$whereNested
})
tpt_all <- tpt_all$get()

notb <- QB$new(conn)$
   from("ohasis_warehouse.form_art_bc")$
   select(PATIENT_ID,
          VISIT_DATE,
          REC_ID,
          TB_STATUS)$
   where("VISIT_DATE", "<=", max)$
   where("TB_STATUS", "0_No active TB")$
   get()

# tpt_ever <- QB$new(conn)$from("ohasis_warehouse.tpt_ever")$get()
id_reg <- QB$new(conn)$from("ohasis_warehouse.id_registry")$select("CENTRAL_ID", "PATIENT_ID")$get()

dbDisconnect(conn)

tpt_ever <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      ),
   ) %>%
   filter(keep == 1) %>%
   distinct(CENTRAL_ID) %>%
   mutate(ever_tpt = 1)

tpt_year <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   # filter(keep == 1) %>%
   filter((TB_IPT_STATUS == "12_Started IPT" & year(VISIT_DATE) == as.numeric(yr)) | (year(TB_IPT_START_DATE) == as.numeric(yr))) %>%
   distinct(CENTRAL_ID) %>%
   mutate(year_tpt = 1)

tpt_s1 <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   # filter(keep == 1) %>%
   filter((TB_IPT_STATUS == "12_Started IPT" &
      VISIT_DATE >= start_ym(yr, "01") &
      VISIT_DATE <= start_ym(yr, "06")) | (TB_IPT_START_DATE >= start_ym(yr, "01") & TB_IPT_START_DATE <= start_ym(yr, "06"))) %>%
   distinct(CENTRAL_ID) %>%
   mutate(s1_tpt = 1)
tpt_s2 <- tpt_all %>%
   filter() %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   # filter(keep == 1) %>%
   filter((TB_IPT_STATUS == "12_Started IPT" &
      VISIT_DATE >= start_ym(yr, "07") &
      VISIT_DATE <= start_ym(yr, "12")) | (TB_IPT_START_DATE >= start_ym(yr, "07") & TB_IPT_START_DATE <= start_ym(yr, "12"))) %>%
   distinct(CENTRAL_ID) %>%
   mutate(s2_tpt = 1)

qr <- list(
   `1` = c(start_ym(yr, "01"), end_ym(yr, "03")),
   `2` = c(start_ym(yr, "04"), end_ym(yr, "06")),
   `3` = c(start_ym(yr, "07"), end_ym(yr, "09")),
   `4` = c(start_ym(yr, "10"), end_ym(yr, "12"))
)

tpt_q1 <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   # filter(keep == 1) %>%
   filter((TB_IPT_STATUS == "12_Started IPT" &
      VISIT_DATE >= qr$`1`[1] &
      VISIT_DATE <= qr$`1`[2]) | (TB_IPT_START_DATE >= qr$`1`[1] & TB_IPT_START_DATE <= qr$`1`[2])) %>%
   distinct(CENTRAL_ID) %>%
   mutate(q1_tpt = 1)
tpt_q2 <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   # filter(keep == 1) %>%
   filter((TB_IPT_STATUS == "12_Started IPT" &
      VISIT_DATE >= qr$`2`[1] &
      VISIT_DATE <= qr$`2`[2]) | (TB_IPT_START_DATE >= qr$`2`[1] & TB_IPT_START_DATE <= qr$`2`[2])) %>%
   distinct(CENTRAL_ID) %>%
   mutate(q2_tpt = 2)
tpt_q3 <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   # filter(keep == 1) %>%
   filter((TB_IPT_STATUS == "12_Started IPT" &
      VISIT_DATE >= qr$`3`[1] &
      VISIT_DATE <= qr$`3`[2]) | (TB_IPT_START_DATE >= qr$`3`[1] & TB_IPT_START_DATE <= qr$`3`[2])) %>%
   distinct(CENTRAL_ID) %>%
   mutate(q3_tpt = 1)
tpt_q4 <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   # filter(keep == 1) %>%
   filter((TB_IPT_STATUS == "12_Started IPT" &
      VISIT_DATE >= qr$`4`[1] &
      VISIT_DATE <= qr$`4`[2]) | (TB_IPT_START_DATE >= qr$`4`[1] & TB_IPT_START_DATE <= qr$`4`[2])) %>%
   distinct(CENTRAL_ID) %>%
   mutate(q4_tpt = 1)

notb_ever <- notb %>%
   get_cid(id_reg, PATIENT_ID) %>%
   distinct(CENTRAL_ID) %>%
   mutate(ever_notb = 1)

tx_curr <- hs_data("harp_tx", "reg", yr, mo) %>%
   read_dta(col_select = c(art_id, PATIENT_ID)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", yr, mo) %>%
         read_dta(col_select = c(art_id, outcome, onart, hub, branch, realhub, realhub_branch, curr_age, sex)),
      by = join_by(art_id)
   )

art_ever_tpt <- tx_curr %>%
   mutate(
      curr_age_c = coalesce(gen_agegrp(curr_age, "harp"), "(no data)"),
      sex        = coalesce(str_to_title(sex), "(no data)")
   ) %>%
   left_join(
      y  = tpt_ever,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tpt_s1,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tpt_s2,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tpt_q1,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tpt_q2,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tpt_q3,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tpt_q4,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = notb_ever,
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate_at(
      .vars = vars(ever_tpt),
      ~coalesce(., as.integer(0))
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(TX_FACI = "hub", TX_SUB_FACI = "branch")
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(REAL_FACI = "realhub", REAL_SUB_FACI = "realhub_branch")
   ) %>%
   mutate_at(
      .vars = vars(TX_FACI, REAL_FACI),
      ~if_else(. == "130000", NA_character_, ., .),
   ) %>%
   ohasis$get_faci(
      list("txfaci" = c("TX_FACI", "TX_SUB_FACI")),
      "name",
      c("txfaci_region", "txfaci_province", "txfaci_muncity")
   ) %>%
   ohasis$get_faci(
      list("realfaci" = c("REAL_FACI", "REAL_SUB_FACI")),
      "name",
      c("realfaci_region", "realfaci_province", "realfaci_muncity")
   ) %>%
   arrange(art_id) %>%
   distinct(art_id, .keep_all = TRUE)

# UPLOADING YOUR OWN DATA INTO MariaDB
# 1) open a connection to the server
conn <- ohasis$conn("lw")

# 2) get your data into an object
data <- art_ever_tpt

# 3) define your primary key (unique id)
# NOTE: can be multiple columns
id <- "art_id"

# 4) which schema/db are you using
schema <- "dashboard"

# 5) what is its table name?
table <- stri_c("tpt_", yr, mo) # "lala.mydata"

# 6) upload data
table_space <- Id(schema = schema, table = table)
if (dbExistsTable(conn, table_space)) {
   dbRemoveTable(conn, table_space)
}
ohasis$upsert(conn, schema, table, data, id)

# 7) close connection
dbDisconnect(conn)

# write_dta(format_stata(art_ever_tpt), "H:/20240809_tbhiv-evertpt_2024-06.dta")
# 
# art_ever_tpt %>% tab(onart, no_tb, ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1) %>%
#    tab(ever_no_tb)
# 
# art_ever_tpt %>%
#    tab(ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1) %>%
#    tab(ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1) %>%
#    tab(ever_no_tb, ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1, ever_no_tb == 1) %>%
#    tab(ever_ipt)
