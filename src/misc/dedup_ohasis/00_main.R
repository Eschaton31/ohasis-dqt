##  ohasis Deduplication Controller -------------------------------------------------

source("src/misc/dedup_ohasis/01_pii.R")
source("src/misc/dedup_ohasis/02_dedup_fns.R")

# ohasis$data_factory("lake", "px_pii", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
# ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
dedup <- dedup_download()
dedup <- dedup_linelist(dedup)

dedup_sure <- function() {
   nrow_reg  <- nrow(.GlobalEnv$check_dupes$registry)
   nrow_norm <- nrow(.GlobalEnv$check_dupes$normal)
   if (nrow_reg > 0)
      .GlobalEnv$dedup$id_registry <- upload_dupes2(.GlobalEnv$check_dupes$registry_up, .GlobalEnv$dedup$id_registry)

   if (nrow_norm > 0)
      .GlobalEnv$dedup$id_registry <- upload_dupes2(.GlobalEnv$check_dupes$normal_up, .GlobalEnv$dedup$id_registry)

   if (nrow_reg + nrow_norm > 0)
      .GlobalEnv$dedup <- dedup_linelist2(.GlobalEnv$dedup)

}

check_dupes <- ohasis_dupes(first_sieve, middle_sieve, last_sieve, uic)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_sieve, uic)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, middle_sieve, last_sieve, uic_sort)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_sieve, uic_sort)
dedup_sure()
# check_dupes <- ohasis_dupes(first_ny, middle_ny, last_ny, uic)
# check_dupes <- ohasis_dupes(first_ny, last_ny, uic)
# check_dupes <- ohasis_dupes(first_sieve, middle_sieve, last_sieve, birthdate, uic_order)
check_dupes <- ohasis_dupes(first_sieve, last_sieve, birthdate, confirm_sieve)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_sieve, birthdate, pxcode_sieve)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_sieve, birthdate, phic)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_sieve, birthdate, philsys)
dedup_sure()
# check_dupes <- ohasis_dupes(first_ny, last_ny, birthdate, confirm_sieve)
# check_dupes <- ohasis_dupes(first_ny, last_ny, birthdate, pxcode_sieve)
# check_dupes <- ohasis_dupes(first_ny, last_ny, birthdate, phic)
# check_dupes <- ohasis_dupes(first_ny, last_ny, birthdate, philsys)
# check_dupes <- ohasis_dupes(first_a, last_a, birthdate, confirm_sieve)
# check_dupes <- ohasis_dupes(first_a, last_a, birthdate, pxcode_sieve)
# check_dupes <- ohasis_dupes(first_a, last_a, birthdate, phic)
# check_dupes <- ohasis_dupes(first_a, last_a, birthdate, philsys)
check_dupes <- ohasis_dupes(first_sieve, middle_sieve, last_a, uic)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic)
dedup_sure()
# check_dupes <- ohasis_dupes(first_a, middle_sieve, last_sieve, uic)
# check_dupes <- ohasis_dupes(first_a, last_sieve, uic)
# check_dupes <- ohasis_dupes(first_a, middle_sieve, last_a, uic)
check_dupes <- ohasis_dupes(namesort_first, namesort_last, uic)
dedup_sure()
# check_dupes <- ohasis_dupes(namesort_first, namesort_last, uic_sort)

check_dupes <- ohasis_dupes(first_sieve, uic, client_mobile)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, birthdate, client_mobile)
dedup_sure()
# check_dupes <- ohasis_dupes(first_ny, uic, client_mobile)
# check_dupes <- ohasis_dupes(first_ny, birthdate, client_mobile)
check_dupes <- ohasis_dupes(first_sieve, uic, client_email)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, birthdate, client_email)
dedup_sure()
# check_dupes <- ohasis_dupes(first_ny, uic, client_email)
# check_dupes <- ohasis_dupes(first_ny, birthdate, client_email)
check_dupes <- ohasis_dupes(first_sieve, uic, perm_prov)
dedup_sure()
# check_dupes <- ohasis_dupes(first_sieve, last_ny, birthdate, perm_munc)
check_dupes <- ohasis_dupes(first_sieve, uic, curr_prov)
dedup_sure()
# check_dupes <- ohasis_dupes(first_sieve, last_ny, birthdate, curr_munc)
# check_dupes <- ohasis_dupes(first_sieve, birthdate, pxcode_sieve)
# check_dupes <- ohasis_dupes(first_sieve, birth_yr, client_mobile)
# check_dupes <- ohasis_dupes(first_sieve, birth_mo, client_mobile)
# check_dupes <- ohasis_dupes(first_sieve, birth_dy, client_mobile)
# check_dupes <- ohasis_dupes(first_ny, birth_yr, client_mobile)
# check_dupes <- ohasis_dupes(first_ny, birth_mo, client_mobile)
# check_dupes <- ohasis_dupes(first_ny, birth_dy, client_mobile)
# check_dupes <- ohasis_dupes(first_sieve, birth_yr, client_email)
# check_dupes <- ohasis_dupes(first_sieve, birth_mo, client_email)
# check_dupes <- ohasis_dupes(first_sieve, birth_dy, client_email)
# check_dupes <- ohasis_dupes(first_ny, birth_yr, client_email)
# check_dupes <- ohasis_dupes(first_ny, birth_mo, client_email)
# check_dupes <- ohasis_dupes(first_ny, birth_dy, client_email)

check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birthdate, perm_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birthdate, perm_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birthdate, curr_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birthdate, curr_munc)
dedup_sure()

check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, uic_2, birth_yr, birth_mo, perm_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, uic_2, birth_yr, birth_dy, perm_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, uic_2, birth_mo, birth_dy, perm_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, uic_2, birth_yr, birth_mo, curr_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, uic_2, birth_yr, birth_dy, curr_munc)
dedup_sure()
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, uic_2, birth_mo, birth_dy, curr_munc)
dedup_sure()

check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birth_yr, birth_mo, perm_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birth_yr, birth_dy, perm_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birth_mo, birth_dy, perm_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birth_yr, birth_mo, perm_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birth_yr, birth_dy, perm_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birth_mo, birth_dy, perm_munc)

check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birth_yr, birth_mo, curr_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birth_yr, birth_dy, curr_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_1, birth_mo, birth_dy, curr_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birth_yr, birth_mo, curr_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birth_yr, birth_dy, curr_munc)
check_dupes <- ohasis_dupes(first_sieve, last_a, uic_2, birth_mo, birth_dy, curr_munc)


check_dupes <- ohasis_dupes(pxcode_sieve, client_mobile, client_email)
dedup_sure()

check_dupes <- ohasis_dupes(first_sieve, confirm_sieve)
dedup_sure()
check_dupes <- ohasis_dupes(first_ny, confirm_sieve)
check_dupes <- ohasis_dupes(first_sieve, pxcode_sieve)
check_dupes <- ohasis_dupes(first_ny, pxcode_sieve, birth_yr, birth_mo)
check_dupes <- ohasis_dupes(last_sieve, confirm_sieve, birth_yr)
check_dupes <- ohasis_dupes(last_sieve, confirm_sieve)
check_dupes <- ohasis_dupes(last_ny, confirm_sieve, birth_yr)
check_dupes <- ohasis_dupes(last_ny, confirm_sieve)
check_dupes <- ohasis_dupes(uic, client_mobile, first_a)
dedup_sure()
check_dupes <- ohasis_dupes(uic, client_email, first_a)
dedup_sure()
check_dupes <- ohasis_dupes(uic, client_mobile, first_ny)
dedup_sure()
check_dupes <- ohasis_dupes(uic, client_email, first_ny)
dedup_sure()
check_dupes <- ohasis_dupes(uic, client_mobile, last_a)
dedup_sure()
check_dupes <- ohasis_dupes(uic, client_email, last_a)
dedup_sure()
check_dupes <- ohasis_dupes(uic, client_mobile, last_ny)
dedup_sure()
check_dupes <- ohasis_dupes(uic, client_email, last_ny)
dedup_sure()

check_dupes <- ohasis_dupes(first_sieve, client_mobile, birth_yr)
dedup_sure()
check_dupes <- ohasis_dupes(first_ny, client_mobile, birth_yr)
check_dupes <- ohasis_dupes(first_a, client_mobile, birth_yr)
check_dupes <- ohasis_dupes(first_sieve, client_mobile, birth_mo)
check_dupes <- ohasis_dupes(first_ny, client_mobile, birth_mo)
check_dupes <- ohasis_dupes(first_a, client_mobile, birth_mo)
check_dupes <- ohasis_dupes(first_sieve, client_mobile, birth_dy)
check_dupes <- ohasis_dupes(first_ny, client_mobile, birth_dy)
check_dupes <- ohasis_dupes(first_a, client_mobile, birth_dy)

check_dupes <- ohasis_dupes(first_sieve, client_email, birth_yr)
dedup_sure()
check_dupes <- ohasis_dupes(first_ny, client_email, birth_yr)
check_dupes <- ohasis_dupes(first_a, client_email, birth_yr)
check_dupes <- ohasis_dupes(first_sieve, client_email, birth_mo)
check_dupes <- ohasis_dupes(first_ny, client_email, birth_mo)
check_dupes <- ohasis_dupes(first_a, client_email, birth_mo)
check_dupes <- ohasis_dupes(first_sieve, client_email, birth_dy)
check_dupes <- ohasis_dupes(first_ny, client_email, birth_dy)
check_dupes <- ohasis_dupes(first_a, client_email, birth_dy)

# upload
upload_dupes(check_dupes$registry_up)
upload_dupes(check_dupes$normal_up)

# new process
dedup$id_registry <- upload_dupes2(check_dupes$registry_up, dedup$id_registry)
dedup$id_registry <- upload_dupes2(check_dupes$normal_up, dedup$id_registry)
dedup             <- dedup_linelist2(dedup)

from              <- "2025-11-12 21:32:00"
dedup$id_registry <- upload_dupes2(check_dupes$normal_up, dedup$id_registry, TRUE, from)

check_dupes$registry %>%
   mutate(
      type    = "reg",
      .before = 1
   ) %>%
   bind_rows(
      check_dupes$normal %>%
         mutate(
            type    = "norm",
            .before = 1
         )
   ) %>%
   # select(-patient_id) %>%
   View()

ohasis$data_factory("lake", "px_pii", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
dedup      <- dedup_download()
pii        <- dedup$pii
id_reg     <- dedup$id_registry
pii_unique <- pii %>%
   get_cid(id_reg, patient_id) %>%
   left_join(
      y  = work,
      by = join_by(rec_id)
   ) %>%
   select(-rec_id, -faci_id, -sub_faci_id, -deleted_at) %>%
   unite(
      col   = "perm_addr",
      sep   = ", ",
      perm_reg,
      perm_prov,
      perm_munc,
      na.rm = TRUE
   ) %>%
   unite(
      col   = "curr_addr",
      sep   = ", ",
      curr_reg,
      curr_prov,
      curr_munc,
      na.rm = TRUE
   ) %>%
   pivot_longer(
      cols = c(
         first,
         middle,
         last,
         suffix,
         uic,
         confirmatory_code,
         patient_code,
         birthdate,
         philsys_id,
         philhealth_no,
         client_email,
         client_mobile,
         sex,
         work,
         curr_addr,
         perm_addr,
      )
   ) %>%
   mutate(
      sort = if_else(!is.na(value), 1, 9999, 9999)
   ) %>%
   arrange(sort, desc(snapshot)) %>%
   distinct(central_id, name, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = central_id,
      names_from  = name,
      values_from = value
   )

write_rds(pii_unique, "H:/20250328-pii_unique.rds")
pii_unique <- read_rds("H:/20250328-pii_unique.rds")

data <- pii_unique %>%
   mutate(id = row_number()) %>%
   rename(occupation = work) %>%
   separate_wider_delim(
      cols    = curr_addr,
      delim   = ", ",
      names   = c("curr_reg", "curr_prov", "curr_munc"),
      too_few = "align_start"
   ) %>%
   separate_wider_delim(
      cols    = perm_addr,
      delim   = ", ",
      names   = c("perm_reg", "perm_prov", "perm_munc"),
      too_few = "align_start"
   ) %>%
   mutate(
      use_curr      = coalesce(curr_munc == "unknown" | curr_munc == "overseas", FALSE),
      permcurr_reg  = if_else(
         condition = use_curr == 1,
         true      = curr_reg,
         false     = perm_reg
      ),
      permcurr_prov = if_else(
         condition = use_curr == 1,
         true      = curr_prov,
         false     = perm_prov
      ),
      permcurr_munc = if_else(
         condition = use_curr == 1,
         true      = curr_munc,
         false     = perm_munc
      ),
   ) %>%
   select(
      -use_curr,
      -starts_with("perm_"),
      -starts_with("curr_"),
   ) %>%
   rename_all(tolower) %>%
   rename(central_id = central_id) %>%
   mutate(
      birthdate = as.Date(parse_date_time(birthdate, "Ymd"))
   )

conn <- connect('mariadb-lw')
per  <- QB$new(conn)$from('ohasis_lake.ohasis_pii_per_cid')$get()
dbDisconnect(conn)

write_rds(per, "H:/ohasis_pii_per_cid.rds")

per <- read_rds("H:/ohasis_pii_per_cid.rds")
per %<>%
   mutate(
      row_id     = row_number(),
      occupation = coalesce(curr_work, prev_work)
   ) %>%
   rename(central_id = cid)
try <- Dedup$new()
try$setMaster(per, "row_id")
try$preparePii()
try$splinkDedupe()
try$exact()


lw_conn <- connect('ohasis-cdc')
dbExecute(lw_conn, glue(r"(TRUNCATE `ohasis`.`dedup_old-exact`)"))
dbAppendTable(lw_conn, 'dedup_old-exact', try$review$exact, row.names = NA)
dbDisconnect(lw_conn)


work   <- QB$new(`oh-lw`)$select(rec_id, work)$from("ohasis_lake.px_occupation")$whereNotNull("work")$get()
id_reg <- QB$new(`oh-lw`)$select(central_id, patient_id)$from("ohasis_warehouse.id_registry")$get()

lw_conn <- ohasis$conn("lw")
dbExecute(lw_conn, "delete from ohasis_lake.pii_unique where patient_id is not NULL;")
ohasis$upsert(lw_conn, "lake", "pii_unique", pii_unique, "patient_id")
dbDisconnect(lw_conn)

change_px_id('2024012814706oj1300000048', '20220120130001d574', "2022071413000173v3")

distinct_pii <- function(cid1, cid2) {
   data <- dedup$pii %>%
      select(
         central_id,
         patient_id,
         first,
         middle,
         last,
         suffix,
         uic,
         confirmatory_code,
         patient_code,
         birthdate,
         philsys_id,
         philhealth_no,
         client_email,
         client_mobile,
         sex,
         perm_reg,
         perm_prov,
         perm_munc,
         curr_reg,
         curr_prov,
         curr_munc,
      )

   return(
      list(
         p1 = data %>% filter(central_id == cid1),
         p2 = data %>% filter(central_id == cid2)
      )
   )
}

lapply(
   c('20240104101246q0400030011'),
   change_px_id,
   'harp08011816074489',
   'harp08011816049826'
)

periods <- list(
   # c(format(start_ym(2023, 7), "%Y-%m-%d 00:00:00"), format(end_ym(2023, 7), "%Y-%m-%d 11:59:59")),
   # c(format(start_ym(2023, 8), "%Y-%m-%d 00:00:00"), format(end_ym(2023, 8), "%Y-%m-%d 11:59:59")),
   # c(format(start_ym(2023, 9), "%Y-%m-%d 00:00:00"), format(end_ym(2023, 9), "%Y-%m-%d 11:59:59")),
   # c(format(start_ym(2021, 1), "%Y-%m-%d 00:00:00"), format(end_ym(2021, 12), "%Y-%m-%d 11:59:59")),
   # c(format(start_ym(2022, 1), "%Y-%m-%d 00:00:00"), format(end_ym(2022, 12), "%Y-%m-%d 11:59:59")),
   # c(format(start_ym(2023, 1), "%Y-%m-%d 00:00:00"), format(end_ym(2023, 12), "%Y-%m-%d 11:59:59")),
   c(format(start_ym(2024, 1), "%Y-%m-%d 00:00:00"), format(end_ym(2024, 12), "%Y-%m-%d 11:59:59")),
   c(format(start_ym(2025, 1), "%Y-%m-%d 00:00:00"), format(end_ym(2025, 12), "%Y-%m-%d 11:59:59"))
)


for (period in periods) download_pii(period[1], period[2])

pii <- read_rds(Sys.getenv("DEDUP_PII"))
write_rds(pii, Sys.getenv("DEDUP_PII"))

download_pii <- function(min, max) {
   if (missing(max)) {
      max <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   }

   pii <- tibble(patient_id = NA_character_, birthdate = NA_Date_) %>%
      slice(0)
   # if (file.exists(Sys.getenv("dedup_pii")))
   #    pii <- read_rds(Sys.getenv("dedup_pii"))

   lw_conn  <- connect("mariadb-lw")
   new_data <- QB$new(lw_conn)$from('ohasis_lake.patients')
   new_data$where(function(query = QB$new(lw_conn)) {
      query$whereBetween('created_at', c(min, max), "or")
      query$whereBetween('updated_at', c(min, max), "or")
      query$whereBetween('deleted_at', c(min, max), "or")
      query$whereNested
   })
   new_data <- new_data$get()
   dbDisconnect(lw_conn)

   new_data %<>%
      mutate_at(
         .vars = vars(
            first,
            middle,
            last,
            suffix,
            confirmatory_code,
            patient_code,
            uic,
            philhealth_no,
            philsys_id,
            client_mobile,
            client_email
         ),
         ~clean_pii(.)
      ) %>%
      mutate(
         client_mobile = str_replace_all(client_mobile, "[^[:digit:]]", ""),
         client_mobile = case_when(
            str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
            str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
            TRUE ~ client_mobile
         ),
         birthdate     = as.character(birthdate)
      )

   # finalize data
   .GlobalEnv$pii %<>%
      # remove old version of record
      anti_join(select(new_data, patient_id)) %>%
      # append new data
      mutate(birthdate = as.character(birthdate)) %>%
      bind_rows(new_data) %>%
      filter(is.na(deleted_at))

   # write to local file for later use
   # write_rds(pii, Sys.getenv("dedup_pii"))
}

dupes <- read_excel("H:/splink_review.xlsx", col_types = "text")
upload_dupes(dupes)