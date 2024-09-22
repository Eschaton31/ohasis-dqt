##  OHASIS Deduplication Controller -------------------------------------------------

source("src/misc/dedup_ohasis/01_pii.R")
source("src/misc/dedup_ohasis/02_dedup_fns.R")

ohasis$data_factory("lake", "px_pii", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
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

check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE, UIC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, UIC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE, UIC_SORT)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, UIC_SORT)
dedup_sure()
# check_dupes <- ohasis_dupes(FIRST_NY, MIDDLE_NY, LAST_NY, UIC)
# check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, UIC)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE, BIRTHDATE, UIC_ORDER)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, CONFIRM_SIEVE)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, PXCODE_SIEVE)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, PHIC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, PHILSYS)
dedup_sure()
# check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, CONFIRM_SIEVE)
# check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PXCODE_SIEVE)
# check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PHIC)
# check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PHILSYS)
# check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, CONFIRM_SIEVE)
# check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, PXCODE_SIEVE)
# check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, PHIC)
# check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, PHILSYS)
check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_A, UIC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC)
dedup_sure()
# check_dupes <- ohasis_dupes(FIRST_A, MIDDLE_SIEVE, LAST_SIEVE, UIC)
# check_dupes <- ohasis_dupes(FIRST_A, LAST_SIEVE, UIC)
# check_dupes <- ohasis_dupes(FIRST_A, MIDDLE_SIEVE, LAST_A, UIC)
check_dupes <- ohasis_dupes(NAMESORT_FIRST, NAMESORT_LAST, UIC)
dedup_sure()
# check_dupes <- ohasis_dupes(NAMESORT_FIRST, NAMESORT_LAST, UIC_SORT)

check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, CLIENT_MOBILE)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTHDATE, CLIENT_MOBILE)
dedup_sure()
# check_dupes <- ohasis_dupes(FIRST_NY, UIC, CLIENT_MOBILE)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTHDATE, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, CLIENT_EMAIL)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTHDATE, CLIENT_EMAIL)
dedup_sure()
# check_dupes <- ohasis_dupes(FIRST_NY, UIC, CLIENT_EMAIL)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTHDATE, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, PERM_PROV)
dedup_sure()
# check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_NY, BIRTHDATE, PERM_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, CURR_PROV)
dedup_sure()
# check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_NY, BIRTHDATE, CURR_MUNC)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTHDATE, PXCODE_SIEVE)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_YR, CLIENT_MOBILE)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_MO, CLIENT_MOBILE)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_DY, CLIENT_MOBILE)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_YR, CLIENT_MOBILE)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_MO, CLIENT_MOBILE)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_DY, CLIENT_MOBILE)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_YR, CLIENT_EMAIL)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_MO, CLIENT_EMAIL)
# check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_DY, CLIENT_EMAIL)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_YR, CLIENT_EMAIL)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_MO, CLIENT_EMAIL)
# check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_DY, CLIENT_EMAIL)

check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTHDATE, PERM_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTHDATE, PERM_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTHDATE, CURR_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTHDATE, CURR_MUNC)
dedup_sure()

check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, UIC_2, BIRTH_YR, BIRTH_MO, PERM_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, UIC_2, BIRTH_YR, BIRTH_DY, PERM_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, UIC_2, BIRTH_MO, BIRTH_DY, PERM_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, UIC_2, BIRTH_YR, BIRTH_MO, CURR_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, UIC_2, BIRTH_YR, BIRTH_DY, CURR_MUNC)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, UIC_2, BIRTH_MO, BIRTH_DY, CURR_MUNC)
dedup_sure()

check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTH_YR, BIRTH_MO, PERM_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTH_YR, BIRTH_DY, PERM_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTH_MO, BIRTH_DY, PERM_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTH_YR, BIRTH_MO, PERM_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTH_YR, BIRTH_DY, PERM_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTH_MO, BIRTH_DY, PERM_MUNC)

check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTH_YR, BIRTH_MO, CURR_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTH_YR, BIRTH_DY, CURR_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_1, BIRTH_MO, BIRTH_DY, CURR_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTH_YR, BIRTH_MO, CURR_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTH_YR, BIRTH_DY, CURR_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC_2, BIRTH_MO, BIRTH_DY, CURR_MUNC)

check_dupes <- ohasis_dupes(FIRST_SIEVE, CONFIRM_SIEVE)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_NY, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST_NY, PXCODE_SIEVE, BIRTH_YR, BIRTH_MO)
check_dupes <- ohasis_dupes(LAST_SIEVE, CONFIRM_SIEVE, BIRTH_YR)
check_dupes <- ohasis_dupes(LAST_SIEVE, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(LAST_NY, CONFIRM_SIEVE, BIRTH_YR)
check_dupes <- ohasis_dupes(LAST_NY, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(UIC, CLIENT_MOBILE, FIRST_A)
dedup_sure()
check_dupes <- ohasis_dupes(UIC, CLIENT_EMAIL, FIRST_A)
dedup_sure()
check_dupes <- ohasis_dupes(UIC, CLIENT_MOBILE, FIRST_NY)
dedup_sure()
check_dupes <- ohasis_dupes(UIC, CLIENT_EMAIL, FIRST_NY)
dedup_sure()
check_dupes <- ohasis_dupes(UIC, CLIENT_MOBILE, LAST_A)
dedup_sure()
check_dupes <- ohasis_dupes(UIC, CLIENT_EMAIL, LAST_A)
dedup_sure()
check_dupes <- ohasis_dupes(UIC, CLIENT_MOBILE, LAST_NY)
dedup_sure()
check_dupes <- ohasis_dupes(UIC, CLIENT_EMAIL, LAST_NY)
dedup_sure()

check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_MOBILE, BIRTH_YR)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_MOBILE, BIRTH_YR)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_MOBILE, BIRTH_YR)
check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_MOBILE, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_MOBILE, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_MOBILE, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_MOBILE, BIRTH_DY)
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_MOBILE, BIRTH_DY)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_MOBILE, BIRTH_DY)

check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_EMAIL, BIRTH_YR)
dedup_sure()
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_EMAIL, BIRTH_YR)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_EMAIL, BIRTH_YR)
check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_EMAIL, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_EMAIL, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_EMAIL, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_EMAIL, BIRTH_DY)
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_EMAIL, BIRTH_DY)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_EMAIL, BIRTH_DY)

# upload
upload_dupes(check_dupes$registry_up)
upload_dupes(check_dupes$normal_up)

# new process
dedup$id_registry <- upload_dupes2(check_dupes$registry_up, dedup$id_registry)
dedup$id_registry <- upload_dupes2(check_dupes$normal_up, dedup$id_registry)
dedup             <- dedup_linelist2(dedup)

from              <- "2024-09-17 14:55:00"
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
   # select(-PATIENT_ID) %>%
   View()

ohasis$data_factory("lake", "px_pii", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
dedup      <- dedup_download()
pii_unique <- dedup$pii %>%
   select(-REC_ID, -FACI_ID, -SUB_FACI_ID, -DELETED_AT) %>%
   pivot_longer(
      cols = c(
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         UIC,
         CONFIRMATORY_CODE,
         PATIENT_CODE,
         BIRTHDATE,
         PHILSYS_ID,
         PHILHEALTH_NO,
         CLIENT_EMAIL,
         CLIENT_MOBILE,
         SEX,
         PERM_REG,
         PERM_PROV,
         PERM_MUNC,
         CURR_REG,
         CURR_PROV,
         CURR_MUNC,
      )
   ) %>%
   mutate(
      sort = if_else(!is.na(value), 1, 9999, 9999)
   ) %>%
   arrange(sort, desc(SNAPSHOT)) %>%
   distinct(PATIENT_ID, name, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = PATIENT_ID,
      names_from  = name,
      values_from = value
   ) %>%
   get_cid(dedup$id_registry, PATIENT_ID)


lw_conn <- ohasis$conn("lw")
dbExecute(lw_conn, "DELETE FROM ohasis_lake.pii_unique WHERE PATIENT_ID IS NOT NULL;")
ohasis$upsert(lw_conn, "lake", "pii_unique", pii_unique, "PATIENT_ID")
dbDisconnect(lw_conn)

change_px_id('2024012814706OJ1300000048', '20220120130001D574', "2022071413000173V3")

distinct_pii <- function(cid1, cid2) {
   data <- dedup$pii %>%
      select(
         CENTRAL_ID,
         PATIENT_ID,
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         UIC,
         CONFIRMATORY_CODE,
         PATIENT_CODE,
         BIRTHDATE,
         PHILSYS_ID,
         PHILHEALTH_NO,
         CLIENT_EMAIL,
         CLIENT_MOBILE,
         SEX,
         PERM_REG,
         PERM_PROV,
         PERM_MUNC,
         CURR_REG,
         CURR_PROV,
         CURR_MUNC,
      )

   return(
      list(
         p1 = data %>% filter(CENTRAL_ID == cid1),
         p2 = data %>% filter(CENTRAL_ID == cid2)
      )
   )
}

lapply(
   c('20240104101246Q0400030011'),
   change_px_id,
   'HARP08011816074489',
   'HARP08011816049826'
)

periods <- list(
   c("2024-08-01 00:00:00", "2024-08-02 11:12:13"),
   c("2024-08-02 11:12:13", "2024-08-07 14:54:11"),
   c("2024-08-07 14:54:11", "2024-08-13 15:17:43"),
   c("2024-08-13 15:17:43", "2024-08-18 14:04:05"),
   c("2024-08-18 14:04:05", "2024-08-22 13:51:14"),
   c("2024-08-22 13:51:14", "2024-08-29 13:49:07"),
   c("2024-08-29 13:49:07", "2024-09-04 12:43:17"),
   c("2024-09-04 12:43:17", "2024-09-09 12:32:49"),
   c("2024-09-09 12:32:49", "2024-09-13 11:02:36"),
   c("2024-09-13 11:02:36", "2024-09-19 10:34:00"),

)

for (period in periods) {
   min <- period[1]
   max <- period[2]
   download_pii(min, max)
}

download_pii <- function(min, max) {
   if (missing(max)) {
      max <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   }

   pii <- tibble(REC_ID = NA_character_, BIRTHDATE = NA_Date_) %>%
      slice(0)
   if (file.exists(Sys.getenv("DEDUP_PII")))
      pii <- read_rds(Sys.getenv("DEDUP_PII"))

   lw_conn  <- connect("ohasis-lw")
   new_data <- QB$new(lw_conn)$
      selectRaw("COALESCE(serv.SERVICE_FACI, pii.FACI_ID)         AS FACI_ID")$
      selectRaw("COALESCE(serv.SERVICE_SUB_FACI, pii.SUB_FACI_ID) AS SUB_FACI_ID")$
      select(pii.REC_ID,
             pii.PATIENT_ID,
             pii.FIRST,
             pii.MIDDLE,
             pii.LAST,
             pii.SUFFIX,
             pii.UIC,
             pii.CONFIRMATORY_CODE,
             pii.PATIENT_CODE,
             pii.BIRTHDATE,
             pii.PHILSYS_ID,
             pii.PHILHEALTH_NO,
             pii.CLIENT_EMAIL,
             pii.CLIENT_MOBILE,
             pii.SEX,
             pii.PERM_PSGC_REG,
             pii.PERM_PSGC_PROV,
             pii.PERM_PSGC_MUNC,
             pii.CURR_PSGC_REG,
             pii.CURR_PSGC_PROV,
             pii.CURR_PSGC_MUNC,
             pii.DELETED_AT,
             pii.SNAPSHOT)$
      from("ohasis_lake.px_pii AS pii")$
      leftJoin("ohasis_lake.px_faci_info AS serv", "pii.REC_ID", "=", "serv.REC_ID")$
      whereBetween("pii.SNAPSHOT", c(min, max))$
      get()
   dbDisconnect(lw_conn)

   new_data %<>%
      ohasis$get_addr(
         c(
            PERM_REG  = "PERM_PSGC_REG",
            PERM_PROV = "PERM_PSGC_PROV",
            PERM_MUNC = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            CURR_REG  = "CURR_PSGC_REG",
            CURR_PROV = "CURR_PSGC_PROV",
            CURR_MUNC = "CURR_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      mutate_at(
         .vars = vars(ends_with("_REG"), ends_with("_PROV"), ends_with("_MUNC")),
         ~if_else(. == "UNKNOWN", NA_character_, ., .)
      ) %>%
      mutate_if(
         .predicate = is.character,
         ~clean_pii(.)
      ) %>%
      mutate(
         CLIENT_MOBILE = str_replace_all(CLIENT_MOBILE, "[^[:digit:]]", ""),
         CLIENT_MOBILE = case_when(
            StrLeft(CLIENT_MOBILE, 1) == "9" ~ stri_c("0", CLIENT_MOBILE),
            StrLeft(CLIENT_MOBILE, 2) == "63" ~ str_replace(CLIENT_MOBILE, "^63", "0"),
            TRUE ~ CLIENT_MOBILE
         ),
         BIRTHDATE     = as.character(BIRTHDATE)
      )

   # finalize data
   pii <- pii %>%
      # remove old version of record
      anti_join(select(new_data, REC_ID)) %>%
      # remove old data that were already deleted
      # anti_join(dedup$deleted) %>%
      # append new data
      mutate(BIRTHDATE = as.character(BIRTHDATE)) %>%
      bind_rows(new_data)

   # write to local file for later use
   write_rds(pii, Sys.getenv("DEDUP_PII"))
}