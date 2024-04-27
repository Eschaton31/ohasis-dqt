##  OHASIS Deduplication Controller -------------------------------------------------

source("src/misc/dedup_ohasis/01_pii.R")
source("src/misc/dedup_ohasis/02_dedup_fns.R")

ohasis$data_factory("lake", "px_pii", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
dedup <- dedup_download()
dedup <- dedup_linelist(dedup)

check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE, UIC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, UIC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE, UIC_SORT)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, UIC_SORT)
check_dupes <- ohasis_dupes(FIRST_NY, MIDDLE_NY, LAST_NY, UIC)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, UIC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE, BIRTHDATE, UIC_ORDER)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, PHIC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, BIRTHDATE, PHILSYS)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PHIC)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PHILSYS)
check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, PHIC)
check_dupes <- ohasis_dupes(FIRST_A, LAST_A, BIRTHDATE, PHILSYS)
check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_A, UIC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_A, UIC)
check_dupes <- ohasis_dupes(FIRST_A, MIDDLE_SIEVE, LAST_SIEVE, UIC)
check_dupes <- ohasis_dupes(FIRST_A, LAST_SIEVE, UIC)
check_dupes <- ohasis_dupes(FIRST_A, MIDDLE_SIEVE, LAST_A, UIC)

check_dupes <- ohasis_dupes(FIRST_SIEVE, MIDDLE_SIEVE, LAST_SIEVE, UIC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_SIEVE, UIC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTHDATE, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_NY, UIC, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTHDATE, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTHDATE, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_NY, UIC, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTHDATE, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, PERM_PROV)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_NY, BIRTHDATE, PERM_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, UIC, CURR_PROV)
check_dupes <- ohasis_dupes(FIRST_SIEVE, LAST_NY, BIRTHDATE, CURR_MUNC)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTHDATE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_YR, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_MO, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_DY, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_YR, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_MO, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_DY, CLIENT_MOBILE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_YR, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_MO, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_SIEVE, BIRTH_DY, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_YR, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_MO, CLIENT_EMAIL)
check_dupes <- ohasis_dupes(FIRST_NY, BIRTH_DY, CLIENT_EMAIL)

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
check_dupes <- ohasis_dupes(FIRST_NY, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(FIRST_SIEVE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST_NY, PXCODE_SIEVE, BIRTH_YR, BIRTH_MO)
check_dupes <- ohasis_dupes(LAST_SIEVE, CONFIRM_SIEVE, BIRTH_YR)
check_dupes <- ohasis_dupes(LAST_SIEVE, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(LAST_NY, CONFIRM_SIEVE, BIRTH_YR)
check_dupes <- ohasis_dupes(LAST_NY, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(UIC, CLIENT_MOBILE)

check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_MOBILE, BIRTH_YR)
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_MOBILE, BIRTH_YR)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_MOBILE, BIRTH_YR)
check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_MOBILE, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_MOBILE, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_MOBILE, BIRTH_MO)
check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_MOBILE, BIRTH_DY)
check_dupes <- ohasis_dupes(FIRST_NY, CLIENT_MOBILE, BIRTH_DY)
check_dupes <- ohasis_dupes(FIRST_A, CLIENT_MOBILE, BIRTH_DY)

check_dupes <- ohasis_dupes(FIRST_SIEVE, CLIENT_EMAIL, BIRTH_YR)
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

from              <- "2024-04-27 15:55:00"
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
   select(-PATIENT_ID) %>%
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