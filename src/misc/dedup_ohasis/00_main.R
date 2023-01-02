##  OHASIS Deduplication Controller -------------------------------------------------

source("src/misc/dedup_ohasis/01_pii.R")
source("src/misc/dedup_ohasis/02_dedup_fns.R")

dedup <- dedup_download()
dedup <- dedup_linelist(dedup)

check_dupes <- ohasis_dupes(c(FIRST, MIDDLE, LAST, UIC))
check_dupes <- ohasis_dupes(c(FIRST, LAST, UIC))
check_dupes <- ohasis_dupes(c(FIRST, MIDDLE, LAST, UIC_SORT))
check_dupes <- ohasis_dupes(c(FIRST, LAST, UIC_SORT))
check_dupes <- ohasis_dupes(c(FIRST_NY, MIDDLE_NY, LAST_NY, UIC))
check_dupes <- ohasis_dupes(c(FIRST_NY, LAST_NY, UIC))
check_dupes <- ohasis_dupes(c(FIRST, MIDDLE, LAST, BIRTHDATE, UIC_ORDER))
check_dupes <- ohasis_dupes(c(FIRST, LAST, BIRTHDATE, CONFIRMATORY_CODE))
check_dupes <- ohasis_dupes(c(FIRST, LAST, BIRTHDATE, PATIENT_CODE))
check_dupes <- ohasis_dupes(c(FIRST, LAST, BIRTHDATE, PHIC))
check_dupes <- ohasis_dupes(c(FIRST, LAST, BIRTHDATE, PHILSYS))
check_dupes <- ohasis_dupes(c(FIRST_NY, LAST_NY, BIRTHDATE, CONFIRMATORY_CODE))
check_dupes <- ohasis_dupes(c(FIRST_NY, LAST_NY, BIRTHDATE, PATIENT_CODE))
check_dupes <- ohasis_dupes(c(FIRST_NY, LAST_NY, BIRTHDATE, PHIC))
check_dupes <- ohasis_dupes(c(FIRST_NY, LAST_NY, BIRTHDATE, PHILSYS))
check_dupes <- ohasis_dupes(c(FIRST, MIDDLE, LAST_A, UIC))
check_dupes <- ohasis_dupes(c(FIRST, LAST_A, UIC))
check_dupes <- ohasis_dupes(c(FIRST_A, MIDDLE, LAST, UIC))
check_dupes <- ohasis_dupes(c(FIRST_A, LAST, UIC))
check_dupes <- ohasis_dupes(c(FIRST_A, MIDDLE, LAST_A, UIC))

# upload
upload_dupes(check_dupes$registry_up)
upload_dupes(check_dupes$normal_up)

check_dupes$registry %>% View("reg")
check_dupes$normal %>% View("norm")
# dbxUpsert(
#    dbConnect(
#       RMariaDB::MariaDB(),
#       user     = 'ohasis',
#       password = 't1rh0uGCyN2sz6zk',
#       host     = '192.168.193.232',
#       port     = '3307',
#       timeout  = -1,
#       'ohasis_interim'
#    ),
#    "registry",
#    ohasis$db_checks$cid_no_pid %>%
#       select(
#          CENTRAL_ID
#       ) %>%
#       mutate(
#          PATIENT_ID = CENTRAL_ID,
#          CREATED_BY = "1300000001",
#          CREATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
#       ),
#    c("PATIENT_ID", "CENTRAL_ID")
# )

hx_m <- dedup$pii %>%
   filter(SEX == "1_Male") %>%
   distinct(CENTRAL_ID)
hx_f <- dedup$pii %>%
   filter(SEX == "2_Female") %>%
   distinct(CENTRAL_ID)

hx_m %>%
   inner_join(hx_f) %>%
   left_join(dedup$linelist) %>%
   write_sheet(as_id("1OPVqAkK_mJXYVeCfwgmr2vZrOHabSdFTHWDQ0LULRA8"), "male and female")