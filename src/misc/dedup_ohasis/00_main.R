##  OHASIS Deduplication Controller -------------------------------------------------

source("src/misc/dedup_ohasis/01_pii.R")
source("src/misc/dedup_ohasis/02_dedup_fns.R")

ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE, to = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
dedup <- dedup_download()
dedup <- dedup_linelist(dedup)

check_dupes <- ohasis_dupes(FIRST, MIDDLE, LAST, UIC)
check_dupes <- ohasis_dupes(FIRST, LAST, UIC)
check_dupes <- ohasis_dupes(FIRST, MIDDLE, LAST, UIC_SORT)
check_dupes <- ohasis_dupes(FIRST, LAST, UIC_SORT)
check_dupes <- ohasis_dupes(FIRST_NY, MIDDLE_NY, LAST_NY, UIC)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, UIC)
check_dupes <- ohasis_dupes(FIRST, MIDDLE, LAST, BIRTHDATE, UIC_ORDER)
check_dupes <- ohasis_dupes(FIRST, LAST, BIRTHDATE, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(FIRST, LAST, BIRTHDATE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST, LAST, BIRTHDATE, PHIC)
check_dupes <- ohasis_dupes(FIRST, LAST, BIRTHDATE, PHILSYS)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, CONFIRM_SIEVE)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PXCODE_SIEVE)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PHIC)
check_dupes <- ohasis_dupes(FIRST_NY, LAST_NY, BIRTHDATE, PHILSYS)
check_dupes <- ohasis_dupes(FIRST, MIDDLE, LAST_A, UIC)
check_dupes <- ohasis_dupes(FIRST, LAST_A, UIC)
check_dupes <- ohasis_dupes(FIRST_A, MIDDLE, LAST, UIC)
check_dupes <- ohasis_dupes(FIRST_A, LAST, UIC)
check_dupes <- ohasis_dupes(FIRST_A, MIDDLE, LAST_A, UIC)

check_dupes <- ohasis_dupes(FIRST, MIDDLE_A, LAST, BIRTHDATE)
# upload
upload_dupes(check_dupes$registry_up)
upload_dupes(check_dupes$normal_up)

check_dupes$registry %>% View("reg")
check_dupes$normal %>% View("norm")