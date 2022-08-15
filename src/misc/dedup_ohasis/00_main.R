##  HARP Tx Linkage Controller -------------------------------------------------

# define datasets
if (!exists("dedup"))
   dedup <- new.env()


dedup$dx <- ohasis$get_data("harp_dx", ohasis$prev_yr, ohasis$prev_mo) %>%
   read_dta() %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   select(-starts_with("CENTRAL_ID")) %>%
   left_join(
      y  = dedup$id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )

ohasis$db_checks <- ohasis$check_consistency()
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)
source("src/misc/dedup_ohasis/01_pii.R")


check_dupes <- ohasis_dupes(c(FIRST, LAST, UIC))

# upload
upload_dupes(check_dupes$registry_up)
upload_dupes(check_dupes$normal_up)

check_dupes$registry
check_dupes$normal_up %>%
   get_dupes(FINAL_CID)
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