##------------------------------------------------------------------------------
##  Filter Initial Data & Remove Already Reported                             --
##------------------------------------------------------------------------------

nhsss$harp_dx$initial <- tbl(dw_conn, "form_a") %>%
   union_all(tbl(dw_conn, "form_hts")) %>%
   # remove data already reported in previous registry
   anti_join(
      y  = tbl(dw_conn, "harp_dx") %>%
         select(REC_ID),
      by = 'REC_ID'
   ) %>%
   # get latest central ids
   left_join(
      y  = tbl(dw_conn, "id_registry") %>%
         select(CENTRAL_ID, PATIENT_ID),
      by = 'PATIENT_ID'
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   # keep only patients not in registry
   anti_join(
      y  = tbl(dw_conn, "harp_dx") %>%
         left_join(
            y  = tbl(dw_conn, "id_registry") %>%
               select(CENTRAL_ID, PATIENT_ID),
            by = "PATIENT_ID"
         ) %>%
         select(CENTRAL_ID),
      by = 'CENTRAL_ID'
   ) %>%
   # keep only positive results
   # filter(substr(CONFIRM_RESULT, 1, 1) == '1') %>%
   collect()
%>%
mutate(
# month of labcode/date received
lab_month_1           = month(DATE_RECEIVE),
lab_month_2           = .mid(CONFIRM_CODE, stri_locate_first_fixed(CONFIRM_CODE, '-') + 1, 2) %>% as.integer(),
lab_month             = if_else(
condition = is.na(DATE_RECEIVE),
true      = lab_month_2,
false     = lab_month_1
),

# year of labcode/date received
lab_year_1            = year(DATE_RECEIVE),
lab_year_2            = case_when(
.left(CONFIRM_TYPE, 1) == '1' ~ paste0('20', .mid(CONFIRM_CODE, 2, 2)),
.left(CONFIRM_TYPE, 1) == '2' ~ paste0('20', .mid(CONFIRM_CODE, 4, 2)),
TRUE ~ NA_character_
) %>% as.integer(),
lab_year              = if_else(
condition = is.na(DATE_RECEIVE),
true      = lab_year_2,
false     = lab_year_1
),

# date variables
encoded_date          = date(CREATED_AT),
visit_date            = RECORD_DATE,
blood_extract_date    = as.Date(DATE_COLLECT),
specimen_receipt_date = as.Date(DATE_RECEIVE),
confirm_date          = as.Date(DATE_CONFIRM),

# date var for keeping
report_date           = as.Date(paste(sep = '-', lab_year, lab_month, '01')),
yr_rec                = year(RECORD_DATE),

# name
name                  = paste0(
if_else(
condition = is.na(LAST),
true      = '',
false     = LAST
), ', ',
if_else(
condition = is.na(FIRST),
true      = '',
false     = FIRST
), ' ',
if_else(
condition = is.na(MIDDLE),
true      = '',
false     = MIDDLE
), ' ',
if_else(
condition = is.na(SUFFIX),
true      = '',
false     = SUFFIX
)
),
name                  = stri_trim_both(name),

# sort for keep by kuya chard
FACI_ID               = if_else(
condition = !is.na(TEST_FACI),
true      = .left(TEST_FACI, 6),
false     = NA_character_
),
) %>%
left_join(
na_matches = 'never',
y          = ohasis[['facilities']] %>%
filter(SERVICE_ART == 1) %>%
select(FACI_ID, SERVICE_ART),
by         = 'FACI_ID'
) %>%
arrange(lab_year, lab_month, desc(CONFIRM_TYPE), SERVICE_ART) %>%
distinct(CENTRAL_ID, .keep_all = TRUE) %>%
filter(report_date < as.Date(paste(sep = '-', next_yr, next_mo, '01')))%>%
select(-FACI_ID)