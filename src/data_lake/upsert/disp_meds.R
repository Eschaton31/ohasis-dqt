##  Grouped Medicine Data ------------------------------------------------------

continue <- 0
id_col   <- c("REC_ID", "REC_ID_GRP", "MEDICINE")
object   <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
   filter(
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
         (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
   ) %>%
   select(REC_ID)

for_delete <- object %>%
   collect()

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      collect() %>%
      inner_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_medicine")),
               by = "REC_ID"
            ) %>%
            left_join(
               y          = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "inventory_product")) %>%
                  select(
                     MEDICINE = ITEM,
                     TYPICAL_BATCH
                  ),
               by         = "MEDICINE",
               na_matches = "never"
            ) %>%
            collect() %>%
            mutate(
               MEDICINE_LEFT       = if_else(is.na(MEDICINE_LEFT), 0, MEDICINE_LEFT),
               DISP_TOTAL_STANDARD = if_else(
                  UNIT_BASIS == 1 &
                     !is.na(DISP_TOTAL) &
                     !is.na(TYPICAL_BATCH),
                  DISP_TOTAL * as.numeric(TYPICAL_BATCH),
                  DISP_TOTAL,
                  DISP_TOTAL
               ),
               SNAPSHOT            = case_when(
                  !is.na(DELETED_AT) ~ DELETED_AT,
                  !is.na(UPDATED_AT) ~ UPDATED_AT,
                  TRUE ~ CREATED_AT
               ),
            ) %>%
            dtplyr::lazy_dt() %>%
            group_by(REC_ID, MEDICINE, DISP_DATE) %>%
            summarise(
               FACI_ID         = FACI_ID,
               SUB_FACI_ID     = SUB_FACI_ID,
               DISP_NUM        = DISP_NUM,
               BATCH_NUM       = paste(collapse = "; ", BATCH_NUM),
               UNIT_BASIS      = 2,
               PER_DAY         = PER_DAY,
               DISP_TOTAL      = sum(DISP_TOTAL_STANDARD, na.rm = TRUE),
               MEDICINE_LEFT   = sum(MEDICINE_LEFT, na.rm = TRUE),
               MEDICINE_MISSED = sum(MEDICINE_MISSED, na.rm = TRUE),
               NEXT_DATE       = NEXT_DATE,
               DISP_BY         = DISP_BY,
               PRIME           = PRIME,
               CREATED_BY      = CREATED_BY,
               CREATED_AT      = max(CREATED_AT, na.rm = TRUE),
               UPDATED_BY      = UPDATED_BY,
               UPDATED_AT      = UPDATED_AT,
               DELETED_BY      = DELETED_BY,
               DELETED_AT      = DELETED_AT,
               SNAPSHOT        = max(SNAPSHOT, na.rm = TRUE)
            ) %>%
            ungroup() %>%
            distinct(REC_ID, MEDICINE, DISP_DATE, .keep_all = TRUE) %>%
            as_tibble() %>%
            mutate(
               TOTAL_PILLS = coalesce(DISP_TOTAL, 0) * coalesce(MEDICINE_LEFT, 0),
               TOTAL_DAYS  = coalesce(TOTAL_PILLS / coalesce(PER_DAY, 0), 0),
               NEW_NEXT    = DISP_DATE %m+% days(if_else(is.infinite(TOTAL_DAYS), 0, TOTAL_DAYS)),
               NEXT_DATE   = if_else(
                  !is.na(UNIT_BASIS) &
                     !is.na(DISP_TOTAL) &
                     !is.na(DISP_DATE) &
                     !is.na(PER_DAY) &
                     !is.na(NEW_NEXT),
                  NEW_NEXT,
                  NEXT_DATE,
                  NEXT_DATE
               ),
               DISP_YR     = year(DISP_DATE),
               DISP_MO     = month(DISP_DATE),
            ) %>%
            select(-NEW_NEXT, -TOTAL_PILLS, -TOTAL_DAYS) %>%
            dtplyr::lazy_dt() %>%
            group_by(REC_ID) %>%
            mutate(REC_ID_GRP = paste0(REC_ID, " - ", cur_group_id())) %>%
            ungroup() %>%
            group_by(REC_ID, DISP_YR, DISP_MO) %>%
            mutate(REC_ID_GRP = paste0(REC_ID_GRP, " - ", cur_group_id())) %>%
            ungroup() %>%
            as_tibble(),
         by = "REC_ID"
      )
}