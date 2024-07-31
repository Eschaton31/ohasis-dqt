con                <- ohasis$conn("db")
inventory          <- QB$new(con)$from("ohasis_interim.inventory")$get()
inventory_product  <- QB$new(con)$from("ohasis_interim.inventory_product")$get()
inventory_transact <- QB$new(con)$
   from("ohasis_interim.inventory_transact AS trxn")$
   join("ohasis_interim.px_record AS rec", "trxn.TRANSACT_ID", "=", "rec.REC_ID")$
   whereBetween('trxn.TRANSACT_DATE', c("2023-10-01", "2024-03-31"))$
   select("rec.PATIENT_ID", "rec.RECORD_DATE",  "trxn.*")$
   get()
registry           <- QB$new(con)$from("ohasis_interim.registry")$get()
dbDisconnect(con)

###

inv <- inventory %>%
   # filter(ITEM_CURR >= 0) %>%
   # inner_join(y = inventory_transact %>% distinct(INVENTORY_ID)) %>%
   right_join(
      y  = inventory_product %>%
         select(ITEM_ID = ITEM, COMMODITY = NAME, TYPICAL_BATCH, SHORT),
      by = join_by(ITEM_ID)
   ) %>%
   mutate(
      CATEGORY = case_when(
         str_detect(ITEM_ID, "^1") ~ "Diagnostics",
         str_detect(ITEM_ID, "^2") ~ "Medicines",
      ),
      CAT2     = if_else(!is.na(SHORT), 'ARV', NA_character_)
   ) %>%
   ohasis$get_faci(
      list(FACILITY = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("REGION", "PROVINCE", "MUNCITY")
   )

transactions <- inventory_transact %>%
   inner_join(
      y  = inv %>%
         select(INVENTORY_ID, CATEGORY, CAT2, COMMODITY, REGION, FACILITY, TYPICAL_BATCH),
      by = join_by(INVENTORY_ID)
   ) %>%
   mutate(
      TRANSACT_TOTAL = case_when(
         UNIT_BASIS == 1 ~ TRANSACT_QUANTITY * as.numeric(TYPICAL_BATCH),
         TRUE ~ TRANSACT_QUANTITY
      )
   )

current_stocks <- inv %>%
   group_by(CATEGORY, CAT2, COMMODITY, REGION, FACILITY) %>%
   summarise(
      CURRENT_STATUS = sum(ITEM_CURR, na.rm = TRUE)
   ) %>%
   ungroup()


expiry <- inv %>%
   mutate(
      EXPIRE_DATE = format(EXPIRE_DATE, "%Y-%m")
   ) %>%
   group_by(CATEGORY, CAT2, COMMODITY, REGION, FACILITY, EXPIRE_DATE) %>%
   summarise(
      STOCK_ON_HAND = sum(ITEM_CURR, na.rm = TRUE)
   ) %>%
   ungroup() %>%
   arrange(EXPIRE_DATE) %>%
   pivot_wider(
      id_cols     = c(CATEGORY, COMMODITY, CAT2, REGION, FACILITY),
      names_from  = EXPIRE_DATE,
      values_from = STOCK_ON_HAND
   )

disp_mo <- transactions %>%
   mutate(
      TRANSACT_DATE = format(TRANSACT_DATE, "%Y-%m")
   ) %>%
   group_by(CATEGORY, CAT2, COMMODITY, REGION, FACILITY, TRANSACT_DATE) %>%
   summarise(
      TRANSACT_TOTAL = sum(TRANSACT_TOTAL, na.rm = TRUE)
   ) %>%
   ungroup() %>%
   arrange(TRANSACT_DATE) %>%
   pivot_wider(
      id_cols     = c(CATEGORY, CAT2, COMMODITY, REGION, FACILITY),
      names_from  = TRANSACT_DATE,
      values_from = TRANSACT_TOTAL
   )

period <- transactions %>%
   group_by(CATEGORY, CAT2, COMMODITY, REGION, FACILITY) %>%
   summarise(
      DISPENSED_PERIOD = sum(TRANSACT_QUANTITY, na.rm = TRUE)
   ) %>%
   ungroup()

clients <- transactions %>%
   get_cid(registry, PATIENT_ID) %>%
   distinct(CATEGORY, COMMODITY, CAT2, REGION, FACILITY, CENTRAL_ID) %>%
   group_by(CATEGORY, COMMODITY, CAT2, REGION, FACILITY) %>%
   summarise(
      UNIQUE_CLIENTS = n()
   ) %>%
   ungroup()

status <- current_stocks %>%
   left_join(expiry) %>%
   left_join(period) %>%
   left_join(clients) %>%
   mutate(
      CAT2 = case_when(
         str_detect(COMMODITY, "Bioline") ~ "T0/T1",
         str_detect(COMMODITY, "Determine") ~ "T2",
         str_detect(COMMODITY, "STAT-PAK") ~ "T3",
         str_detect(COMMODITY, "Geenius") ~ "T3",
         TRUE ~ CAT2
      )
   ) %>%
   relocate(UNIQUE_CLIENTS, .after = CURRENT_STATUS)

status %>% write_clip()

status <- current_stocks %>%
   filter(CATEGORY == "Diagnostics") %>%
   left_join(disp_mo) %>%
   mutate(
      CAT2 = case_when(
         str_detect(COMMODITY, "Bioline") ~ "T0/T1",
         str_detect(COMMODITY, "Determine") ~ "T2",
         str_detect(COMMODITY, "STAT-PAK") ~ "T3",
         str_detect(COMMODITY, "Geenius") ~ "T3",
      )
   ) %>%
   relocate(CAT2, .after = CATEGORY)

write_sheet(status, "19MRpTfgAeNQ14EVeG23MWu3wuLM6lcE_wA5YwmYSbKA", "stock_status")

### COnfirmatory

con     <- ohasis$conn("lw")
pending <- QB$new(con)$
   from("ohasis_lake.px_hiv_testing AS test")$
   join("ohasis_lake.px_pii AS pii", "test.REC_ID", "=", "pii.REC_ID")$
   leftJoin("ohasis_warehouse.id_registry AS reg", "pii.PATIENT_ID", "=", "reg.PATIENT_ID")$
   where("CONFIRM_RESULT", "regexp", "Pending")$
   select(pii.FACI_ID,
          pii.SUB_FACI_ID,
          pii.RECORD_DATE,
          pii.DISEASE,
          pii.MODULE,
          pii.PRIME,
          pii.CONFIRMATORY_CODE,
          pii.UIC,
          pii.PHILHEALTH_NO,
          pii.SEX,
          pii.BIRTHDATE,
          pii.PATIENT_CODE,
          pii.PHILSYS_ID,
          pii.FIRST,
          pii.MIDDLE,
          pii.LAST,
          pii.SUFFIX,
          test.CONFIRM_FACI,
          test.CONFIRM_SUB_FACI,
          test.CONFIRM_TYPE,
          test.CONFIRM_CODE,
          test.SPECIMEN_REFER_TYPE,
          test.SPECIMEN_SOURCE,
          test.SPECIMEN_SUB_SOURCE,
          test.CONFIRM_RESULT,
          test.CONFIRM_REMARKS,
          test.SIGNATORY_1,
          test.SIGNATORY_2,
          test.SIGNATORY_3,
          test.DATE_RELEASE,
          test.DATE_CONFIRM,
          test.IDNUM,
          test.T0_DATE,
          test.T0_RESULT,
          test.T1_DATE,
          test.T1_KIT,
          test.T1_RESULT,
          test.T2_DATE,
          test.T2_KIT,
          test.T2_RESULT,
          test.T3_DATE,
          test.T3_KIT,
          test.T3_RESULT)$
   selectRaw("COALESCE(reg.CENTRAL_ID, pii.PATIENT_ID) AS CENTRAL_ID")$
   get()
pos     <- QB$new(con)$
   from("harp_dx.reg_202312 AS dx")$
   leftJoin("ohasis_warehouse.id_registry AS reg", "dx.PATIENT_ID", "=", "reg.PATIENT_ID")$
   select(dx.PATIENT_ID,
          dx.labcode2,
          dx.confirm_date,
          dx.firstname,
          dx.middle,
          dx.last,
          dx.name_suffix,
          dx.bdate,
          dx.sex,
          dx.dx_region,
          dx.dx_province,
          dx.dx_muncity,
          dx.dxlab_standard)$
   selectRaw("COALESCE(reg.CENTRAL_ID, dx.PATIENT_ID) AS CENTRAL_ID")$
   get()
dbDisconnect(con)


pending_pos <- pending %>%
   inner_join(
      y  = pos %>%
         select(CENTRAL_ID,
                labcode2,
                confirm_date,
                dx_region,
                dx_province,
                dx_muncity,
                dxlab_standard),
      by = join_by(CENTRAL_ID)
   )