min <- "2023-01-01"
max <- "2024-07-01"

con               <- ohasis$conn("db")
inventory_product <- QB$new(con)$from("ohasis_interim.inventory_product")$get()
dbDisconnect(con)


write_flat_file(list(arv = inventory_product %>%
   filter(!is.na(TYPICAL_BATCH), CATEGORY == '2000') %>%
   select(
      ARV               = NAME,
      `Pills in bottle` = TYPICAL_BATCH,
   ) %>%
   mutate(
      `Typical pills per day` = as.numeric(`Pills in bottle`) / 30
   )), "H:/20240515_arv-bottles.xlsx")

con       <- ohasis$conn("lw")
disp_meds <- QB$new(con)$
   from("ohasis_lake.disp_meds")$
   join("ohasis_lake.px_pii AS rec", "disp_meds.REC_ID", "=", "rec.REC_ID")$
   select("rec.PATIENT_ID",
          "rec.FACI_ID AS RECORD_FACI",
          "rec.SUB_FACI_ID AS RECORD_SUB_FACI",
          "disp_meds.*")$
   whereBetween("disp_meds.DISP_DATE", c(min, max))$
   get()
registry  <- QB$new(con)$from("ohasis_interim.registry")$get()
dbDisconnect(con)

tx <- read_dta(hs_data("harp_tx", "reg", 2024, 6))


dispensing <- disp_meds %>%
   get_cid(registry, PATIENT_ID) %>%
   left_join(
      y  = inventory_product %>%
         select(MEDICINE = ITEM, COMMODITY = NAME, TYPICAL_BATCH),
      by = join_by(MEDICINE)
   ) %>%
   mutate(
      FACI_ID   = coalesce(FACI_ID, RECORD_FACI),
      DISP_DATE = as.Date(DISP_DATE),
      NEXT_DATE = as.Date(NEXT_DATE),
      BOTTLES   = DISP_TOTAL / as.numeric(TYPICAL_BATCH)
   ) %>%
   ohasis$get_faci(
      list(FACILITY = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("REGION", "PROVINCE", "MUNCITY")
   ) %>%
   filter(
      !is.na(COMMODITY),
      DISP_DATE >= min,
      DISP_DATE < max
   )


tx_2023 <- tx %>%
   filter(artstart_date >= "2023-01-01") %>%
   get_cid(registry, PATIENT_ID) %>%
   left_join(
      y  = dispensing %>%
         select(REC_ID, REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY),
      by = join_by(REC_ID)
   ) %>%
   mutate(
      NEW_2023Q1 = if_else(artstart_date >= "2023-01-01" & artstart_date <= "2023-03-31", 1, 0, 0),
      NEW_2023Q2 = if_else(artstart_date >= "2023-04-01" & artstart_date <= "2023-06-30", 1, 0, 0),
      NEW_2023Q3 = if_else(artstart_date >= "2023-07-01" & artstart_date <= "2023-09-30", 1, 0, 0),
      NEW_2023Q4 = if_else(artstart_date >= "2023-10-01" & artstart_date <= "2023-12-31", 1, 0, 0),
      NEW_2024Q1 = if_else(artstart_date >= "2024-01-01" & artstart_date <= "2024-03-31", 1, 0, 0),
      NEW_2024Q2 = if_else(artstart_date >= "2024-04-01" & artstart_date <= "2024-06-30", 1, 0, 0),
   ) %>%
   distinct(art_id, REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, .keep_all = TRUE) %>%
   group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
   summarise_at(
      .vars = vars(NEW_2023Q1, NEW_2023Q2, NEW_2023Q3, NEW_2023Q4, NEW_2024Q1, NEW_2024Q2),
      ~sum(.)
   ) %>%
   ungroup()

disp_bottles <- dispensing %>%
   group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
   summarise(
      `DISP_202301-202403` = sum(BOTTLES, na.rm = TRUE),
      DISP_2023Q1          = sum(if_else(DISP_DATE >= "2023-01-01" & DISP_DATE <= "2023-03-31", BOTTLES, 0, 0), na.rm = TRUE),
      DISP_2023Q2          = sum(if_else(DISP_DATE >= "2023-04-01" & DISP_DATE <= "2023-06-30", BOTTLES, 0, 0), na.rm = TRUE),
      DISP_2023Q3          = sum(if_else(DISP_DATE >= "2023-07-01" & DISP_DATE <= "2023-09-30", BOTTLES, 0, 0), na.rm = TRUE),
      DISP_2023Q4          = sum(if_else(DISP_DATE >= "2023-10-01" & DISP_DATE <= "2023-12-31", BOTTLES, 0, 0), na.rm = TRUE),
      DISP_2024Q1          = sum(if_else(DISP_DATE >= "2024-01-01" & DISP_DATE <= "2024-03-31", BOTTLES, 0, 0), na.rm = TRUE),
      DISP_2024Q2          = sum(if_else(DISP_DATE >= "2024-04-01" & DISP_DATE <= "2024-06-30", BOTTLES, 0, 0), na.rm = TRUE),
   ) %>%
   ungroup() %>%
   left_join(
      y  = dispensing %>%
         distinct(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, CENTRAL_ID) %>%
         group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
         summarise(
            `CLIENTS_202301-202403` = n()
         ) %>%
         ungroup(),
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   ) %>%
   left_join(
      y  = dispensing %>%
         filter(DISP_DATE >= "2023-01-01", DISP_DATE <= "2023-03-31") %>%
         distinct(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, CENTRAL_ID) %>%
         group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
         summarise(
            CLIENTS_2023Q1 = n()
         ) %>%
         ungroup(),
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   ) %>%
   left_join(
      y  = dispensing %>%
         filter(DISP_DATE >= "2023-04-01", DISP_DATE <= "2023-06-30") %>%
         distinct(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, CENTRAL_ID) %>%
         group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
         summarise(
            CLIENTS_2023Q2 = n()
         ) %>%
         ungroup(),
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   ) %>%
   left_join(
      y  = dispensing %>%
         filter(DISP_DATE >= "2023-07-01", DISP_DATE <= "2023-09-30") %>%
         distinct(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, CENTRAL_ID) %>%
         group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
         summarise(
            CLIENTS_2023Q3 = n()
         ) %>%
         ungroup(),
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   ) %>%
   left_join(
      y  = dispensing %>%
         filter(DISP_DATE >= "2023-10-01", DISP_DATE <= "2023-12-31") %>%
         distinct(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, CENTRAL_ID) %>%
         group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
         summarise(
            CLIENTS_2023Q4 = n()
         ) %>%
         ungroup(),
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   ) %>%
   left_join(
      y  = dispensing %>%
         filter(DISP_DATE >= "2024-01-01", DISP_DATE <= "2024-03-31") %>%
         distinct(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, CENTRAL_ID) %>%
         group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
         summarise(
            CLIENTS_2024Q1 = n()
         ) %>%
         ungroup(),
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   ) %>%
   left_join(
      y  = dispensing %>%
         filter(DISP_DATE >= "2024-04-01", DISP_DATE <= "2024-06-30") %>%
         distinct(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY, CENTRAL_ID) %>%
         group_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY) %>%
         summarise(
            CLIENTS_2024Q2 = n()
         ) %>%
         ungroup(),
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   ) %>%
   left_join(
      y  = tx_2023,
      by = join_by(REGION, PROVINCE, MUNCITY, FACILITY, COMMODITY)
   )

write_sheet(disp_bottles, "19MRpTfgAeNQ14EVeG23MWu3wuLM6lcE_wA5YwmYSbKA", "disp_bottles (jun 2024)")