tx        <- read_dta(hs_data("harp_tx", "outcome", 2023, 8))
dx        <- read_dta(hs_data("harp_full", "reg", 2023, 8))
faci_type <- read_sheet("1aOqYjx5wbc403xy-64YHJU6NzhEBRUu6Ldg59yDEUMw", "Sheet1", range = "A:D", col_types = "c") %>%
   mutate(
      FINAL_FACI_TYPE = case_when(
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Public" ~ "Public hospital",
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Private" ~ "Private hospital",
         TRUE ~ FINAL_FACI_TYPE
      )
   )

dx %<>%
   mutate(
      dxlab_standard = case_when(
         idnum %in% c(166980, 166981, 166982, 166983) ~ "MANDAUE SHC",
         TRUE ~ dxlab_standard
      ),
   ) %>%
   dxlab_to_id(
      c("dx_faci_id", "dx_sub_faci_id"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
      ohasis$ref_faci
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(tx_faci_id = "realhub", tx_sub_faci_id = "realhub_branch")
   ) %>%
   left_join(
      y          = faci_type %>%
         select(
            dx_faci_id   = HARP_FACI,
            dx_pubpriv   = FINAL_PUBPRIV,
            dx_faci_type = FINAL_FACI_TYPE
         ) %>%
         distinct(dx_faci_id, .keep_all = TRUE),
      by         = join_by(dx_faci_id),
      na_matches = "never"
   ) %>%
   left_join(
      y  = faci_type %>%
         select(
            tx_faci_id   = HARP_FACI,
            tx_pubpriv   = FINAL_PUBPRIV,
            tx_faci_type = FINAL_FACI_TYPE
         ) %>%
         distinct(tx_faci_id, .keep_all = TRUE),
      by = join_by(tx_faci_id)
   ) %>%
   mutate(
      reactive_date = coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date(),
   )

tx %<>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(tx_faci_id = "realhub", tx_sub_faci_id = "realhub_branch")
   ) %>%
   left_join(
      y  = faci_type %>%
         select(
            tx_faci_id   = HARP_FACI,
            tx_pubpriv   = FINAL_PUBPRIV,
            tx_faci_type = FINAL_FACI_TYPE
         ) %>%
         distinct(tx_faci_id, .keep_all = TRUE),
      by = join_by(tx_faci_id)
   )

tx %>%
   filter(is.na(tx_faci_type)) %>%
   distinct(realhub) %>%
   write_clip()

write_dta(tx, "H:/20231010_onart-vl_2023-08_mod-faci_type.dta")
write_dta(dx, "H:/20231012_harp_2023-08_wVL-reactive_date-faci_type_v2.dta")