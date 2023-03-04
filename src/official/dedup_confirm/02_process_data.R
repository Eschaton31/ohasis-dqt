for_match <- dedup_confirm$data$for_dedup %>%
   get_cid(dedup_confirm$data$id_reg, PATIENT_ID) %>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, UIC, PHILHEALTH_NO),
      ~str_squish(toupper(.))
   ) %>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, UIC, PHILHEALTH_NO),
      ~case_when(
         . == "--" ~ NA_character_,
         . == "N/A" ~ NA_character_,
         . == "NULL" ~ NA_character_,
         TRUE ~ .
      )
   ) %>%
   mutate(
      SEX  = case_when(
         SEX == 1 ~ "MALE",
         SEX == 2 ~ "FEMALE",
         TRUE ~ NA_character_
      ),
      name = stri_c(
         sep = ", ",
         str_squish(stri_c(sep = " ", coalesce(LAST, ""), coalesce(SUFFIX, ""))),
         str_squish(stri_c(sep = " ", coalesce(FIRST, ""), coalesce(MIDDLE, "")))
      )
   ) %>%
   rename(
      sex   = SEX,
      bdate = BIRTHDATE
   ) %>%
   ohasis$get_faci(
      list(SPECIMEN_SOURCE_FACI = c("SOURCE", "SUB_SOURCE")),
      "name"
   ) %>%
   ohasis$get_faci(
      list(CONFIRM_LAB = c("FACI_ID", "SUB_FACI_ID")),
      "name"
   ) %>%
   left_join(
      y  = dedup_confirm$harp$dx %>%
         select(
            CENTRAL_ID,
            old_dx_labcode = labcode2,
            old_dx_lab     = DX_LAB,
            old_dx_conf    = confirm_date
         ) %>%
         mutate(old_dx = 1),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      old_dx = coalesce(old_dx, 0)
   )

match_ref <- dedup_confirm$harp$dx %>%
   mutate_at(
      .vars = vars(firstname, middle, last, name_suffix, uic, philhealth),
      ~str_squish(toupper(.))
   ) %>%
   mutate_at(
      .vars = vars(firstname, middle, last, name_suffix, uic, philhealth),
      ~case_when(
         . == "--" ~ NA_character_,
         . == "N/A" ~ NA_character_,
         . == "NULL" ~ NA_character_,
         TRUE ~ .
      )
   ) %>%
   mutate(
      name = stri_c(
         sep = ", ",
         str_squish(stri_c(sep = " ", coalesce(last, ""), coalesce(name_suffix, ""))),
         str_squish(stri_c(sep = " ", coalesce(firstname, ""), coalesce(middle, "")))
      )
   )


rc_matches <- quick_reclink(
   for_match %>% filter(old_dx == 0),
   match_ref,
   "REC_ID",
   "idnum",
   c("name", "bdate"),
   "name"
)
rc_matches %<>%
   filter(AVG_DIST >= 0.9)

for_upload            <- list()
for_upload$registry   <- dedup_confirm$data$for_dedup %>%
   inner_join(
      y  = select(rc_matches, REC_ID, idnum),
      by = join_by(REC_ID)
   ) %>%
   inner_join(
      y  = select(dedup_confirm$harp$dx, CENTRAL_ID, idnum),
      by = join_by(idnum)
   ) %>%
   select(
      CENTRAL_ID,
      PATIENT_ID
   )
for_upload$px_confirm <- for_match %>%
   left_join(
      y  = rc_matches %>%
         select(REC_ID, idnum) %>%
         mutate(new_match = 1),
      by = join_by(REC_ID)
   ) %>%
   left_join(
      y  = dedup_confirm$harp$dx %>%
         select(
            CENTRAL_ID,
            idnum,
            new_match_labcode = labcode2,
            new_match_lab     = DX_LAB,
            new_match_conf    = confirm_date
         ),
      by = join_by(idnum)
   ) %>%
   mutate(
      new_match      = coalesce(new_match, 0),
      labcode2       = coalesce(old_dx_labcode, new_match_labcode),
      dxlab_standard = coalesce(old_dx_lab, new_match_lab),
      new_match_conf = coalesce(old_dx_conf, new_match_conf),
      FINAL_RESULT   = case_when(
         old_dx == 1 ~ "Duplicate",
         new_match == 1 ~ "Duplicate",
         TRUE ~ FINAL_RESULT
      ),
      REMARKS        = case_when(
         old_dx == 1 ~ stri_c("Client was previously confirmed on ", format(new_match_conf, " %b %d, %Y"), " at ", dxlab_standard, " (", labcode2, ")."),
         new_match == 1 ~ stri_c("Client was previously confirmed on ", format(new_match_conf, " %b %d, %Y"), " at ", dxlab_standard, " (", labcode2, ")."),
         REMARKS == "rHIVda Test No. 2 not completed." ~ "No previous result found. Proceed to rHIVda Test No. 2.",
         REMARKS == "rHIVda Test No. 3 not completed." ~ "No previous result found. Proceed to rHIVda Test No. 3.",
         TRUE ~ REMARKS
      ),
   ) %>%
   select(
      REC_ID,
      CONFIRM_CODE,
      FINAL_RESULT,
      REMARKS
   )
