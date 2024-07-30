con <- ohasis$conn("db")
tph <- QB$new(con)
tph$selectRaw("COALESCE(id.CENTRAL_ID, rec.PATIENT_ID) AS CENTRAL_ID")
tph$from("ohasis_interim.px_record AS rec")
tph$leftJoin("ohasis_interim.registry AS id", "rec.PATIENT_ID", "=", "id.PATIENT_ID")
tph$distinct()
tph$where("FACI_ID", "040013")
tph$where("MODULE", "3")
cohort <- tph$get()
id_reg <- QB$new(con)$from("ohasis_interim.registry")$select("CENTRAL_ID", "PATIENT_ID")$get()
dbDisconnect(con)

dx    <- read_dta(hs_data("harp_full", "reg", 2024, 5))
txreg <- read_dta(hs_data("harp_tx", "reg", 2024, 5))
txout <- read_dta(hs_data("harp_tx", "outcome", 2024, 5))

export <- cohort %>%
   inner_join(
      y  = txreg %>%
         select(-CENTRAL_ID) %>%
         get_cid(id_reg, PATIENT_ID) %>%
         mutate(
            dead = if_else(!is.na(mort_id), "Yes", NA_character_)
         ) %>%
         select(
            CENTRAL_ID,
            art_id,
            idnum,
            confirmatory_code,
            uic,
            px_code,
            first,
            middle,
            last,
            suffix,
            dead,
            sex,
            philhealth_no,
            birthdate,
            artstart_date,
            baseline_cd4_date,
            baseline_cd4_result
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = txout %>%
         mutate(
            lte          = if_else(str_detect(regimen, "TDF/3TC/EFV"), 1, 0, 0),
            tld          = if_else(str_detect(regimen, "TDF/3TC/DTG"), 1, 0, 0),

            diff         = floor(interval(previous_nextpickup, latest_ffupdate) / days(1)),
            prev_outcome = hiv_tx_outcome(outcome, previous_nextpickup, end_ym(2024, 4), 30),
            iit          = case_when(
               outcome == "alive on arv" &
                  outcome != prev_outcome &
                  diff < 90 ~ "IIT (ART <3 months)",
               outcome == "alive on arv" &
                  outcome != prev_outcome &
                  diff %in% seq(90, 179) ~ "IIT (ART 3-5 months)",
               outcome == "onart" &
                  outcome != prev_outcome &
                  diff >= 180 ~ "IIT (ART >=6 months)",
            ),
            ltfu_date    = if_else(!is.na(iit), previous_nextpickup, NA_Date_),

            pickup       = if_else(is.na(latest_nextpickup), latest_ffupdate + 30, latest_nextpickup, latest_nextpickup),
            ltfu_months  = floor(interval(pickup, end_ym(2024, 5)) / months(1)),
            ltfulen      = case_when(
               ltfu_months <= 1 ~ "(1) 1 mo. LTFU",
               ltfu_months %in% seq(2, 3) ~ "(2) 2-3 mos. LTFU",
               ltfu_months %in% seq(4, 5) ~ "(3) 4-5 mos. LTFU",
               ltfu_months %in% seq(6, 12) ~ "(4) 6-12 mos. LTFU",
               ltfu_months %in% seq(13, 36) ~ "(5) 2-3 yrs. LTFU",
               ltfu_months %in% seq(37, 60) ~ "(6) 4-5 yrs. LTFU",
               ltfu_months %in% seq(61, 120) ~ "(7) 6-10 yrs. LTFU",
               ltfu_months > 120 ~ "(8) 10+ yrs. LTFU",
               TRUE ~ "(no data)"
            ),
         ) %>%
         select(
            art_id,
            latest_regimen,
            latest_ffupdate,
            latest_nextpickup,
            line,
            lte,
            tld,
            realhub,
            realhub_branch,
            outcome,
            iit,
            ltfu_date,
            ltfulen,
            vl_date,
            vl_result,
            baseline_vl,
            vlp12m,
            curr_age
         ),
      by = join_by(art_id)
   ) %>%
   left_join(
      y  = dx %>%
         mutate(
            #sex
            sex             = case_when(
               StrLeft(toupper(sex), 1) == "M" ~ "Male",
               StrLeft(toupper(sex), 1) == "F" ~ "Female",
               TRUE ~ "(no data)"
            ),

            # MOT
            mot             = case_when(
               transmit == "SEX" & sexhow == "BISEXUAL" ~ "Sex w/ Males & Females",
               transmit == "SEX" & sexhow == "HETEROSEXUAL" ~ "Male-Female Sex",
               transmit == "SEX" & sexhow == "HOMOSEXUAL" ~ "Male-Male Sex",
               transmit == "IVDU" ~ "Sharing of infected needles",
               transmit == "PERINATAL" ~ "Mother-to-child",
               transmit == "TRANSFUSION" ~ "Blood transfusion",
               transmit == "OTHERS" ~ "Others",
               transmit == "UNKNOWN" ~ "(no data)",
               TRUE ~ transmit
            ),

            msm             = if_else(
               sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"),
               1,
               0,
               0
            ),
            tgw             = if_else(
               sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"),
               1,
               0,
               0
            ),
            kap_type        = case_when(
               transmit == "IVDU" ~ "PWID",
               msm == 1 & tgw == 0 ~ "MSM",
               msm == 1 & tgw == 1 ~ "MSM-TGW",
               sex == "Male" ~ "Other Males",
               # sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
               sex == "Female" ~ "Other Females",
               TRUE ~ "Other"
            ),

            confirm_type    = case_when(
               confirmlab == "OTHERS" ~ "OTHERS",
               is.na(rhivda_done) ~ "SACCL",
               rhivda_done == 0 ~ "SACCL",
               rhivda_done == 1 ~ "CrCL",
            ),

            mortality       = if_else(
               (dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"),
               0,
               1,
               1
            ),
            dx              = if_else(!is.na(idnum), 1, 0, 0),
            dx_plhiv        = if_else(dx == 1 & mortality == 0, 1, 0, 0),

            tat_confirm_art = floor(interval(confirm_date, artstart_date) / days(1)),
            tat_confirm_art = case_when(
               tat_confirm_art < 0 ~ "0) Treatment before confirmatory",
               tat_confirm_art == 0 ~ "1) Same day",
               tat_confirm_art >= 1 & tat_confirm_art <= 14 ~ "2) Within 7 days",
               tat_confirm_art >= 15 & tat_confirm_art <= 30 ~ "3) Within 30 days",
               tat_confirm_art >= 31 ~ "4) More than 30 days",
               is.na(confirm_date) & !is.na(idnum) ~ "4) More than 30 days",
               TRUE ~ "(not yet confirmed)",
            )
         ) %>%
         select(
            idnum,
            confirmlab,
            confirm_date,
            dxlab_standard,
            dx_region,
            dx_province,
            dx_muncity,
            region,
            province,
            muncity,
            mot,
            tat_confirm_art
         ),
      by = join_by(idnum)
   ) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   mutate(
      sex            = stri_trans_totitle(sex),
      confirm_branch = NA_character_
   ) %>%
   dxlab_to_id(
      c("HARP_FACI", "HARP_SUB_FACI"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
      ohasis$ref_faci
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(CONFIRM_FACI = "confirmlab", CONFIRM_SUB_FACI = "confirm_branch")
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(ART_FACI = "realhub", ART_SUB_FACI = "realhub_branch")
   ) %>%
   mutate(
      refer_status = case_when(
         ART_FACI == "040013" ~ "Last visited here",
         TRUE ~ "Transferred out"
      )
   ) %>%
   mutate_at(
      .vars = vars(HARP_FACI, HARP_SUB_FACI, CONFIRM_FACI, CONFIRM_SUB_FACI),
      ~replace_na(., "")
   ) %>%
   ohasis$get_faci(
      list(dx_lab = c("HARP_FACI", "HARP_SUB_FACI")),
      "name",
   ) %>%
   ohasis$get_faci(
      list(confirm_lab = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
      "name",
   ) %>%
   ohasis$get_faci(
      list(tx_hub = c("ART_FACI", "ART_SUB_FACI")),
      "name",
   ) %>%
   mutate(
      vlstatus      = case_when(
         !is.na(vl_result) & is.na(vlp12m) ~ "VL Test beyond past 12 months",
         !is.na(vl_result) & !is.na(vlp12m) ~ "VL Test w/in past 12 months",
         TRUE ~ "VL Naive"
      ),
      vlsuppress    = if_else(
         outcome == "alive on arv" &
            is.na(baseline_vl) &
            (vlp12m == 1 | vl_result < 1000),
         1,
         0,
         0
      ),
      vlsuppress_50 = if_else(
         outcome == "alive on arv" &
            is.na(baseline_vl) &
            vlp12m == 1,
         1,
         0,
         0
      ),
   ) %>%
   mutate_at(
      .vars = vars(lte, tld, baseline_vl, vlsuppress, vlsuppress_50),
      ~case_when(
         . == 1 ~ "Yes",
         TRUE ~ "No"
      )
   ) %>%
   select(
      `Central ID`                                  = CENTRAL_ID,
      `Confirmatory Code`                           = confirmatory_code,
      `Date Confirmed`                              = confirm_date,
      `UIC`                                         = uic,
      `Patient Code`                                = px_code,
      `First Name`                                  = first,
      `Middle Name`                                 = middle,
      `Last Name`                                   = last,
      `Suffix (Jr., Sr., III, etc)`                 = suffix,
      `Age`                                         = curr_age,
      `Sex (at birth)`                              = sex,
      `PhilHealth No.`                              = philhealth_no,
      `Permanent Address: Region`                   = region,
      `Permanent Address: Province`                 = province,
      `Permanent Address: City/Municipality`        = muncity,
      `Baseline CD4: Date`                          = baseline_cd4_date,
      `Baseline CD4: Result cells/μL`               = baseline_cd4_result,
      `Diagnosing Laboratory/Clinic`                = dx_lab,
      `Confirmatory Facility`                       = confirm_lab,
      `Mode of Transmission`                        = mot,
      `Reported Dead`                               = dead,
      `ART Enrollment Date`                         = artstart_date,
      `TAT from Confirmatory to Enrollment`         = tat_confirm_art,
      `Latest ARV Regimen`                          = latest_regimen,
      `Latest ARV Visit`                            = latest_ffupdate,
      `ARVs will last until:`                       = latest_nextpickup,
      `Line of Regimen`                             = line,
      `On LTE`                                      = lte,
      `On TLD`                                      = tld,
      `Last accessed treatment at:`                 = tx_hub,
      `ART Outcome`                                 = outcome,
      `Referral Status`                             = refer_status,
      `Time passed since client ran out of ARVs`    = iit,
      `Date ARVs ran out on before client returned` = ltfu_date,
      `Length of Lost to follow-up`                 = ltfulen,
      `VL: Testing Status`                          = vlstatus,
      `VL: Reported Date`                           = vl_date,
      `VL: Reported Result`                         = vl_result,
      `VL: Suppressed <1000 copies/mL`              = vlsuppress,
      `VL: Undetectable <50 copies/mL`              = vlsuppress_50,
      `VL: Is this a baseline test?`                = baseline_vl,
   )

write_xlsx(export, "H:/20240726_txcohort-imu_2024-05.xlsx")