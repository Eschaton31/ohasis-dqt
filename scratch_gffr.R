tmp <- tempfile(fileext = ".xlsx")
drive_download("https://docs.google.com/spreadsheets/d/1hwU9VtPIUi68-Wi5j-M-b02SarizdhmV/edit#gid=1120355249", tmp, overwrite = TRUE)
gf_sites <- read_xlsx(tmp, "100 cities plus whole Cebu Prov", range = "A2:D151") %>%
   select(
      region   = 1,
      province = 2,
      muncity  = 3,
      aem_cat  = 4
   ) %>%
   mutate(
      region = gsub(".0", "", region)
   ) %>%
   mutate_all(~as.character(.)) %>%
   mutate(
      region   = case_when(
         region == "" & province == "LANAO DEL NORTE" ~ "10",
         region == "" & province == "MISAMIS ORIENTAL" ~ "10",
         TRUE ~ region
      ),
      province = case_when(
         province == "METRO MANILA" ~ "NCR",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         TRUE ~ province
      ),
      muncity  = case_when(
         muncity == "LAPU LAPU" ~ "LAPU-LAPU",
         muncity == "CEBU CITY" ~ "CEBU",
         muncity == "QUEZON CITY" ~ "QUEZON",
         muncity == "ISLAND GARDEN SAMAL" ~ "SAMAL",
         stri_detect_fixed(muncity, "(") ~ substr(muncity, 1, stri_locate_first_fixed(muncity, " (") - 1),
         TRUE ~ muncity
      )
   ) %>%
   full_join(
      y  = ohasis$ref_addr %>%
         mutate(
            drop = case_when(
               StrLeft(PSGC_PROV, 4) == "1339" & (PSGC_MUNC != "133900000" | is.na(PSGC_MUNC)) ~ 1,
               StrLeft(PSGC_REG, 4) == "1300" & PSGC_MUNC == "" ~ 1,
               stri_detect_fixed(NAME_PROV, "City") & NHSSS_MUNC == "UNKNOWN" ~ 1,
               TRUE ~ 0
            ),
         ) %>%
         filter(drop == 0) %>%
         add_row(
            PSGC_REG   = "130000000",
            PSGC_PROV  = "",
            PSGC_MUNC  = "",
            NAME_REG   = "National Capital Region (NCR)",
            NAME_PROV  = "Unknown",
            NAME_MUNC  = "Unknown",
            NHSSS_REG  = "NCR",
            NHSSS_PROV = "UNKNOWN",
            NHSSS_MUNC = "UNKNOWN",
         ) %>%
         mutate(
            NAME_PROV = case_when(
               stri_detect_fixed(NAME_PROV, "NCR") ~ stri_replace_all_fixed(NAME_PROV, " (Not a Province)", ""),
               TRUE ~ NAME_PROV
            ),
            PSGC_PROV = case_when(
               PSGC_MUNC == "129804000" ~ "124700000",
               TRUE ~ PSGC_PROV
            ),
            NAME_MUNC = case_when(
               PSGC_MUNC == "031405000" ~ "Bulacan City",
               TRUE ~ NAME_MUNC
            )
         ) %>%
         select(
            region   = NHSSS_REG,
            province = NHSSS_PROV,
            muncity  = NHSSS_MUNC,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC
         ),
      by = join_by(region, province, muncity)
   ) %>%
   mutate(
      gf2426 = if_else(!is.na(aem_cat), 1, 0, 0)
   )
unlink(tmp)
rm(tmp)

harp <- hs_data("harp_full", "reg", 2022, 12) %>%
   read_dta() %>%
   left_join(
      y  = hs_data("harp_full", "reg", 2023, 1) %>%
         read_dta(col_select = c(idnum, artstart_date, everonart)) %>%
         rename(
            artstart_2023  = artstart_date,
            everonart_2023 = everonart
         ),
      by = join_by(idnum)
   ) %>%
   mutate(
      overseas_addr      = case_when(
         muncity == "OUT OF COUNTRY" ~ 1,
         TRUE ~ 0
      ),
      everonart          = case_when(
         everonart == 1 ~ 1,
         everonart_2023 == 1 ~ 1,
         TRUE ~ 0
      ),

      tat_confirm_enroll = interval(confirm_date, coalesce(artstart_date, artstart_2023)) / days(1),

      muncity            = case_when(
         muncity == "PINAMUNGAHAN" & province == "CEBU" ~ "PINAMUNGAJAN",
         muncity == "SAN JUAN" & province == "BULACAN" ~ "MALOLOS",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ muncity
      ),
      province           = case_when(
         muncity == "UNKNOWN" & province == "NCR" ~ "UNKNOWN",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ province
      ),
      region             = case_when(
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ region
      ),

      mortality          = if_else(
         (dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"),
         0,
         1,
         1
      ),


      dx                 = if_else(!is.na(idnum), 1, 0, 0),
      dx_plhiv           = if_else(!is.na(idnum) &
                                      (dead != 1 | is.na(dead)) &
                                      (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      plhiv              = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      everonart          = if_else(everonart == 1, 1, 0, 0),
      everonart_plhiv    = if_else(everonart == 1 & outcome != "alive on arv", 1, 0, 0),
      ononart            = if_else(onart == 1, 1, 0, 0),
      vl_tested          = if_else(
         onart == 1 &
            (is.na(baseline_vl) | baseline_vl == 0) &
            !is.na(vlp12m),
         1,
         0,
         0
      ),
      vl_suppressed      = if_else(
         onart == 1 &
            (is.na(baseline_vl) | baseline_vl == 0) &
            vlp12m == 1,
         1,
         0,
         0
      ),

      rep_age            = calc_age(
         bdate,
         as.Date("2022-12-31")
      ),
      rep_age            = if_else(
         is.na(bdate),
         (2022 - year) + age,
         rep_age,
         rep_age
      ),

      gf_rep_age_dx      = case_when(
         is.na(rep_age) & year <= 2007 ~ "15+",
         rep_age >= 15 ~ "15+",
         rep_age < 15 ~ "<15",
         TRUE ~ "(no data)"
      ),

      sex                = case_when(
         idnum == 1192 ~ "FEMALE",
         sex == "" ~ "MALE",
         TRUE ~ sex
      )
   ) %>%
   left_join(
      y  = gf_sites %>%
         select(
            region,
            province,
            muncity,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            gf2426
         ),
      by = join_by(region, province, muncity)
   )

art <- hs_data("harp_tx", "outcome", 2022, 12) %>%
   read_dta() %>%
   left_join(
      y = hs_data("harp_tx", "reg", 2022, 12) %>%
         read_dta(col_select = c(art_id, birthdate)),
      join_by(art_id)
   ) %>%
   mutate(
      rep_age         = calc_age(
         birthdate,
         as.Date("2022-12-31")
      ),
      rep_age         = if_else(
         is.na(birthdate),
         curr_age,
         rep_age,
         rep_age
      ),

      gf_rep_age_dx   = case_when(
         is.na(rep_age) ~ "15+",
         rep_age >= 15 ~ "15+",
         rep_age < 15 ~ "<15",
         TRUE ~ "(no data)"
      ),


      outcome30       = hiv_tx_outcome(outcome, latest_nextpickup, as.Date("2022-12-31"), 30),
      onart_new       = if_else(
         condition = outcome30 == "alive on arv",
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      baseline_vl_new = if_else(
         condition = floor(interval(artstart_date, vl_date) / months(1)) < 6,
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      vltested_new    = if_else(
         onart_new == 1 &
            baseline_vl_new == 0 &
            !is.na(vlp12m),
         1,
         0,
         0
      ),
      vlsuppress_50   = if_else(
         onart_new == 1 &
            baseline_vl_new == 0 &
            vlp12m == 1 &
            vl_result < 50,
         1,
         0,
         0
      ),
   ) %>%
   left_join(
      y  = gf_sites %>%
         select(
            tx_reg  = region,
            tx_prov = province,
            tx_munc = muncity,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            gf2426
         ),
      by = join_by(tx_reg, tx_prov, tx_munc)
   )

conn      <- ohasis$conn("lw")
oh_prep22 <- tracked_select(conn, r"(
SELECT DISTINCT COALESCE(id.CENTRAL_ID, prep.PATIENT_ID) AS CENTRAL_ID
FROM ohasis_warehouse.form_prep AS prep
         LEFT JOIN ohasis_warehouse.id_registry AS id ON prep.PATIENT_ID = id.PATIENT_ID
WHERE prep.MEDICINE_SUMMARY IS NOT NULL AND YEAR(prep.RECORD_DATE) = 2022;
)", "prep")
oh_prep21 <- tracked_select(conn, r"(
SELECT DISTINCT COALESCE(id.CENTRAL_ID, prep.PATIENT_ID) AS CENTRAL_ID
FROM ohasis_warehouse.form_prep AS prep
         LEFT JOIN ohasis_warehouse.id_registry AS id ON prep.PATIENT_ID = id.PATIENT_ID
WHERE prep.MEDICINE_SUMMARY IS NOT NULL AND YEAR(prep.RECORD_DATE) = 2021;
)", "prep")
id_reg    <- tracked_select(conn, r"(
SELECT CENTRAL_ID, PATIENT_ID FROM ohasis_warehouse.id_registry;
)", "id_reg")
dbDisconnect(conn)
rm(conn)
prep <- hs_data("prep", "outcome", 2022, 12) %>%
   read_dta() %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y  = oh_prep22 %>%
         mutate(prep2022 = 1),
      by = join_by(CENTRAL_ID),
   ) %>%
   left_join(
      y  = oh_prep21 %>%
         mutate(prep2021 = 1),
      by = join_by(CENTRAL_ID),
   ) %>%
   mutate(
      rep_age       = calc_age(
         birthdate,
         as.Date("2022-12-31")
      ),
      rep_age       = if_else(
         is.na(birthdate),
         curr_age,
         rep_age,
         rep_age
      ),

      gf_rep_age_dx = case_when(
         rep_age >= 15 & rep_age < 20 ~ "15-19",
         rep_age >= 20 & rep_age < 25 ~ "20-24",
         rep_age < 15 ~ "<15",
         rep_age >= 25 ~ "25+",
         TRUE ~ "(no data)"
      ),

      # KAP
      msm           = case_when(
         sex == "MALE" & stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
         sex == "MALE" & stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
         sex == "MALE" & kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw           = if_else(
         condition = sex == "MALE" & self_identity == "FEMALE",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      pwid          = case_when(
         stri_detect_fixed(prep_risk_injectdrug, "yes") ~ 1,
         stri_detect_fixed(hts_risk_injectdrug, "yes") ~ 1,
         kp_pwid == 1 ~ 1,
         TRUE ~ 0
      ),

      prep2022      = if_else(!is.na(prepstart_date) & prep2022 == 1, 1, 0, 0)
   ) %>%
   left_join(
      y  = gf_sites %>%
         select(
            prep_reg  = region,
            prep_prov = province,
            prep_munc = muncity,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            gf2426
         ),
      by = join_by(prep_reg, prep_prov, prep_munc)
   )

disagg <- harp %>%
   mutate(
      o11_total_natl = dx_plhiv,
      o11_total_gf   = if_else(dx_plhiv == 1 & gf2426 == 1, 1, 0, 0),
      o11_m_natl     = if_else(dx_plhiv == 1 &
                                  sex == "MALE" &
                                  gf_rep_age_dx == "15+", 1, 0, 0),
      o11_m_gf       = if_else(dx_plhiv == 1 &
                                  gf2426 == 1 &
                                  sex == "MALE" &
                                  gf_rep_age_dx == "15+", 1, 0, 0),
      o11_f_natl     = if_else(dx_plhiv == 1 &
                                  sex == "FEMALE" &
                                  gf_rep_age_dx == "15+", 1, 0, 0),
      o11_f_gf       = if_else(dx_plhiv == 1 &
                                  gf2426 == 1 &
                                  sex == "FEMALE" &
                                  gf_rep_age_dx == "15+", 1, 0, 0),
   ) %>%
   group_by(gf_rep_age_dx) %>%
   summarise_at(
      .vars = vars(starts_with("o11")),
      ~sum(.)
   ) %>%
   ungroup() %>%
   bind_rows(
      hs_data("harp_dx", "reg", 2023, 1) %>%
         read_dta(
            col_select = c(
               idnum,
               confirm_date,
               year,
               region,
               province,
               muncity,
               sex,
               bdate,
               age
            )
         ) %>%
         filter(
            year == 2022
         ) %>%
         left_join(
            y  = gf_sites %>%
               select(
                  region,
                  province,
                  muncity,
                  PSGC_REG,
                  PSGC_PROV,
                  PSGC_MUNC,
                  gf2426
               ),
            by = join_by(region, province, muncity)
         ) %>%
         left_join(
            y  = hs_data("harp_tx", "outcome", 2023, 1) %>%
               read_dta(
                  col_select = c(
                     idnum,
                     artstart_date
                  )
               ),
            by = join_by(idnum)
         ) %>%
         mutate(
            rep_age            = calc_age(
               bdate,
               as.Date("2022-12-31")
            ),
            rep_age            = if_else(
               is.na(bdate),
               (2022 - year) + age,
               rep_age,
               rep_age
            ),

            gf_rep_age_dx      = case_when(
               is.na(rep_age) & year <= 2007 ~ "15+",
               rep_age >= 15 ~ "15+",
               rep_age < 15 ~ "<15",
               TRUE ~ "(no data)"
            ),

            sex                = case_when(
               idnum == 1192 ~ "FEMALE",
               sex == "" ~ "MALE",
               TRUE ~ sex
            ),
            dx                 = 1,
            tat_confirm_enroll = floor(interval(confirm_date, artstart_date) / days(1)),
            hts5_total_natl    = if_else(dx == 1 &
                                            year == 2022 &
                                            tat_confirm_enroll <= 7, 1, 0, 0),
            hts5_total_gf      = if_else(dx == 1 &
                                            year == 2022 &
                                            tat_confirm_enroll <= 7 &
                                            gf2426 == 1, 1, 0, 0),
            hts5_m_natl        = if_else(dx == 1 &
                                            year == 2022 &
                                            tat_confirm_enroll <= 7 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            hts5_m_gf          = if_else(dx == 1 &
                                            year == 2022 &
                                            tat_confirm_enroll <= 7 &
                                            gf2426 == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            hts5_f_natl        = if_else(dx == 1 &
                                            year == 2022 &
                                            tat_confirm_enroll <= 7 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            hts5_f_gf          = if_else(dx == 1 &
                                            year == 2022 &
                                            tat_confirm_enroll <= 7 &
                                            gf2426 == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
         ) %>%
         group_by(gf_rep_age_dx) %>%
         summarise_at(
            .vars = vars(starts_with("hts5")),
            ~sum(.)
         ) %>%
         ungroup()
   ) %>%
   bind_rows(
      art %>%
         mutate(
            o21_total_ntl      = if_else(onart_new == 0 & outcome != "dead", 1, 0, 0),
            o21_total_gf       = if_else(onart_new == 0 & outcome != "dead" & gf2426 == 1, 1, 0, 0),
            o21_m15plus_ntl    = if_else(onart_new == 0 &
                                            outcome != "dead" &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            o21_m15plus_gf     = if_else(onart_new == 0 &
                                            outcome != "dead" &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            o21_f15plus_ntl    = if_else(onart_new == 0 &
                                            outcome != "dead" &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            o21_f15plus_gf     = if_else(onart_new == 0 &
                                            outcome != "dead" &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            tcs8_total_ntl     = if_else(onart_new == 1 &
                                            vltested_new == 1, 1, 0, 0),
            tcs8_total_gf      = if_else(onart_new == 1 &
                                            vltested_new == 1 &
                                            gf2426 == 1, 1, 0, 0),
            tcs8_m15plus_ntl   = if_else(onart_new == 1 &
                                            vltested_new == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            tcs8_m15plus_gf    = if_else(onart_new == 1 &
                                            vltested_new == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            tcs8_f15plus_ntl   = if_else(onart_new == 1 &
                                            vltested_new == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            tcs8_f15plus_gf    = if_else(onart_new == 1 &
                                            vltested_new == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            o12_total_ntl      = if_else(onart_new == 1 &
                                            vlsuppress_50 == 1, 1, 0, 0),
            o12_total_gf       = if_else(onart_new == 1 &
                                            vlsuppress_50 == 1 &
                                            gf2426 == 1, 1, 0, 0),
            o12_m15plus_ntl    = if_else(onart_new == 1 &
                                            vlsuppress_50 == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            o12_m15plus_gf     = if_else(onart_new == 1 &
                                            vlsuppress_50 == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            o12_f15plus_ntl    = if_else(onart_new == 1 &
                                            vlsuppress_50 == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            o12_f15plus_gf     = if_else(onart_new == 1 &
                                            vlsuppress_50 == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            tcs1b_m15plus_ntl  = if_else(onart_new == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            tcs1b_m15plus_gf   = if_else(onart_new == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            tcs1b_f15plus_ntl  = if_else(onart_new == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+", 1, 0, 0),
            tcs1b_f15plus_gf   = if_else(onart_new == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "15+" &
                                            gf2426 == 1, 1, 0, 0),
            tcs1c_m15below_ntl = if_else(onart_new == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "<15", 1, 0, 0),
            tcs1c_m15below_gf  = if_else(onart_new == 1 &
                                            sex == "MALE" &
                                            gf_rep_age_dx == "<15" &
                                            gf2426 == 1, 1, 0, 0),
            tcs1c_f15below_ntl = if_else(onart_new == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "<15", 1, 0, 0),
            tcs1c_f15below_gf  = if_else(onart_new == 1 &
                                            sex == "FEMALE" &
                                            gf_rep_age_dx == "<15" &
                                            gf2426 == 1, 1, 0, 0),
         ) %>%
         group_by(gf_rep_age_dx) %>%
         summarise_at(
            .vars = vars(starts_with("o21"), starts_with("o12"), starts_with("test1"), starts_with("tcs")),
            ~sum(.)
         ) %>%
         ungroup()
   ) %>%
   bind_rows(
      prep %>%
         mutate(
            kp6a_total_ntl   = if_else(msm == 1 & tgw == 0 & prep2022 == 1, 1, 0, 0),
            kp6a_total_gf    = if_else(msm == 1 & tgw == 0 & prep2022 == 1 & gf2426 == 1, 1, 0, 0),
            kp7b_total_ntl   = if_else(msm == 1 & tgw == 1 & prep2022 == 1, 1, 0, 0),
            kp7b_total_gf    = if_else(msm == 1 & tgw == 1 & prep2022 == 1 & gf2426 == 1, 1, 0, 0),
            kp6d_total_ntl   = if_else(msm == 0 & pwid == 1 & prep2022 == 1, 1, 0, 0),
            kp6d_total_gf    = if_else(msm == 0 &
                                          pwid == 1 &
                                          prep2022 == 1 &
                                          gf2426 == 1, 1, 0, 0),
            kp6d_m15plus_ntl = if_else(msm == 0 &
                                          pwid == 1 &
                                          sex == "MALE" &
                                          prep2022 == 1, 1, 0, 0),
            kp6d_m15plus_gf  = if_else(msm == 0 &
                                          pwid == 1 &
                                          sex == "MALE" &
                                          prep2022 == 1 &
                                          gf2426 == 1, 1, 0, 0),
            kp6d_f15plus_ntl = if_else(msm == 0 &
                                          pwid == 1 &
                                          sex == "FEMALE" &
                                          prep2022 == 1, 1, 0, 0),
            kp6d_f15plus_gf  = if_else(msm == 0 &
                                          pwid == 1 &
                                          sex == "FEMALE" &
                                          prep2022 == 1 &
                                          gf2426 == 1, 1, 0, 0),
         ) %>%
         group_by(gf_rep_age_dx) %>%
         summarise_at(
            .vars = vars(starts_with("kp6a"), starts_with("kp7b"), starts_with("kp6d")),
            ~sum(.)
         ) %>%
         ungroup()
   )

cols      <- select(disagg, -gf_rep_age_dx) %>% get_names()
gf_disagg <- disagg %>%
   pivot_longer(
      cols = all_of(cols)
   ) %>%
   mutate(
      coverage = case_when(
         grepl("_ntl$", name) ~ "PH",
         grepl("_natl$", name) ~ "PH",
         grepl("_gf", name) ~ "GF",
      ),
      var      = substr(name, 1, stri_locate_last_fixed(name, "_") - 1)
   ) %>%
   filter(!is.na(value)) %>%
   pivot_wider(
      id_cols     = c(gf_rep_age_dx, var),
      names_from  = coverage,
      values_from = value,
   ) %>%
   arrange(var, gf_rep_age_dx)

write_xlsx(gf_disagg, "C:/20230313_FR1545-PHL-H_PerformanceFramework_draft20230309 - Bene.xlsx")


tx_ml <- art %>%
   left_join(
      y  = hs_data("harp_tx", "reg", 2022, 12) %>%
         read_dta() %>%
         mutate(
            confirmatory_code = case_when(
               confirmatory_code == "" & uic != "" ~ as.character(glue("*{uic}")),
               confirmatory_code == "" & px_code != "" ~ as.character(glue("*{px_code}")),
               art_id == 43460 ~ "JEJO0111221993_1",
               art_id == 82604 ~ "JEJO0111221993_2",
               TRUE ~ confirmatory_code
            ),
            sacclcode         = gsub("[^[:alnum:]]", "", confirmatory_code)
         ) %>%
         select(
            art_id,
            sacclcode
         ),
      by = "art_id"
   ) %>%
   rename(
      outcome30_2022 = outcome30
   ) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", 2021, 12) %>%
         read_dta() %>%
         mutate(
            sacclcode = gsub("[^[:alnum:]]", "", sacclcode),
            outcome30 = hiv_tx_outcome(outcome, latest_nextpickup, as.Date("2021-12-31"), 30),
         ) %>%
         select(
            sacclcode,
            outcome30_2021 = outcome30
         ),
      by = join_by(sacclcode)
   )

tx_ml %>%
   mutate(
      outcome30_2021 = if_else(year(artstart_date) == 2022, "newly enrolled", outcome30_2021, outcome30_2021),
      o21            = if_else(
         outcome30_2021 %in% c("alive on arv", "newly enrolled"),
         1,
         0, 0
      ),
      tx_ml          = if_else(
         outcome30_2021 %in% c("alive on arv", "newly enrolled") & outcome30_2022 != "alive on arv",
         1,
         0, 0
      )
   ) %>%
   filter(o21 == 1) %>%
   # filter(outcome30_2021 %in% c("alive on arv", "newly enrolled")) %>%
   # filter(outcome30_2021 %in% c("alive on arv", "newly enrolled"), outcome30_2022 != "alive on arv") %>%
   # tab(outcome30_2021, sex, gf_rep_age_dx)
   tab(gf_rep_age_dx, tx_ml)

