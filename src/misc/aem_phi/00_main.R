aem      <- list()
aem$ref  <- psgc_aem(ohasis$ref_addr)
aem$harp <- list(
   dx   = list(
      jul = read_dta(hs_data("harp_full", "reg", 2023, 7)),
      aug = read_dta(hs_data("harp_full", "reg", 2023, 8))
   ),
   tx   = list(
      jul = read_dta(hs_data("harp_tx", "outcome", 2023, 7)),
      aug = read_dta(hs_data("harp_tx", "outcome", 2023, 8))
   ),
   dead = list(
      jul = read_dta(hs_data("harp_dead", "reg", 2023, 7)),
      aug = read_dta(hs_data("harp_dead", "reg", 2023, 8))
   ),
   est  = aem$ref$aem %>%
      select(
         PSGC_REG,
         PSGC_PROV,
         PSGC_AEM,
         report_yr,
         est,
      ) %>%
      pivot_wider(
         id_cols      = c(PSGC_REG, PSGC_PROV, PSGC_AEM),
         names_from   = report_yr,
         names_prefix = "est",
         values_from  = est
      ) %>%
      mutate_at(
         .vars = vars(starts_with("PSGC")),
         ~gsub("^PH", "", .)
      )
)

aem$harp$dx <- lapply(aem$harp$dx, function(data) {
   data %<>%
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
      )

   return(data)
})
aem$harp$tx <- lapply(aem$harp$tx, function(data) {
   data %<>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         list(tx_faci_id = "realhub", tx_sub_faci_id = "realhub_branch")
      ) %>%
      mutate_at(
         .vars = vars(outcome),
         ~case_when(
            . == "alive on arv" ~ "onart",
            . == "alive on art" ~ "onart",
            . == "lost to follow up" ~ "ltfu",
            . == "trans out" ~ "transout",
            . == "stopped - refused" ~ "refused",
            . == "stopped - negative" ~ "negative",
            . == "transout - overseas" ~ "abroad",
            . == "dead" ~ "dead",
            TRUE ~ .
         )
      )

   return(data)
})
lw_conn     <- ohasis$conn("lw")
aem$oh$tx   <- tracked_select(lw_conn, r"(
SELECT DISTINCT COALESCE(id.CENTRAL_ID, art.PATIENT_ID)                                    AS CENTRAL_ID,
                CASE
                    WHEN SERVICE_FACI = '130000' THEN FACI_ID
                    WHEN SERVICE_FACI IS NULL THEN FACI_ID
                    ELSE SERVICE_FACI END                                                  AS FACI_ID,
                IF(SERVICE_FACI IN ("130001", "130605", "040200"), SERVICE_SUB_FACI, NULL) AS SUB_FACI_ID,
                VISIT_DATE
FROM ohasis_warehouse.form_art_bc AS art
         LEFT JOIN ohasis_warehouse.id_registry AS id ON art.PATIENT_ID = id.PATIENT_ID;
   )", "OHASIS tx") %>%
   arrange(VISIT_DATE) %>%
   distinct(CENTRAL_ID, FACI_ID, SUB_FACI_ID, .keep_all = TRUE)
dbDisconnect(lw_conn)


sum_dx <- aem$harp$dx$jul %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = TRUE
   ) %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~if_else(coalesce(., "") != "", stri_c("PH", .), ., .)
   ) %>%
   left_join(
      y  = aem$ref$addr %>%
         select(
            PERM_PSGC_REG  = PSGC_REG,
            PERM_PSGC_PROV = PSGC_PROV,
            PERM_PSGC_MUNC = PSGC_MUNC,
            PERM_PSGC_AEM  = PSGC_AEM,
            REGION         = NHSSS_REG,
            PROVINCE       = NHSSS_PROV,
            MUNCITY        = NHSSS_MUNC,
            MUNCITY_AEM    = NHSSS_AEM,
         ),
      by = join_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_MUNC)
   ) %>%
   mutate(
      mortality = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
      plhiv     = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
   ) %>%
   group_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_AEM, REGION, PROVINCE, MUNCITY_AEM) %>%
   summarise(
      plhiv_prev = sum(plhiv),
   ) %>%
   ungroup() %>%
   full_join(
      y  = aem$harp$dx$aug %>%
         harp_addr_to_id(
            ohasis$ref_addr,
            c(
               PERM_PSGC_REG  = "region",
               PERM_PSGC_PROV = "province",
               PERM_PSGC_MUNC = "muncity"
            ),
            aem_sub_ntl = TRUE
         ) %>%
         mutate(
            mortality = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv     = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
            new       = if_else(year == 2023 & month == 8 & plhiv == 1, 1, 0, 0),
            newdx       = if_else(year == 2023 & month == 8, 1, 0, 0),
         ) %>%
         mutate_at(
            .vars = vars(contains("PSGC", ignore.case = FALSE)),
            ~if_else(coalesce(., "") != "", stri_c("PH", .), ., .)
         ) %>%
         left_join(
            y  = aem$ref$addr %>%
               select(
                  PERM_PSGC_REG  = PSGC_REG,
                  PERM_PSGC_PROV = PSGC_PROV,
                  PERM_PSGC_MUNC = PSGC_MUNC,
                  PERM_PSGC_AEM  = PSGC_AEM,
                  REGION         = NHSSS_REG,
                  PROVINCE       = NHSSS_PROV,
                  MUNCITY        = NHSSS_MUNC,
                  MUNCITY_AEM    = NHSSS_AEM,
               ),
            by = join_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_MUNC)
         ) %>%
         group_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_AEM, REGION, PROVINCE, MUNCITY_AEM) %>%
         summarise(
            plhiv_curr    = sum(plhiv),
            newplhiv_curr = sum(new)
         ) %>%
         ungroup(),
      by = join_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_AEM, REGION, PROVINCE, MUNCITY_AEM)
   ) %>%
   full_join(
      y  = aem$harp$est %>%
         select(
            PERM_PSGC_REG  = PSGC_REG,
            PERM_PSGC_PROV = PSGC_PROV,
            PERM_PSGC_AEM  = PSGC_AEM,
            est2023
         ) %>%
         mutate_at(
            .vars = vars(contains("PSGC", ignore.case = FALSE)),
            ~if_else(coalesce(., "") != "", stri_c("PH", .), ., .)
         ),
      by = join_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_AEM)
   ) %>%
   filter(!is.na(REGION)) %>%
   mutate(
      plhiv_targe2023     = est2023 * .95,
      plhiv_coverage_prev = plhiv_prev / est2023,
      plhiv_gap_prev      = plhiv_targe2023 - plhiv_prev,
      plhiv_targetmo_prev = plhiv_gap_prev / 5,
      plhiv_coverage_curr = plhiv_curr / est2023,
      plhiv_gap_curr      = plhiv_targe2023 - plhiv_curr,
      plhiv_targetmo_curr = plhiv_gap_curr / 4,
   ) %>%
   select(
      REGION,
      PROVINCE,
      MUNCITY_AEM,
      est2023,
      plhiv_targe2023,
      plhiv_prev,
      plhiv_coverage_prev,
      plhiv_gap_prev,
      plhiv_targetmo_prev,
      newplhiv_curr,
      plhiv_curr,
      plhiv_coverage_curr,
      plhiv_gap_curr,
      plhiv_targetmo_curr,
   )

sum_tx <- aem$harp$tx$jul %>%
   left_join(
      y  = aem$harp$dx$aug %>%
         select(
            idnum,
            region,
            province,
            muncity,
            dead
         ),
      by = join_by(idnum)
   ) %>%
   mutate(
      outcome = if_else(outcome == "ltfu" & dead == 1, "dead", outcome, outcome)
   ) %>%
   mutate_at(
      .vars = vars(region, province, muncity),
      ~coalesce(na_if(., ""), "UNKNOWN")
   ) %>%
   group_by(region, province, muncity, outcome) %>%
   summarise(
      clients = n(),
   ) %>%
   ungroup() %>%
   mutate(
      outcome = stri_c("prev_", outcome)
   ) %>%
   pivot_wider(
      id_cols     = c(region, province, muncity),
      names_from  = outcome,
      values_from = clients
   ) %>%
   full_join(
      y  = aem$harp$tx$aug %>%
         left_join(
            y  = aem$harp$dx$aug %>%
               select(
                  idnum,
                  region,
                  province,
                  muncity,
                  dead
               ),
            by = join_by(idnum)
         ) %>%
         left_join(
            y  = aem$harp$tx$jul %>%
               select(
                  art_id,
                  prev_outcome = outcome
               ),
            by = join_by(art_id)
         ) %>%
         mutate(
            outcome = case_when(
               newonart == 1 & outcome == "onart" ~ "newonart",
               outcome == "onart" & prev_outcome == "onart" ~ "retained",
               outcome == "onart" & prev_outcome != "onart" ~ "rtt",
               outcome == "ltfu" & prev_outcome == "onart" ~ "ml",
               outcome == "ltfu" & dead == 1 ~ "dead",
               outcome == prev_outcome ~ outcome,
               TRUE ~ outcome
            )
         ) %>%
         mutate_at(
            .vars = vars(region, province, muncity),
            ~coalesce(na_if(., ""), "UNKNOWN")
         ) %>%
         group_by(region, province, muncity, outcome) %>%
         summarise(
            clients = n(),
         ) %>%
         ungroup() %>%
         mutate(
            outcome = stri_c("curr_", outcome)
         ) %>%
         pivot_wider(
            id_cols     = c(region, province, muncity),
            names_from  = outcome,
            values_from = clients
         ),
      by = join_by(region, province, muncity)
   ) %>%
   full_join(
      y  = aem$harp$dx$jul %>%
         mutate(
            mortality  = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv      = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
            plhiv_dead = if_else(!is.na(idnum) & coalesce(dead, 0) == 1, 1, 0, 0),
            new        = if_else(year == 2023 & month == 8 & plhiv == 1, 1, 0, 0),
         ) %>%
         select(
            plhiv,
            plhiv_dead,
            region,
            province,
            muncity,
            everonart
         ) %>%
         group_by(region, province, muncity) %>%
         summarise(
            dx_prev       = n(),
            plhiv_prev    = sum(plhiv),
            dead_prev     = sum(plhiv_dead),
            notstart_prev = sum(if_else(plhiv == 1 & is.na(everonart), 1, 0, 0))
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   full_join(
      y  = aem$harp$dx$aug %>%
         mutate(
            mortality  = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv      = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
            plhiv_dead = if_else(!is.na(idnum) & coalesce(dead, 0) == 1, 1, 0, 0),
            new        = if_else(year == 2023 & month == 8 & plhiv == 1, 1, 0, 0),
         ) %>%
         select(
            year,
            month,
            plhiv,
            plhiv_dead,
            region,
            province,
            muncity,
            everonart
         ) %>%
         group_by(region, province, muncity) %>%
         summarise(
            dx_curr       = n(),
            dead_curr     = sum(plhiv_dead),
            newplhiv_curr = sum(if_else(plhiv == 1 & year == 2023 & month == 8, 1, 0, 0)),
            plhiv_curr    = sum(plhiv),
            notstart_curr = sum(if_else(plhiv == 1 & is.na(everonart), 1, 0, 0))
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   # full_join(
   #    y  = aem$harp$dead$jul %>%
   #       filter(!is.na(idnum)) %>%
   #       select(
   #          region   = final_region,
   #          province = final_province,
   #          muncity  = final_muncity,
   #       ) %>%
   #       group_by(region, province, muncity) %>%
   #       summarise(
   #          dead_prev = n()
   #       ) %>%
   #       ungroup(),
   #    by = join_by(region, province, muncity)
   # ) %>%
   # full_join(
   #    y  = aem$harp$dead$aug %>%
   #       filter(!is.na(idnum)) %>%
   #       select(
   #          region   = final_region,
   #          province = final_province,
   #          muncity  = final_muncity,
   #       ) %>%
   #       group_by(region, province, muncity) %>%
   #       summarise(
   #          dead_curr = n()
   #       ) %>%
   #       ungroup(),
   #    by = join_by(region, province, muncity)
   # ) %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(coalesce(., 0))
   ) %>%
   mutate(
      txcoverage_prev  = `prev_onart` / (plhiv_prev - `prev_abroad`),
      target_prev      = plhiv_prev * .95,
      txgap_prev       = (plhiv_prev - `prev_abroad`) - `prev_onart`,
      target_curr      = plhiv_curr * .95,
      tx_targetmo_prev = txgap_prev / 5,
      txcoverage_curr  = (`curr_newonart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_onart`) / (plhiv_curr - `curr_abroad`),
      txgap_curr       = (plhiv_curr - `curr_abroad`) - (`curr_newonart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_onart`),

      `curr_retained`  = `curr_retained` + `curr_onart`,
   ) %>%
   select(
      region,
      province,
      muncity,
      dx_prev,
      plhiv_prev,
      dead_prev,
      target_prev,
      notstart_prev,
      `prev_onart`,
      `prev_ltfu`,
      `prev_stopped - refused`,
      `prev_abroad`,
      txcoverage_prev,
      txgap_prev,
      newplhiv_curr,
      plhiv_curr,
      notstart_curr,
      `curr_newonart`,
      `curr_retained`,
      `curr_rtt`,
      `curr_ml`,
      `curr_ltfu`,
      `curr_stopped - refused`,
      `curr_abroad`,
      txcoverage_curr,
      txgap_curr,
   )

sum_txfaci <- aem$harp$tx$jul %>%
   mutate(
      realhub = coalesce(na_if(realhub_branch, ""), realhub)
   ) %>%
   group_by(real_reg, real_prov, real_munc, realhub, outcome) %>%
   summarise(
      clients = n(),
   ) %>%
   ungroup() %>%
   mutate(
      outcome = stri_c("prev_", outcome)
   ) %>%
   pivot_wider(
      id_cols     = c(real_reg, real_prov, real_munc, realhub),
      names_from  = outcome,
      values_from = clients
   ) %>%
   full_join(
      y  = aem$harp$tx$aug %>%
         left_join(
            y  = aem$harp$tx$jul %>%
               select(
                  art_id,
                  prev_outcome = outcome
               ),
            by = join_by(art_id)
         ) %>%
         mutate(
            outcome = case_when(
               newonart == 1 & outcome == "onart" ~ "newonart",
               outcome == "onart" & prev_outcome == "onart" ~ "retained",
               outcome == "onart" & prev_outcome != "onart" ~ "rtt",
               outcome == "ltfu" & prev_outcome == "onart" ~ "ml",
               outcome == prev_outcome ~ outcome,
               TRUE ~ outcome
            )
         ) %>%
         mutate(
            realhub = coalesce(na_if(realhub_branch, ""), realhub)
         ) %>%
         group_by(real_reg, real_prov, real_munc, realhub, outcome) %>%
         summarise(
            clients = n(),
         ) %>%
         ungroup() %>%
         mutate(
            outcome = stri_c("curr_", outcome)
         ) %>%
         pivot_wider(
            id_cols     = c(real_reg, real_prov, real_munc, realhub),
            names_from  = outcome,
            values_from = clients
         ),
      by = join_by(real_reg, real_prov, real_munc, realhub)
   ) %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(coalesce(., 0))
   ) %>%
   mutate(
      total_prev      = (`prev_onart` +
         `prev_ltfu` +
         `prev_dead` +
         `prev_stopped - refused` +
         `prev_abroad`),
      txcoverage_prev = `prev_onart` / total_prev,
      ltfurate_prev   = `prev_ltfu` / total_prev,

      total_curr      = `curr_newonart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_onart` +
         `curr_ml` +
         `curr_ltfu` +
         `curr_dead` +
         `curr_stopped - refused` +
         `curr_abroad`,
      txcoverage_curr = (`curr_newonart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_onart`) / total_curr,
      ltfu_curr       = `curr_ml` + `curr_ltfu`,
      ltfurate_curr   = ltfu_curr / total_curr,
   ) %>%
   select(
      real_reg,
      real_prov,
      real_munc,
      realhub,
      `prev_onart`,
      `prev_ltfu`,
      `prev_stopped - refused`,
      `prev_abroad`,
      txcoverage_prev,
      ltfurate_prev,
      `curr_newonart`,
      `curr_retained`,
      `curr_rtt`,
      `curr_ml`,
      `curr_ltfu`,
      `curr_stopped - refused`,
      `curr_abroad`,
      txcoverage_curr,
      ltfurate_curr,
      ltfu_curr
   )

sum_txfaci <- aem$harp$tx$jul %>%
   group_by(tx_faci_id, tx_sub_faci_id, outcome) %>%
   summarise(
      clients = n(),
   ) %>%
   ungroup() %>%
   mutate(
      outcome = stri_c("prev_", outcome)
   ) %>%
   pivot_wider(
      id_cols     = c(tx_faci_id, tx_sub_faci_id),
      names_from  = outcome,
      values_from = clients
   ) %>%
   full_join(
      y = aem$harp$tx$jul %>%
         filter(outcome %in% c("onart", "ltfu", "refused")) %>%
         group_by(tx_faci_id, tx_sub_faci_id) %>%
         summarise(
            txplhiv_prev = n(),
         ) %>%
         ungroup()
   ) %>%
   full_join(
      y = aem$harp$tx$aug %>%
         filter(outcome %in% c("onart", "ltfu", "refused")) %>%
         group_by(tx_faci_id, tx_sub_faci_id) %>%
         summarise(
            txplhiv_curr = n(),
         ) %>%
         ungroup()
   ) %>%
   # select(-prev_onart, -prev_ltfu, -prev_refused) %>%
   full_join(
      y  = aem$harp$tx$aug %>%
         left_join(
            y  = aem$harp$dx$aug %>%
               select(
                  idnum,
                  region,
                  province,
                  muncity,
                  dead
               ),
            by = join_by(idnum)
         ) %>%
         left_join(
            y  = aem$harp$tx$jul %>%
               select(
                  art_id,
                  prev_outcome = outcome
               ),
            by = join_by(art_id)
         ) %>%
         mutate(
            outcome = case_when(
               newonart == 1 & outcome == "onart" ~ "newonart",
               outcome == "onart" & prev_outcome == "onart" ~ "retained",
               outcome == "onart" & prev_outcome != "onart" ~ "rtt",
               outcome == "ltfu" & prev_outcome == "onart" ~ "ml",
               outcome == "ltfu" & dead == 1 ~ "dead",
               outcome == prev_outcome ~ outcome,
               TRUE ~ outcome
            )
         ) %>%
         group_by(tx_faci_id, tx_sub_faci_id, outcome) %>%
         summarise(
            clients = n(),
         ) %>%
         ungroup() %>%
         mutate(
            outcome = stri_c("curr_", outcome)
         ) %>%
         pivot_wider(
            id_cols     = c(tx_faci_id, tx_sub_faci_id),
            names_from  = outcome,
            values_from = clients
         ),
      by = join_by(tx_faci_id, tx_sub_faci_id)
   ) %>%
   left_join(
      y  = aem$harp$dx$jul %>%
         mutate(
            mortality  = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv      = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
            plhiv_dead = if_else(!is.na(idnum) & coalesce(dead, 0) == 1, 1, 0, 0),
            new        = if_else(year == 2023 & month == 8 & plhiv == 1, 1, 0, 0),
         ) %>%
         select(
            plhiv,
            plhiv_dead,
            tx_faci_id     = dx_faci_id,
            tx_sub_faci_id = dx_sub_faci_id,
            everonart
         ) %>%
         group_by(tx_faci_id, tx_sub_faci_id) %>%
         summarise(
            dx_prev       = n(),
            plhiv_prev    = sum(plhiv),
            dead_prev     = sum(plhiv_dead),
            notstart_prev = sum(if_else(plhiv == 1 & is.na(everonart), 1, 0, 0))
         ) %>%
         ungroup(),
      by = join_by(tx_faci_id, tx_sub_faci_id)
   ) %>%
   left_join(
      y  = aem$harp$dx$jul %>%
         mutate(
            mortality = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv     = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
         ) %>%
         filter(plhiv == 1) %>%
         mutate(
            FACI   = dx_faci_id,
            SUB_ID = dx_sub_faci_id,
         ) %>%
         ohasis$get_faci(
            list(ACTUAL_FACI_CODE = c("dx_faci_id", "dx_sub_faci_id")),
            "code"
         ) %>%
         mutate(
            ACTUAL_BRANCH = ACTUAL_FACI_CODE,
         ) %>%
         mutate(
            across(
               names(select(., ends_with("_BRANCH", ignore.case = FALSE))),
               ~if_else(nchar(.) > 3, ., NA_character_)
            )
         ) %>%
         mutate_at(
            .vars = vars(ends_with("_FACI_CODE", ignore.case = FALSE)),
            ~case_when(
               str_detect(., "^TLY") ~ "TLY",
               str_detect(., "^SHIP") ~ "SHP",
               str_detect(., "^SAIL") ~ "SAIL",
               TRUE ~ .
            )
         ) %>%
         mutate(
            ACTUAL_BRANCH = case_when(
               ACTUAL_FACI_CODE == "TLY" & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
               ACTUAL_FACI_CODE == "SHP" & is.na(ACTUAL_BRANCH) ~ "SHIP-MAKATI",
               TRUE ~ coalesce(ACTUAL_BRANCH, "")
            ),
            enroll_same   = if_else(realhub == ACTUAL_FACI_CODE &
                                       realhub_branch == ACTUAL_BRANCH &
                                       everonart == 1 &
                                       plhiv == 1, 1, 0, 0),
            enroll_other  = if_else(enroll_same == 0 &
                                       outcome != "abroad" &
                                       everonart == 1 &
                                       plhiv == 1, 1, 0, 0),
            enroll_ocw    = if_else(enroll_same == 0 &
                                       outcome == "abroad" &
                                       everonart == 1 &
                                       plhiv == 1, 1, 0, 0),
            # prev_same_onart  = if_else(enroll_same == 1 & outcome == "onart", 1, 0, 0),
            # prev_same_ltfu   = if_else(enroll_same == 1 & outcome == "ltfu", 1, 0, 0),
            # prev_same_refused = if_else(enroll_same == 1 & outcome == "refused", 1, 0, 0),
         ) %>%
         select(
            tx_faci_id     = FACI,
            tx_sub_faci_id = SUB_ID,
            enroll_same,
            enroll_other,
            enroll_ocw,
            # prev_same_onart,
            # prev_same_ltfu,
            # prev_same_refused
         ) %>%
         group_by(tx_faci_id, tx_sub_faci_id) %>%
         summarise(
            enroll_same_prev  = sum(enroll_same),
            enroll_other_prev = sum(enroll_other),
            enroll_ocw_prev   = sum(enroll_ocw),
            # prev_same_onart   = sum(prev_same_onart),
            # prev_same_ltfu    = sum(prev_same_ltfu),
            # prev_same_refused  = sum(prev_same_refused),
         ) %>%
         ungroup(),
      by = join_by(tx_faci_id, tx_sub_faci_id)
   ) %>%
   left_join(
      y  = aem$harp$dx$aug %>%
         mutate(
            mortality  = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv      = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
            plhiv_dead = if_else(!is.na(idnum) & coalesce(dead, 0) == 1, 1, 0, 0),
            new        = if_else(year == 2023 & month == 8 & plhiv == 1, 1, 0, 0),
         ) %>%
         select(
            year,
            month,
            plhiv,
            plhiv_dead,
            tx_faci_id     = dx_faci_id,
            tx_sub_faci_id = dx_sub_faci_id,
            everonart
         ) %>%
         group_by(tx_faci_id, tx_sub_faci_id) %>%
         summarise(
            dx_curr       = n(),
            plhiv_curr    = sum(plhiv),
            newplhiv_curr = sum(if_else(plhiv == 1 & year == 2023 & month == 8, 1, 0, 0)),
            dead_curr     = sum(plhiv_dead),
            notstart_curr = sum(if_else(plhiv == 1 & is.na(everonart), 1, 0, 0))
         ) %>%
         ungroup(),
      by = join_by(tx_faci_id, tx_sub_faci_id)
   ) %>%
   left_join(
      y  = aem$harp$dx$aug %>%
         mutate(
            mortality = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv     = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
         ) %>%
         filter(plhiv == 1) %>%
         mutate(
            FACI   = dx_faci_id,
            SUB_ID = dx_sub_faci_id,
         ) %>%
         ohasis$get_faci(
            list(ACTUAL_FACI_CODE = c("dx_faci_id", "dx_sub_faci_id")),
            "code"
         ) %>%
         mutate(
            ACTUAL_BRANCH = ACTUAL_FACI_CODE,
         ) %>%
         mutate(
            across(
               names(select(., ends_with("_BRANCH", ignore.case = FALSE))),
               ~if_else(nchar(.) > 3, ., NA_character_)
            )
         ) %>%
         mutate_at(
            .vars = vars(ends_with("_FACI_CODE", ignore.case = FALSE)),
            ~case_when(
               str_detect(., "^TLY") ~ "TLY",
               str_detect(., "^SHIP") ~ "SHP",
               str_detect(., "^SAIL") ~ "SAIL",
               TRUE ~ .
            )
         ) %>%
         mutate(
            ACTUAL_BRANCH = case_when(
               ACTUAL_FACI_CODE == "TLY" & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
               ACTUAL_FACI_CODE == "SHP" & is.na(ACTUAL_BRANCH) ~ "SHIP-MAKATI",
               TRUE ~ coalesce(ACTUAL_BRANCH, "")
            ),
            enroll_same   = if_else(realhub == ACTUAL_FACI_CODE &
                                       realhub_branch == ACTUAL_BRANCH &
                                       everonart == 1 &
                                       plhiv == 1, 1, 0, 0),
            enroll_other  = if_else(enroll_same == 0 &
                                       outcome != "abroad" &
                                       everonart == 1 &
                                       plhiv == 1, 1, 0, 0),
            enroll_ocw    = if_else(enroll_same == 0 &
                                       outcome == "abroad" &
                                       everonart == 1 &
                                       plhiv == 1, 1, 0, 0),
         ) %>%
         select(
            tx_faci_id     = FACI,
            tx_sub_faci_id = SUB_ID,
            enroll_same,
            enroll_other,
            enroll_ocw
         ) %>%
         group_by(tx_faci_id, tx_sub_faci_id) %>%
         summarise(
            enroll_same_curr  = sum(enroll_same),
            enroll_other_curr = sum(enroll_other),
            enroll_ocw_curr   = sum(enroll_ocw),
         ) %>%
         ungroup(),
      by = join_by(tx_faci_id, tx_sub_faci_id)
   ) %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(coalesce(floor(.), 0))
   ) %>%
   mutate(
      FACI   = tx_faci_id,
      SUB_ID = tx_sub_faci_id,
   ) %>%
   ohasis$get_faci(
      list(hub_name = c("FACI", "SUB_ID")),
      "name"
   ) %>%
   ohasis$get_faci(
      list(tx_faci = c("tx_faci_id", "tx_sub_faci_id")),
      "code",
      c("region", "province", "muncity")
   ) %>%
   ungroup() %>%
   mutate(
      total_prev          = (`prev_onart` +
         `prev_ltfu` +
         `prev_dead` +
         `prev_refused` +
         `prev_abroad`
      ),
      enrollcoverage_prev = ((enroll_same_prev + enroll_other_prev) - notstart_prev) / (plhiv_prev - enroll_ocw_prev),
      enrollgap_prev      = notstart_prev,
      txcoverage_prev     = `prev_onart` / total_prev,
      txgap_prev          = total_prev -
         `prev_abroad` -
         `prev_onart`,
      refer_prev          = (`prev_onart` +
         `prev_ltfu` +
         `prev_refused`) - enroll_same_prev,
      ltfurate_prev       = (`prev_ltfu` + `prev_refused`) / total_prev,

      newdead_curr        = dead_curr - dead_prev,
      total_curr          = `curr_newonart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_onart` +
         `curr_ml` +
         `curr_ltfu` +
         `curr_dead` +
         `curr_refused` +
         `curr_abroad`,
      enrollcoverage_curr = ((enroll_same_curr + enroll_other_curr) - notstart_curr) / (plhiv_curr - enroll_ocw_curr),
      enrollgap_curr      = notstart_curr,
      `curr_onart`        = `curr_newonart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_onart`,
      `prev_ltfu`         = `curr_ml` +
         `curr_ltfu`,
      txcoverage_curr     = `curr_onart` / total_curr,
      txgap_curr          = total_curr -
         `curr_abroad` -
         `curr_onart`,
      refer_curr          = (`curr_onart` +
         `curr_ltfu` +
         `curr_refused`) - enroll_same_curr,
      ltfurate_curr       = (`curr_ltfu` + `curr_refused`) / total_curr,

      # txcoverage_curr = (`curr_newonart` +
      #    `curr_retained` +
      #    `curr_rtt` +
      #    `curr_onart`) / total_curr,
      # ltfu_curr       = `curr_ml` + `curr_ltfu`,
      # ltfurate_curr   = ltfu_curr / total_curr,
   ) %>%
   select(
      region,
      province,
      muncity,
      hub_name,
      dx_prev,
      plhiv_prev,
      dead_prev,
      notstart_prev,
      enroll_same_prev,
      enroll_other_prev,
      enroll_ocw_prev,
      enrollcoverage_prev,
      enrollgap_prev,
      prev_onart,
      prev_ltfu,
      prev_refused,
      refer_prev,
      txplhiv_prev,
      txcoverage_prev,
      ltfurate_prev,
      txgap_prev,
      newplhiv_curr,
      newdead_curr,
      dx_curr,
      plhiv_curr,
      dead_curr,
      notstart_curr,
      enroll_same_curr,
      enroll_other_curr,
      enroll_ocw_curr,
      enrollcoverage_curr,
      enrollgap_curr,
      `curr_newonart`,
      `curr_retained`,
      `curr_rtt`,
      `curr_ml`,
      # refer_curr,
      `curr_ltfu`,
      `curr_refused`,
      txplhiv_curr,
      txcoverage_curr,
      ltfurate_curr,
      txgap_curr,
   ) %>%
   mutate(
      reg_order = region,
      reg_order = case_when(
         reg_order == "1" ~ 1,
         reg_order == "2" ~ 2,
         reg_order == "CAR" ~ 3,
         reg_order == "3" ~ 4,
         reg_order == "NCR" ~ 5,
         reg_order == "4A" ~ 6,
         reg_order == "4B" ~ 7,
         reg_order == "5" ~ 8,
         reg_order == "6" ~ 9,
         reg_order == "7" ~ 10,
         reg_order == "8" ~ 11,
         reg_order == "9" ~ 12,
         reg_order == "10" ~ 13,
         reg_order == "11" ~ 14,
         reg_order == "12" ~ 15,
         reg_order == "CARAGA" ~ 16,
         reg_order == "ARMM" ~ 17,
         reg_order == "BARMM" ~ 17,
         TRUE ~ 9999
      ),
   )

write_clip(sum_dx)
write_clip(sum_tx)
write_clip(sum_txfaci %>%
              arrange(reg_order, province, hub_name) %>%
              select(-reg_order))