aem      <- list()
aem$ref  <- psgc_aem(ohasis$ref_addr)
aem$harp <- list(
   dx  = list(
      jul = read_dta(hs_data("harp_full", "reg", 2023, 7)),
      aug = read_dta(hs_data("harp_full", "reg", 2023, 8))
   ),
   tx  = list(
      jul = read_dta(hs_data("harp_tx", "outcome", 2023, 7)),
      aug = read_dta(hs_data("harp_tx", "outcome", 2023, 8))
   ),
   est = aem$ref$aem %>%
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
sum_dx   <- aem$harp$dx$jul %>%
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
            muncity
         ),
      by = join_by(idnum)
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
                  muncity
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
               newonart == 1 & outcome == "alive on arv" ~ "new onart",
               outcome == "alive on arv" & prev_outcome == "alive on arv" ~ "retained",
               outcome == "alive on arv" & prev_outcome != "alive on arv" ~ "rtt",
               outcome == "lost to follow up" & prev_outcome == "alive on arv" ~ "ml",
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
            mortality = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv     = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
            new       = if_else(year == 2023 & month == 8 & plhiv == 1, 1, 0, 0),
         ) %>%
         select(
            plhiv,
            region,
            province,
            muncity,
            everonart
         ) %>%
         group_by(region, province, muncity) %>%
         summarise(
            plhiv_prev    = sum(plhiv),
            notstart_prev = sum(if_else(plhiv == 1 & is.na(everonart), 1, 0, 0))
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   full_join(
      y  = aem$harp$dx$aug %>%
         mutate(
            mortality = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
            plhiv     = if_else(!is.na(idnum) & coalesce(dead, 0) == 0, 1, 0, 0),
            new       = if_else(year == 2023 & month == 8 & plhiv == 1, 1, 0, 0),
         ) %>%
         select(
            year,
            month,
            plhiv,
            region,
            province,
            muncity,
            everonart
         ) %>%
         group_by(region, province, muncity) %>%
         summarise(
            newplhiv_curr = sum(if_else(plhiv == 1 & year == 2023 & month == 8, 1, 0, 0)),
            plhiv_curr    = sum(plhiv),
            notstart_curr = sum(if_else(plhiv == 1 & is.na(everonart), 1, 0, 0))
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(coalesce(., 0))
   ) %>%
   mutate(
      target_prev      = plhiv_prev * .95,
      txcoverage_prev  = `prev_alive on arv` / plhiv_prev,
      txgap_prev       = target_prev - `prev_alive on arv`,
      target_curr      = plhiv_curr * .95,
      tx_targetmo_prev = txgap_prev / 5,
      txcoverage_curr  = (`curr_new onart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_alive on arv`) / plhiv_curr,
      txgap_curr       = target_curr - (`curr_new onart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_alive on arv`),
      tx_targetmo_curr = txgap_curr / 4,
   ) %>%
   select(
      region,
      province,
      muncity,
      plhiv_prev,
      target_prev,
      notstart_prev,
      `prev_alive on arv`,
      `prev_lost to follow up`,
      # `prev_dead`,
      `prev_stopped - refused`,
      `prev_transout - overseas`,
      txcoverage_prev,
      txgap_prev,
      newplhiv_curr,
      plhiv_curr,
      notstart_curr,
      `curr_new onart`,
      `curr_retained`,
      `curr_rtt`,
      `curr_ml`,
      `curr_lost to follow up`,
      `curr_stopped - refused`,
      `curr_transout - overseas`,
      txcoverage_curr,
      txgap_curr,
      tx_targetmo_curr,
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
               newonart == 1 & outcome == "alive on arv" ~ "new onart",
               outcome == "alive on arv" & prev_outcome == "alive on arv" ~ "retained",
               outcome == "alive on arv" & prev_outcome != "alive on arv" ~ "rtt",
               outcome == "lost to follow up" & prev_outcome == "alive on arv" ~ "ml",
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
      total_prev      = (`prev_alive on arv` +
         `prev_lost to follow up` +
         `prev_dead` +
         `prev_stopped - refused` +
         `prev_transout - overseas`),
      txcoverage_prev = `prev_alive on arv` / total_prev,
      ltfurate_prev   = `prev_lost to follow up` / total_prev,

      total_curr      = `curr_new onart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_alive on arv` +
         `curr_ml` +
         `curr_lost to follow up` +
         `curr_dead` +
         `curr_stopped - refused` +
         `curr_transout - overseas`,
      txcoverage_curr = (`curr_new onart` +
         `curr_retained` +
         `curr_rtt` +
         `curr_alive on arv`) / total_curr,
      ltfu_curr       = `curr_ml` + `curr_lost to follow up`,
      ltfurate_curr   = ltfu_curr / total_curr,
   ) %>%
   select(
      real_reg,
      real_prov,
      real_munc,
      realhub,
      `prev_alive on arv`,
      `prev_lost to follow up`,
      `prev_stopped - refused`,
      `prev_transout - overseas`,
      txcoverage_prev,
      ltfurate_prev,
      `curr_new onart`,
      `curr_retained`,
      `curr_rtt`,
      `curr_ml`,
      `curr_lost to follow up`,
      `curr_stopped - refused`,
      `curr_transout - overseas`,
      txcoverage_curr,
      ltfurate_curr,
      ltfu_curr
   )

write_clip(sum_dx)
write_clip(sum_tx)
write_clip(sum_txfaci)