nhsss$hcr$for_match <- nhsss$hcr$requests %>%
   mutate_at(
      .vars = vars(first, middle, last, suffix),
      ~as.character(.)
   ) %>%
   anti_join(
      y = nhsss$hcr$requestor %>%
         filter(linkage_done == TRUE | results_released == TRUE),
      by = c("faci_request", "date_request")
   ) %>%
   mutate(
      # name
      name = paste0(
         if_else(
            condition = is.na(last),
            true      = "",
            false     = last
         ), ", ",
         if_else(
            condition = is.na(first),
            true      = "",
            false     = first
         ), " ",
         if_else(
            condition = is.na(middle),
            true      = "",
            false     = middle
         ), " ",
         if_else(
            condition = is.na(suffix),
            true      = "",
            false     = suffix
         )
      ),
      name = str_squish(name),
      name = if_else(name == ",", NA_character_, name, name),
      bdate = as.Date(bdate)
   )

reclink_df <- fastLink(
   dfA              = nhsss$hcr$for_match,
   dfB              = nhsss$hcr$harp,
   varnames         = c("name", "bdate"),
   stringdist.match = "name",
   partial.match    = "name",
   cut.a            = 0.90,
   cut.p            = 0.85,
   dedupe.matches   = FALSE,
   n.cores          = 4,
)

if (length(reclink_df$matches$inds.a) > 0) {
   reclink_matched <- getMatches(
      dfA         = nhsss$hcr$for_match,
      dfB         = nhsss$hcr$harp,
      fl.out      = reclink_df,
      combine.dfs = FALSE
   )

   reclink_review <- reclink_matched$dfA.match %>%
      mutate(
         MATCH_ID = row_number()
      ) %>%
      select(
         MATCH_ID,
         faci_request,
         date_request,
         request_num,
         idnum,
         labcode,
         confirm_date,
         first,
         middle,
         last,
         suffix,
         uic,
         bdate,
         age,
         sex,
         dxlab_standard,
         row_id,
         cell_idnum,
         cell_labcode,
         name,
         posterior
      ) %>%
      left_join(
         y  = reclink_matched$dfB.match %>%
            mutate(
               MATCH_ID = row_number()
            ) %>%
            select(
               MATCH_ID,
               harp_idnum          = idnum,
               harp_labcode        = labcode2,
               harp_name           = name,
               harp_uic            = uic,
               harp_bdate          = bdate,
               harp_sex            = sex,
               harp_dxlab_standard = dxlab_standard,
               posterior
            ),
         by = "MATCH_ID"
      ) %>%
      select(-posterior.y) %>%
      rename(posterior = posterior.x) %>%
      arrange(desc(posterior)) %>%
      relocate(posterior, .before = MATCH_ID) %>%
      # Additional sift through of matches
      mutate(
         # levenshtein
         LV       = stringdist::stringsim(name, harp_name, method = 'lv'),
         # jaro-winkler
         JW       = stringdist::stringsim(name, harp_name, method = 'jw'),
         # qgram
         QGRAM    = stringdist::stringsim(name, harp_name, method = 'qgram', q = 3),
         AVG_DIST = (LV + QGRAM + JW) / 3,
      ) %>%
      # choose 60% and above match
      filter(AVG_DIST >= 0.60, !is.na(posterior), bdate == harp_bdate) %>%
      distinct(row_id, .keep_all = TRUE) %>%
      arrange(row_id)
}
