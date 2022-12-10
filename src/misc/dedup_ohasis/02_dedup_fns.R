ohasis_dupes <- function(group_ids) {
   # tag duplicates based on grouping
   Dup.Duplicates <- dedup$standard %>%
      filter_at(
         .vars           = vars({{group_ids}}),
         .vars_predicate = all_vars(!is.na(.))
      ) %>%
      get_dupes({{group_ids}}) %>%
      filter(dupe_count > 0) %>%
      left_join(
         # * merge w/ registry to see which Central ID to keep for those matched
         y  = dedup$dx %>%
            select(
               CENTRAL_ID,
               idnum
            ),
         by = 'CENTRAL_ID'
      ) %>%
      left_join(
         y  = dedup$id_registry %>%
            group_by(CENTRAL_ID) %>%
            summarise(
               NUM_LINKED = n()
            ) %>%
            ungroup(),
         by = 'CENTRAL_ID'
      ) %>%
      mutate(
         # tag those in registry
         harp_registry = if_else(!is.na(idnum), 1, 0)
      ) %>%
      group_by(across({{group_ids}})) %>%
      mutate(
         # generate a group id to identify groups of duplicates
         group_id    = cur_group_id(),

         # tag groups who do and do not have registry central ids in the dup group
         harpgrp_tag = max(harp_registry),
         notgrp_tag  = min(harp_registry),
      ) %>%
      ungroup() %>%
      mutate(
         reg_tag = harpgrp_tag + notgrp_tag,
      ) %>%
      arrange(group_id, idnum)

   Dup.Duplicates.Registry <- Dup.Duplicates %>% filter(reg_tag == 1)
   Dup.Duplicates.Within   <- Dup.Duplicates %>% filter(reg_tag == 2)
   Dup.Duplicates.Normal   <- Dup.Duplicates %>% filter(reg_tag == 0)

   Dup.Duplicates.Registry.Final       <- data.frame()
   Dup.Duplicates.Registry.Final.Conso <- list()
   if (nrow(Dup.Duplicates.Registry) > 0) {
      Dup.Duplicates.Registry.Final <- Dup.Duplicates.Registry %>%
         arrange(group_id, desc(harp_registry)) %>%
         group_by(group_id) %>%
         mutate(
            FINAL_CID = first(na.omit(CENTRAL_ID)),
            CID_NUM   = row_number(),
         ) %>%
         ungroup() %>%
         pivot_wider(
            id_cols     = FINAL_CID,
            names_from  = CID_NUM,
            names_glue  = 'CID_{CID_NUM}',
            values_from = CENTRAL_ID
         ) %>%
         select(-CID_1)

      for (name in get_names(Dup.Duplicates.Registry.Final, "CID_")) {
         Dup.Duplicates.Registry.Final.Conso[[name]] <- Dup.Duplicates.Registry.Final %>%
            select(
               FINAL_CID,
               LINK_CID = !!as.symbol(name)
            ) %>%
            filter(!is.na(LINK_CID))
      }
      Dup.Duplicates.Registry.Final.Conso <- bind_rows(Dup.Duplicates.Registry.Final.Conso)
   }

   Dup.Duplicates.Normal.Final       <- data.frame()
   Dup.Duplicates.Normal.Final.Conso <- list()
   if (nrow(Dup.Duplicates.Normal) > 0) {
      Dup.Duplicates.Normal.Final <- Dup.Duplicates.Normal %>%
         arrange(group_id) %>%
         group_by(group_id) %>%
         mutate(
            FINAL_CID = first(na.omit(CENTRAL_ID)),
            CID_NUM   = row_number(),
         ) %>%
         ungroup() %>%
         pivot_wider(
            id_cols     = FINAL_CID,
            names_from  = CID_NUM,
            names_glue  = 'CID_{CID_NUM}',
            values_from = CENTRAL_ID
         ) %>%
         select(-CID_1)

      for (name in get_names(Dup.Duplicates.Normal.Final, "CID_")) {
         Dup.Duplicates.Normal.Final.Conso[[name]] <- Dup.Duplicates.Normal.Final %>%
            select(
               FINAL_CID,
               LINK_CID = !!as.symbol(name)
            ) %>%
            filter(!is.na(LINK_CID))
      }
      Dup.Duplicates.Normal.Final.Conso <- bind_rows(Dup.Duplicates.Normal.Final.Conso)
   }

   reg_pairs  <- n_groups(Dup.Duplicates.Registry %>% group_by(across({{group_ids}})))
   norm_pairs <- n_groups(Dup.Duplicates.Normal %>% group_by(across({{group_ids}})))
   cat(
      crayon::blue("Remaining Unmatched:"),
      crayon::underline(crayon::magenta(
         nrow(
            dedup$standard %>%
               select(CENTRAL_ID) %>%
               anti_join(dedup$id_registry))
      )
      ),
      "rows \n"
   )
   cat(
      crayon::blue("Registry Clients:"),
      crayon::underline(crayon::magenta(nrow(Dup.Duplicates.Registry))),
      "rows |",
      crayon::underline(crayon::magenta(reg_pairs)),
      "pairs\n"
   )
   cat(
      crayon::blue("Normal Clients:"),
      crayon::underline(crayon::magenta(nrow(Dup.Duplicates.Normal))),
      "rows |",
      crayon::underline(crayon::magenta(norm_pairs)),
      "pairs\n"
   )

   data_list <- list(
      "registry"       = Dup.Duplicates.Registry,
      "registry_pivot" = Dup.Duplicates.Registry.Final,
      "registry_up"    = Dup.Duplicates.Registry.Final.Conso,
      "normal"         = Dup.Duplicates.Normal,
      "normal_pivot"   = Dup.Duplicates.Normal.Final,
      "normal_up"      = Dup.Duplicates.Normal.Final.Conso,
      "within"         = Dup.Duplicates.Within
   )
}