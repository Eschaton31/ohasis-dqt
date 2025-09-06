ohasis_dupes <- function(...) {
   # tag duplicates based on grouping
   Dup.Duplicates <- dedup$standard %>%
      select(row_id, ...) %>%
      filter(if_all(c(...), ~!is.na(.))) %>%
      group_by(...) %>%
      mutate(
         # generate a group id to identify groups of duplicates
         group_id   = cur_group_id(),
         dupe_count = n(),
      ) %>%
      ungroup() %>%
      filter(dupe_count > 1) %>%
      inner_join(dedup$standard %>% select(-c(...)), by = join_by(row_id)) %>%
      # merge w/ registry to see which Central ID to keep for those matched
      left_join(dedup$dx %>% select(central_id, idnum), join_by(central_id)) %>%
      left_join(dedup$num_linked, join_by(central_id)) %>%
      mutate(
         # tag those in registry
         harp_registry = if_else(!is.na(idnum), 1, 0)
      ) %>%
      group_by(group_id) %>%
      mutate(
         # tag groups who do and do not have registry central ids in the dup group
         harpgrp_tag = max(harp_registry),
         notgrp_tag  = min(harp_registry),
      ) %>%
      ungroup() %>%
      mutate(
         reg_tag = harpgrp_tag + notgrp_tag,
      ) %>%
      arrange(group_id, idnum) %>%
      relocate(central_id, group_id, row_id, .before = 1)

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
            final_cid = first(na.omit(central_id)),
            cid_num   = row_number(),
         ) %>%
         ungroup() %>%
         pivot_wider(
            id_cols     = final_cid,
            names_from  = cid_num,
            names_glue  = 'cid_{cid_num}',
            values_from = central_id
         ) %>%
         select(-cid_1)

      for (name in get_names(Dup.Duplicates.Registry.Final, "cid_")) {
         Dup.Duplicates.Registry.Final.Conso[[name]] <- Dup.Duplicates.Registry.Final %>%
            select(
               final_cid,
               link_cid = !!as.symbol(name)
            ) %>%
            filter(!is.na(link_cid))
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
            final_cid = first(na.omit(central_id)),
            cid_num   = row_number(),
         ) %>%
         ungroup() %>%
         pivot_wider(
            id_cols     = final_cid,
            names_from  = cid_num,
            names_glue  = 'cid_{cid_num}',
            values_from = central_id
         ) %>%
         select(-cid_1)

      for (name in get_names(Dup.Duplicates.Normal.Final, "cid_")) {
         Dup.Duplicates.Normal.Final.Conso[[name]] <- Dup.Duplicates.Normal.Final %>%
            select(
               final_cid,
               link_cid = !!as.symbol(name)
            ) %>%
            filter(!is.na(link_cid))
      }
      Dup.Duplicates.Normal.Final.Conso <- bind_rows(Dup.Duplicates.Normal.Final.Conso)
   }

   reg_pairs  <- n_groups(Dup.Duplicates.Registry %>% group_by(group_id))
   norm_pairs <- n_groups(Dup.Duplicates.Normal %>% group_by(group_id))
   cat(
      crayon::blue("Remaining Unmatched:"),
      crayon::underline(crayon::magenta(
         nrow(
            dedup$standard %>%
               select(central_id) %>%
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