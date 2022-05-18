##  Process contests (electoral posts) -----------------------------------------

# list of current vars for code cleanup
clear_env()
currEnv <- ls()[ls() != "currEnv"]

wd         <- "C:/Users/johnb/Downloads/elections_ph_2022/data/contests"
json_files <- list.files(wd, "*.json", full.names = TRUE)

contests            <- list()
contests$posts      <- data.frame()
contests$candidates <- data.frame()

pb <- progress_bar$new(format = ":current of :total .json files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = length(json_files), width = 100, clear = FALSE)
pb$tick(0)

# iterate over all available json files
lapply(json_files, function(json) {

   # read data as list
   candidate <- jsonlite::read_json(json)

   # combine all dataframes available inside json
   data  <- bind_rows(candidate$bos)
   posts <- data.frame(json = json)

   # add metadata contest
   for (var in names(candidate))
      if (!is.list(candidate[[var]])) {
         data[var]  <- candidate[[var]]
         posts[var] <- candidate[[var]]
      }

   # append to overall dataframe
   .GlobalEnv$contests$posts      <- bind_rows(.GlobalEnv$contests$posts, posts)
   .GlobalEnv$contests$candidates <- bind_rows(.GlobalEnv$contests$candidates, data)
   pb$tick(1)
})


# final candidates list
contests$candidates %<>%
   distinct_all() %>%
   arrange(pre, ccc, to) %>%
   select(
      ballot_order   = boc,
      candidate      = bon,
      ticket_no      = to,
      partylist_code = pc,
      partylist_name = pn,
      contest_code   = cc,
      contest_name   = cn,
      contest_cat    = ccn,
      contest_type   = type
   )

# final contests list
contests$posts %<>%
   distinct_all() %>%
   arrange(pre, ccc) %>%
   select(
      contest_code = cc,
      contest_name = cn,
      contest_cat  = ccn,
      contest_type = type
   )

ph22$contests <- contests

##  Process Certificate of Canvas ----------------------------------------------

wd         <- "C:/Users/johnb/Downloads/elections_ph_2022/data/results"
json_files <- list.files(wd, "*.json", full.names = TRUE, recursive = TRUE)
json_info  <- json_files[stri_detect_fixed(json_files, "info.json")]
json_vote  <- json_files[!stri_detect_fixed(json_files, "info.json")]

counts      <- list()
counts$coc  <- list()
counts$info <- list()
for (i in 1:5)
   counts$info[[i]] <- list()

# iterate over all info files to get precinct data
pb <- progress_bar$new(format = ":current of :total .json files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = length(json_info), width = 100, clear = FALSE)
pb$tick(0)
lapply(json_info, function(json) {

   # read data as list
   canvas <- jsonlite::read_json(json)
   level  <- canvas$cl %>% as.numeric()
   poll   <- canvas$rcc %>% as.character()

   if (level == 2) {
      polling_precinct <- bind_rows(canvas$srs) %>%
         select(
            prov_code = rcc,
            province  = rn
         ) %>%
         mutate(
            region  = canvas$rn,
            .before = 1
         )
   }

   if (level > 2 | level == 1) {
      polling_stations <- bind_rows(canvas$pps)
      precincts        <- bind_rows(polling_stations$vbs) %>%
         select(-matches("cs")) %>%
         distinct_all()

      if (sum(dim(polling_stations)) > 0) {
         polling_precinct <- polling_stations %>%
            select(-matches("vbs")) %>%
            left_join(
               y  = precincts %>% rename(ppcc = pre, ppc = vbc),
               by = c("ppcc", "ppc")
            ) %>%
            mutate(
               geo_code   = canvas$rc,
               area_code  = canvas$rcc,
               area_name  = canvas$rn,
               area_level = canvas$cl,
               .before    = 1
            ) %>%
            rename_all(
               ~case_when(
                  . == "ppc" ~ "polling_place_code",
                  . == "ppn" ~ "polling_place",
                  . == "ppcc" ~ "precinct_code",
                  . == "cpre" ~ "precincts",
                  . == "url" ~ "comelec_asset",
                  . == "type" ~ "stn_type",
                  TRUE ~ .
               )
            )

      }
   }

   # append to overall dataframe
   if (exists("polling_precinct"))
      .GlobalEnv$counts$info[[level]][[poll]] <- polling_precinct

   pb$tick(1)
})

# combine info data
for (i in 1:5)
   counts$info[[i]] <- bind_rows(counts$info[[i]])

counts$info$brgy <- counts$info[[2]] %>%
   select(
      region,
      province,
      prov_code
   ) %>%
   right_join(
      y  = counts$info[[3]] %>%
         select(
            prov_code = area_code,
            pboc      = polling_place
         ),
      by = "prov_code"
   ) %>%
   right_join(
      y  = counts$info[[4]] %>%
         mutate(
            prov_code = stri_pad_right(StrLeft(area_code, 2), 4, "0")
         ) %>%
         select(
            muncity   = area_name,
            munc_code = area_code,
            mboc      = polling_place,
            prov_code
         ),
      by = "prov_code"
   ) %>%
   right_join(
      y  = counts$info[[5]] %>%
         mutate(
            munc_code = StrLeft(area_code, 4)
         ) %>%
         rename(
            brgy = area_name
         ),
      by = "munc_code"
   )

counts$info$munc <- counts$info[[2]] %>%
   select(
      region,
      province,
      prov_code
   ) %>%
   right_join(
      y  = counts$info[[3]] %>%
         select(
            prov_code = area_code,
            pboc      = polling_place
         ),
      by = "prov_code"
   ) %>%
   right_join(
      y  = counts$info[[4]] %>%
         mutate(
            prov_code = stri_pad_right(StrLeft(area_code, 2), 4, "0")
         ) %>%
         rename(
            muncity   = area_name,
            munc_code = area_code,
            mboc      = polling_place
         ),
      by = "prov_code"
   ) %>%
   distinct_all()

counts$info$prov <- counts$info[[2]] %>%
   select(
      region,
      province,
      prov_code
   ) %>%
   right_join(
      y  = counts$info[[3]] %>%
         rename(
            prov_code = area_code,
            pboc      = polling_place
         ),
      by = "prov_code"
   )

posts_file <- "C:/Users/johnb/Downloads/elections_ph_2022/posts.RDS"
saveRDS(contests$posts, posts_file)
candidates_file <- "C:/Users/johnb/Downloads/elections_ph_2022/candidates.RDS"
saveRDS("C:/Users/johnb/Downloads/elections_ph_2022/candidates.RDS", candidates_file)

done <- json_vote %>%
   as.data.frame() %>%
   rename(file = 1) %>%
   slice(0)
saveRDS(done, "C:/Users/johnb/Downloads/elections_ph_2022/done.RDS")

# iterate over all available json files
for_process <- setdiff(json_vote, readRDS("C:/Users/johnb/Downloads/elections_ph_2022/done.RDS")$file)
pb          <- progress_bar$new(format = ":current of :total .json files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = length(for_process), width = 100, clear = FALSE)
pb$tick(0)

lapply(for_process, function(json) {

   # read data as list
   canvas <- jsonlite::read_json(json)

   # generate summary
   summary <- bind_rows(canvas$cos)
   if (nrow(summary) > 0)
      summary %<>%
         mutate(
            cn = stri_replace_all_fixed(cn, "-", "_")
         ) %>%
         pivot_wider(
            id_cols     = cc,
            names_from  = cn,
            values_from = ct
         ) %>%
         rename(
            contest_code = cc
         ) %>%
         left_join(
            y  = readRDS("C:/Users/johnb/Downloads/elections_ph_2022/posts.RDS") %>%
               select(contest_code, contest_cat),
            by = "contest_code"
         ) %>%
         mutate(
            polling_place_code = canvas$vbc,
            .before            = 1
         )

   # generate vote count
   votes <- bind_rows(canvas$rs)
   if (nrow(votes) > 0)
      votes %<>%
         rename(
            contest_code = cc,
            ballot_order = bo,
            votes        = v,
            n_total      = tot,
            perc_total   = per
         ) %>%
         left_join(
            y  = readRDS("C:/Users/johnb/Downloads/elections_ph_2022/candidates.RDS") %>%
               select(contest_code, ballot_order, contest_cat, candidate),
            by = c("contest_code", "ballot_order")
         )


   summary_file <- "C:/Users/johnb/Downloads/elections_ph_2022/summary.RDS"
   if (file.exists(summary_file))
      df_summary <- bind_rows(readRDS(summary_file), summary)
   else
      df_summary <- summary

   votes_file <- "C:/Users/johnb/Downloads/elections_ph_2022/votes.RDS"
   if (file.exists(votes_file))
      df_votes <- bind_rows(readRDS(votes_file), votes)
   else
      df_votes <- votes

   saveRDS(df_summary, summary_file)
   saveRDS(df_votes, votes_file)

   done <- readRDS("C:/Users/johnb/Downloads/elections_ph_2022/done.RDS") %>%
      add_row(file = json)

   saveRDS(done, "C:/Users/johnb/Downloads/elections_ph_2022/done.RDS")

   pb$tick(1)
})