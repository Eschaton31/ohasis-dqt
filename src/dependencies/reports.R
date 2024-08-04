base64_img_uri <- function(file) {
   uri <- knitr::image_uri(file)
   uri <- sprintf("<img src=\"%s\" />\n", uri)
   return(uri)
}

template_replace <- function(template, key_val) {
   keys <- names(key_val)
   vals <- key_val

   pairs <- length(key_val)
   for (i in 1:pairs) {
      template <- stri_replace_all_fixed(template, stri_c("{{", keys[[i]], "}}"), vals[[i]])
   }

   return(template)
}

waterfall_linelist <- function(start, end) {
   sites    <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", col_types = "c", .name_repair = "unique_quiet")
   min_date <- as.Date(start) %m-% days(1)
   min_mo   <- month(min_date)
   min_yr   <- year(min_date)

   max_date <- as.Date(end)
   max_mo   <- month(max_date)
   max_yr   <- year(max_date)

   cols <- c("art_id", "outcome", "artstart_date", "real_reg", "realhub", "realhub_branch")

   old_outcome <- hs_data("harp_tx", "outcome", min_yr, min_mo) %>%
      read_dta(col_select = any_of(cols)) %>%
      mutate(
         realhub        = if (max_yr <= 2022) hub else realhub,
         realhub_branch = if (max_yr <= 2022) NA_character_ else realhub_branch,
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
      ) %>%
      left_join(
         sites %>%
            distinct(FACI_ID, .keep_all = TRUE) %>%
            select(
               FACI_ID,
               site_epic_2023,
               site_icap_2023
            ),
         join_by(FACI_ID)
      ) %>%
      ohasis$get_faci(
         list(site_name = c("FACI_ID", "SUB_FACI_ID")),
         "name",
      )

   new_outcome <- hs_data("harp_tx", "outcome", max_yr, max_mo) %>%
      read_dta(col_select = any_of(cols)) %>%
      mutate(
         realhub        = if (max_yr <= 2022) hub else realhub,
         realhub_branch = if (max_yr <= 2022) NA_character_ else realhub_branch,
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
      ) %>%
      left_join(
         sites %>%
            distinct(FACI_ID, .keep_all = TRUE) %>%
            select(
               FACI_ID,
               site_epic_2023,
               site_icap_2023
            ),
         join_by(FACI_ID)
      ) %>%
      ohasis$get_faci(
         list(site_name = c("FACI_ID", "SUB_FACI_ID")),
         "name",
      )

   new_outcome %<>%
      select(-any_of("prev_outcome")) %>%
      mutate(
         new = 1
      ) %>%
      left_join(
         y  = old_outcome %>%
            select(
               art_id,
               prev_outcome = outcome,
            ) %>%
            mutate(
               old = 1
            ),
         by = join_by(art_id)
      ) %>%
      mutate(
         newonart = if_else(artstart_date > min_date, 1, 0, 0)
      )

   return(list(old = old_outcome, new = new_outcome))
}

waterfall_aggregate <- function(old_outcome, new_outcome, title) {
   old_onart  <- old_outcome %>%
      filter(outcome == "alive on arv") %>%
      nrow()
   new_enroll <- new_outcome %>%
      filter(newonart == 1) %>%
      nrow()
   rtt        <- new_outcome %>%
      filter(prev_outcome == "lost to follow up", outcome == "alive on arv") %>%
      nrow()
   resurrect  <- new_outcome %>%
      filter(prev_outcome == "dead", outcome == "alive on arv") %>%
      nrow()
   late_onart <- new_outcome %>%
      filter(is.na(prev_outcome), outcome == "alive on arv", newonart == 0) %>%
      nrow()

   potential_onart <- old_onart +
      new_enroll +
      rtt +
      resurrect +
      late_onart

   new_stop    <- new_outcome %>%
      filter(str_detect(outcome, "stopped"), prev_outcome == "alive on arv") %>%
      nrow()
   new_dead    <- new_outcome %>%
      filter(outcome == "dead", prev_outcome == "alive on arv") %>%
      nrow()
   new_ml      <- new_outcome %>%
      filter(outcome == "lost to follow up", prev_outcome == "alive on arv") %>%
      nrow()
   ml_enroll   <- new_outcome %>%
      filter(newonart == 1, coalesce(outcome, "") != "alive on arv") %>%
      nrow()
   drop_dupe   <- old_outcome %>%
      filter(outcome == "alive on arv") %>%
      anti_join(new_outcome, join_by(art_id)) %>%
      nrow()
   new_onart   <- new_outcome %>%
      filter(outcome == "alive on arv") %>%
      nrow()
   final_onart <- potential_onart -
      new_stop -
      new_dead -
      new_ml -
      ml_enroll -
      drop_dupe

   if (new_onart != final_onart) {
      log_warn("Mismatch for final onart!")
      log_info(r"(Actual = {green(new_onart)})")
      log_info(r"(Calculated = {green(final_onart)})")
   }

   net_diff <- final_onart - old_onart

   group   <- c(
      "Previously\nAlive",
      "New\nEnrollees",
      "RTT",
      "Resurrect",
      "Late\nReports",
      "Potential\nOn ART",
      "Stopped",
      "Expired",
      "New ML",
      "Expired\nEnrollees",
      "No Longer\nin Scope",
      "Currently\nAlive",
      "Net +/-"
   )
   value   <- c(
      old_onart,
      new_enroll,
      rtt,
      resurrect,
      late_onart,
      potential_onart,
      new_stop * -1,
      new_dead * -1,
      new_ml * -1,
      ml_enroll * -1,
      drop_dupe * -1,
      final_onart,
      net_diff * -1
   )
   measure <- c(
      "total",
      "pos",
      "pos",
      "pos",
      "pos",
      "total",
      "neg",
      "neg",
      "neg",
      "neg",
      "neg",
      "total",
      ifelse(net_diff < 0, "neg", "pos")
   )

   offset  <- ifelse(potential_onart <= 20, floor((potential_onart * .2)), floor((potential_onart * .1)))
   offset  <- ifelse(offset < 10, 10, offset)
   max_y   <- potential_onart + offset
   lim     <- c(0, max_y)
   data_wf <- tibble(label = factor(group, levels = group), value, measure) %>%
      mutate(
         start     = case_when(
            row_number() == 1 ~ 0,
            measure == "total" ~ 0,
            lag(measure) == "total" ~ lag(value),
         ),
         start     = if_else(is.na(start), lag(start) + lag(value), start, start),
         start     = if_else(is.na(start), lag(start) + lag(value), start, start),
         start     = if_else(is.na(start), lag(start) + lag(value), start, start),
         start     = if_else(is.na(start), lag(start) + lag(value), start, start),
         end       = start + value,
         id        = row_number(),
         new_value = if_else(row_number() == n(), value * -1, value)
      )

   plot_wf <- ggplot(data_wf, aes(label, fill = measure, label = scales::comma(new_value))) +
      suppress_warnings(geom_rect(
         aes(
            x    = label,
            xmin = id - 0.45,
            xmax = id + 0.45,
            ymin = end,
            ymax = start
         )
      ), "Ignoring unknown") +
      geom_segment(
         aes(
            x    = id - .45,
            xend = ifelse(id == last(id), id + .45, id + 1.45),
            y    = end,
            yend = end
         ),
         colour = "black"
      ) +
      scale_fill_manual(values = c('#ee8d8b', '#85e89d', '#48787f')) +
      scale_y_continuous(expand = c(0, 0), limits = lim, label = scales::comma) +
      geom_text(
         aes(y = pmax(start, end)),
         vjust    = -0.75,
         family   = "Inter",
         size     = 4,
         fontface = "bold"
      ) +
      xlab("") +
      ylab("") +
      theme_bw() +
      theme(
         panel.border     = element_blank(),
         panel.grid.major = element_blank(),
         panel.grid.minor = element_blank(),
         legend.position  = "none",
         text             = element_text(size = 11, family = "Inter"),
         axis.text        = element_text(size = 12, family = "Inter"),
         axis.text.x      = element_text(vjust = 1, face = 'bold'),
         axis.ticks.x     = element_blank(),
         plot.title       = element_text(size = 18, family = 'Inter', hjust = .5, vjust = 0, face = 'bold'),
      ) +
      geom_hline(yintercept = 0)

   if (!is.null(title))
      plot_wf <- plot_wf + ggtitle(stri_c("Treatment Waterfall: ", title))

   return(plot_wf)
}