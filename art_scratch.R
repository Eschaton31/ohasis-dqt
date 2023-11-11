library(showtext)
library(scales)
font_add_google("Inter", family = "Inter")
showtext_auto()

cols <- c("art_id", "outcome", "artstart_date", "real_reg", "realhub", "realhub_branch")
old  <- read_dta(hs_data("harp_tx", "outcome", 2023, 7), col_select = any_of(cols))
new  <- read_dta(hs_data("harp_tx", "outcome", 2023, 8), col_select = any_of(cols))

old <- read_dta(hs_data("harp_tx", "outcome", 2023, 8), col_select = any_of(cols))
new <- harp_tx$official$new_outcome %>% select(any_of(cols))
data    <- list()
regions <- unique(new$real_reg)
for (reg in regions) {
   data[[reg]] <- list(
      old = old %>% filter(real_reg == reg),
      new = new %>% filter(real_reg == reg)
   )
}

plot_bmp <- function(plot, file) {
   bmp(file, width = 10, height = 7, units = "in", res = 200)
   print(plot)
   dev.off()
}

waterfall_tx <- function(new, old, yr, mo, title) {
   old_outcome <- old
   new_outcome <- new

   params   <- list()
   max_date <- as.Date(end_ym(yr, mo))

   params$yr  <- year(max_date)
   params$mo  <- month(max_date)
   params$ym  <- str_c(sep = ".", params$yr, stri_pad_left(params$mo, 2, "0"))
   params$min <- max_date %m-% months(1) %>% as.character()
   params$max <- max

   prev_mo <- month(max_date %m-% months(1))
   prev_yr <- year(max_date %m-% months(1))

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
         newonart = if_else(year(artstart_date) == as.numeric(yr) & month(artstart_date) == as.numeric(mo), 1, 0, 0)
      )

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

   max_y   <- potential_onart + floor((potential_onart * .1))
   lim     <- c(0, max_y)
   data_wf <- tibble(label = factor(group, levels = group), value, measure)
   plot_wf <- data_wf %>%
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
      ) %>%
      ggplot(aes(label, fill = measure, label = scales::comma(new_value))) +
      geom_rect(aes(x = label, xmin = id - 0.45, xmax = id + 0.45, ymin = end, ymax = start)) +
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
      scale_y_continuous(expand = c(0, 0), limits = lim, label = comma) +
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

plot <- waterfall_tx(new, old, 2023, 9, "PH")

old %<>%
   mutate(
      site = coalesce(na_if(realhub_branch, ""), realhub)
   )

new %<>%
   mutate(
      site = coalesce(na_if(realhub_branch, ""), realhub)
   )

data    <- list()
sites <- unique(new$site)
sites <- sites[!is.na(sites)]
for (hub in sites) {
   data[[hub]] <- list(
      old = old %>% filter(site == hub),
      new = new %>% filter(site == hub)
   )
}
dir   <- "C:/Users/johnb/Downloads/tx-waterfall_2023-09/site-level"
plot_bmp(plot, file.path(dir, "PH.bmp"))
plots <- list()
for (reg in regions) {
   plots[[reg]] <- waterfall_tx(data[[reg]]$new, data[[reg]]$old, 2023, 9, reg)
   file         <- file.path(dir, stri_c(reg, ".bmp"))
   plot_bmp(plots[[reg]], file)
}

plots <- list()
for (hub in sites) {
   plots[[hub]] <- waterfall_tx(data[[hub]]$new, data[[hub]]$old, 2023, 9, hub)
   file         <- file.path(dir, stri_c(hub, ".bmp"))
   plot_bmp(plots[[hub]], file)
}
