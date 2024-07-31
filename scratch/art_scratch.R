library(showtext)
library(scales)
font_add_google("Inter", family = "Inter")
showtext_auto()

sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", col_types = "c", .name_repair = "unique_quiet")
cols  <- c("art_id", "outcome", "artstart_date", "real_reg", "realhub", "realhub_branch")
old   <- read_dta(hs_data("harp_tx", "outcome", 2023, 8), col_select = any_of(cols)) %>%
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
new   <- read_dta(hs_data("harp_tx", "outcome", 2023, 9), col_select = any_of(cols)) %>%
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

old     <- read_dta(hs_data("harp_tx", "outcome", 2023, 8), col_select = any_of(cols))
new     <- harp_tx$official$new_outcome %>% select(any_of(cols))
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
   params$ym  <- str_c(sep = "..", params$yr, stri_pad_left(params$mo, 2, "0"))
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

data  <- list()
sites <- unique(new$site)
sites <- sites[!is.na(sites)]
for (hub in sites) {
   data[[hub]] <- list(
      old = old %>% filter(site == hub),
      new = new %>% filter(site == hub)
   )
}

dir <- "C:/Users/Administrator/Downloads/waterfall_2023-09/site-level"
plot_bmp(plot, file.path(dir, "PH.bmp"))
plots <- list()
for (reg in regions) {
   plots[[reg]] <- waterfall_tx(data[[reg]]$new, data[[reg]]$old, 2023, 9, reg)
   file         <- file.path(dir, stri_c(reg, ".bmp"))
   plot_bmp(plots[[reg]], file)
}

dir   <- "D:/waterfall_2023-09/Hubs"
sites <- new %>%
   # filter(site_icap_2023 == 1) %>%
   select(site_name)
sites <- sites$site_name
sites <- sites[!is.na(sites)]
sites <- unique(sites)
for (hub in sites) {
   plot   <- waterfall_tx(new %>% filter(site_name == hub), old %>% filter(site_name == hub), 2023, 9, hub)
   region <- ohasis$ref_faci %>%
      filter(FACI_NAME == hub) %>%
      distinct(FACI_ID, .keep_all = TRUE)
   region <- region$FACI_NAME_REG[1]
   subdir <- file.path(dir, region)
   check_dir(subdir)
   file <- file.path(subdir, stri_c(path_sanitize(hub), ".bmp"))
   plot_bmp(plot, file)
}

wf           <- waterfall_linelist("2022-06-01", "2023-05-31")
regions      <- unique(wf$new$real_reg)
plots        <- lapply(regions, function(reg) {
   old  <- wf$old %>% filter(real_reg == reg)
   new  <- wf$new %>% filter(real_reg == reg)
   plot <- waterfall_aggregate(old, new, reg)
   return(plot$data)
})
names(plots) <- regions

plots %>%
   write_xlsx("H:/20240708_waterfalls_202206-202305.xlsx")

wf <- waterfall_linelist("2023-06-01", "2024-05-31")
wf$old %<>% mutate(final_hub = coalesce(na_if(realhub_branch, ""), realhub))
wf$new %<>% mutate(final_hub = coalesce(na_if(realhub_branch, ""), realhub))
hubs         <- unique(wf$new$final_hub)
plots        <- lapply(hubs, function(site) {
   old  <- wf$old %>% filter(final_hub == site)
   new  <- wf$new %>% filter(final_hub == site)
   plot <- waterfall_aggregate(old, new, site)
   return(plot$data)
})
names(plots) <- hubs

plot <- waterfall_aggregate(wf$old, wf$new, "PH")
plot$data


agg <- plots %>%
   bind_rows(.id = "branch") %>%
   mutate(
      hub    = if_else(str_detect(branch, "-"), substr(branch, 1, stri_locate_first_fixed(branch, "-") - 1), branch),
      hub    = if_else(hub == "SHIP-MAKATI", "SHIP", hub, hub),
      branch = if_else(hub == branch, "", branch, branch),
      branch = if_else(hub == "SHIP-MAKATI", "", branch, branch),
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(ART_FACI = "hub", ART_SUB_FACI = "branch")
   ) %>%
   ohasis$get_faci(
      list(tx_hub = c("ART_FACI", "ART_SUB_FACI")),
      "name",
      c("tx_reg", "tx_prov", "tx_munc")
   ) %>%
   select(-hub, -branch) %>%
   relocate(tx_reg, tx_prov, tx_munc, tx_hub, .before = 1) %>%
   arrange(tx_reg, tx_prov, tx_munc, tx_hub)

agg %>%
   write_xlsx("H:/20240712_waterfalls_202206-202305_bySite.xlsx")

plot$data %>%
   write_xlsx("H:/20240712_waterfalls_202306-202405_ph.xlsx")