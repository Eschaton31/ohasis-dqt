read_harp <- function(sys, type, date) mutate_if(read_dta(hs_data(sys, type, year(date), month(date)), col_select = c(artstart_date, onart, idnum)), is.character, ~na_if(., ""))

dx <- read_dta(hs_data("harp_dx", "reg", 2024, 12), col_select = c(idnum, year, month)) %>%
   mutate(
      dx_date = as.Date(str_c(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
   ) %>%
   select(-year, -month)
tx <- data$`2024` %>%
   left_join(
      y = dx
   )

## year analysis
periods <- seq(2019, 2024)
periods <- str_c(periods, "-12-01")

data        <- lapply(periods, read_harp, sys = "harp_tx", type = "outcome")
names(data) <- as.character(year(periods))

# flow_dta(data$`2022`, "harp_tx", "outcome", 2022, 12)

count_onart <- function(data) nrow(filter(data, onart == 1))
count_new <- function(data, year) nrow(filter(data, year(artstart_date) == year))
count_dx <- function(data, year) nrow(filter(data, year(dx_date) == year))
count_dxtx <- function(data, year) nrow(filter(data, year(dx_date) == year, year(artstart_date) == year))

stats                 <- list(
   onart    = lapply(data, count_onart),
   newonart = lapply(names(data), count_new, data = tx),
   newdx    = lapply(names(data), count_dx, data = dx),
   dxtx     = lapply(names(data), count_dxtx, data = tx)
)
names(stats$newonart) <- as.character(year(periods))
names(stats$newdx)    <- as.character(year(periods))
names(stats$dxtx)    <- as.character(year(periods))

stats <- pivot_longer(as.data.frame(stats), cols = c(starts_with("onart"), starts_with("newonart"), starts_with("newdx"), starts_with("dxtx")))
stats %>%
   separate_wider_delim(name, ".", names = c("indicator", "period")) %>%
   group_by(indicator) %>%
   mutate(
      `%net +/-` = ((value / lag(value)) - 1) * 100
   ) %>%
   pivot_wider(
      names_from  = indicator,
      values_from = c(value, `%net +/-`),
   ) %>%
   mutate(
      ratio_dxtx = value_newonart / value_dxtx
   )

## quarter analysis

periods <- seq(2019, 2024)
q1      <- str_c(periods, "-03-01")
q2      <- str_c(periods, "-06-01")
q3      <- str_c(periods, "-09-01")
q4      <- str_c(periods, "-12-01")
periods <- sort(c(q1, q2, q3, q4))

data        <- lapply(periods, read_harp, sys = "harp_tx", type = "outcome")
names(data) <- str_left(periods, 7)

count_onart <- function(data) nrow(filter(data, onart == 1))
count_new <- function(data, ym) nrow(filter(data, year(artstart_date) == str_left(ym, 4), month(artstart_date) == as.numeric(str_right(ym, 2))))

stats                 <- list(
   onart    = lapply(data, count_onart),
   newonart = lapply(names(data), count_new, data = data$`2024-12`)
)
names(stats$newonart) <- str_left(periods, 7)

stats <- pivot_longer(as.data.frame(stats), cols = c(starts_with("onart"), starts_with("newonart")))
stats %>%
   separate_wider_delim(name, ".", names = c("indicator", "year", "month")) %>%
   group_by(indicator) %>%
   mutate(
      `%net +/-` = ((value / lag(value)) - 1) * 100
   ) %>%
   pivot_wider(
      names_from  = indicator,
      values_from = c(value, `%net +/-`),
   )

## monthly analysis

periods <- seq(2022, 2024)
m01     <- str_c(periods, "-01-01")
m02     <- str_c(periods, "-02-01")
m03     <- str_c(periods, "-03-01")
m04     <- str_c(periods, "-04-01")
m05     <- str_c(periods, "-05-01")
m06     <- str_c(periods, "-06-01")
m07     <- str_c(periods, "-07-01")
m08     <- str_c(periods, "-08-01")
m09     <- str_c(periods, "-09-01")
m10     <- str_c(periods, "-10-01")
m11     <- str_c(periods, "-11-01")
m12     <- str_c(periods, "-12-01")
periods <- sort(c(m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12))

data        <- lapply(periods, read_harp, sys = "harp_tx", type = "outcome")
names(data) <- str_left(periods, 7)

count_onart <- function(data) nrow(filter(data, onart == 1))
count_new <- function(data, ym) nrow(filter(data, year(artstart_date) == str_left(ym, 4), month(artstart_date) == as.numeric(str_right(ym, 2))))

stats                 <- list(
   onart    = lapply(data, count_onart),
   newonart = lapply(names(data), count_new, data = data$`2024-12`)
)
names(stats$newonart) <- str_left(periods, 7)

stats <- pivot_longer(as.data.frame(stats), cols = c(starts_with("onart"), starts_with("newonart")))
stats %>%
   separate_wider_delim(name, ".", names = c("indicator", "year", "month")) %>%
   group_by(indicator) %>%
   mutate(
      `%net +/-` = ((value / lag(value)) - 1) * 100
   ) %>%
   pivot_wider(
      names_from  = indicator,
      values_from = c(value, `%net +/-`),
   )
