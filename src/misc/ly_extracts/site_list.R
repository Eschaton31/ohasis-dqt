sites <- ohasis$ref_faci %>%
   filter(FACI_NAME %like% "LoveYourself")
staff <- ohasis$ref_staff %>%
   filter(
      STAFF_ID %in% c('0700210045', '0700210056', '1300010158', '9900050053', '9900050036')
   )

min <- "2024-01-01"
max <- "2024-06-30"