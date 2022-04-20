##  Notify via Slack -----------------------------------------------------------

data_dta   <- strsplit(nhsss$harp_dx$official$new_file, "/")
data_zip   <- stri_replace_last_fixed(data_dta[[1]][length(data_dta[[1]])], ".dta", ".zip")
data_share <- drop_share(glue("/DQT/Data Factory/HARP Dx/{data_zip}"))$url

slack <- slackr_users()
users <- c(
   'jrpalo',
   'kmasilo',
   'jmvelayo',
   'mcrendon',
   'mgzapanta',
   'appadilla',
   'cjmaranan.doh',
   'mcamoroso.doh',
   'nspalaypayon',
   'kapamittan'
)
link  <- "https://www.dropbox.com/t/WH1W3rnHydzuxLCj"
for (user in users) {
   id <- (slack %>% filter(name == user))$id
   slackr_msg(
      channel = id,
      paste0(">Hi! The HARP Dx Registry dataset for the reporting period of ",
             month.abb[as.numeric(ohasis$mo)], " ", ohasis$yr,
             " has now been uploaded. You may click on the link below to download the dataset.\n><", link,
             "|", toupper(month.abb[as.numeric(ohasis$mo)]), " ", ohasis$yr, ">"),
      mrkdwn  = "true"
   )
}

slackr_msg(
   channel = "harp",
   paste0(">Hi! The HARP Dx Registry dataset for the reporting period of ",
          month.abb[as.numeric(ohasis$mo)], " ", ohasis$yr,
          " is now available. Those concerned should have already received a message from the *Slackbot* and an email from <@U0328EFNAQL> containing the dataset.\n>Have a great day!"),
   mrkdwn  = "true"
)

df <- nhsss$harp_dx$converted$data %>%
   mutate(
      stata = if_else(
         !is.na(ahd),
         paste0("replace ahd = ", ahd, " if labcode == \"", labcode, "\""),
         paste0("replace ahd = . if labcode == \"", labcode, "\"")
      )
   )
write_clip(df$stata)