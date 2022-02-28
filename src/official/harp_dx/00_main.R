##  HARP Registry Linkage Controller -------------------------------------------

# update warehouse - Form A
ohasis$data_factory("warehouse", "form_a", "upsert", TRUE)

# update warehouse - HTS Form
ohasis$data_factory("warehouse", "form_hts", "upsert", TRUE)

# update warehouse - OHASIS IDs
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)

# define datasets
if (!exists('nhsss'))
   nhsss <- list()

##  Begin linkage of datasets --------------------------------------------------

source("src/official/harp_dx/01_load_corrections.R")
source("src/official/harp_dx/02_load_harp.R")
source("src/official/harp_dx/03_data_initial.R")
source("src/official/harp_dx/04_data_convert.R")

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
   'nspalaypayon'
)
link  <- "https://www.dropbox.com/s/41i0nzu8xtb8qeb/JAN%202022_2.0.rar?dl=0"
for (user in users) {
   id <- (slack %>% filter(name == user))$id
   slackr_msg(
      channel = id,
      paste0(">Hi! The HARP Dx Registry dataset for the reporting period of ",
             month.abb[as.numeric(ohasis$mo)], " ", ohasis$yr,
             " has now been updated with the necessary changes for tagging of advanced HIV disease. You may click on the link below to download the updated dataset.\n><", link,
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