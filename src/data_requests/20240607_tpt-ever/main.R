yr <- "2025"
mo <- "12"

min <- "2025-01-01"
max <- as.character(end_ym(yr, mo))

conn <- connect("mariadb-lw")

tpt_all <- QB$new(conn)$from("ohasis_warehouse.form_art_bc")
tpt_all$where("visit_date", "<=", max)
tpt_all$whereNull("deleted_at")
tpt_all$select(patient_id,
               rec_id,
               visit_date,
               tb_ipt_status,
               tb_ipt_outcome,
               tb_ipt_outcome_other,
               tb_ipt_start_date,
               tb_ipt_end_date)
tpt_all$where(function(query = QB$new(conn)) {
   query$whereNotNull("tb_ipt_status", "or")$
      whereNotNull("tb_ipt_outcome", "or")$
      whereNotNull("tb_ipt_outcome_other", "or")$
      whereNotNull("tb_ipt_start_date", "or")$
      whereNotNull("tb_ipt_end_date", "or")
   query$whereNested
})
tpt_all <- tpt_all$get()

notb <- QB$new(conn)$
   from("ohasis_warehouse.form_art_bc")$
   select(patient_id,
          visit_date,
          rec_id,
          tb_status)$
   where("visit_date", "<=", max)$
   whereNull("deleted_at")$
   where(function(query = QB$new(conn)) {
   query$whereNotNull("tb_ipt_status", "or")$
      whereRaw("left(tb_status, 1) = '0'", 'or')$
      whereNotNull("tb_ipt_outcome", "or")$
      whereNotNull("tb_ipt_outcome_other", "or")$
      whereNotNull("tb_ipt_start_date", "or")$
      whereNotNull("tb_ipt_end_date", "or")
   query$whereNested
})$
   get()

# tpt_ever <- QB$new(conn)$from("ohasis_warehouse.tpt_ever")$get()
# id_reg <- QB$new(conn)$from("ohasis_warehouse.id_registry")$select("central_id", "patient_id")$get()

dbDisconnect(conn)

id_reg <- update_idreg()
tpt_id <- tpt_all %>% get_cid(id_reg, patient_id)

tpt_started <- function(data, min, max, var) {
   data %>%
      filter(coalesce(tb_ipt_status, "") != "0_Not on IPT") %>%
      mutate(
         keep = case_when(
            tb_ipt_status == "12_Started IPT" & between(visit_date, min, max) ~ 1,
            between(tb_ipt_start_date, min, max) ~ 1,
            TRUE ~ 0
         )
      ) %>%
      filter(keep == 1) %>%
      distinct(central_id) %>%
      mutate({{var}} := 1) %>%
      return()
}

tpt_given <- function(data, min, max, var) {
   data %>%
      filter(coalesce(tb_ipt_status, "") != "0_Not on IPT") %>%
      mutate(
         keep = case_when(
            between(visit_date, min, max) ~ 1,
            between(tb_ipt_start_date, min, max) ~ 1,
            TRUE ~ 0
         )
      ) %>%
      filter(keep == 1) %>%
      distinct(central_id) %>%
      mutate({{var}} := 1) %>%
      return()
}

tpt_ever <- tpt_id %>%
   mutate(
      keep = case_when(
         tb_ipt_status == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      ),
   ) %>%
   filter(keep == 1) %>%
   distinct(central_id) %>%
   mutate(ever_tpt = 1)

startedtpt_year <- tpt_started(tpt_id, start_ym(yr, 1), end_ym(yr, 12), year_startedtpt)
giventpt_year   <- tpt_given(tpt_id, start_ym(yr, 1), end_ym(yr, 12), year_giventpt)
tpt_year        <- startedtpt_year %>% full_join(giventpt_year, join_by(central_id))

startedtpt_s1 <- tpt_started(tpt_id, start_ym(yr, 1), end_ym(yr, 6), s1_startedtpt)
giventpt_s1   <- tpt_given(tpt_id, start_ym(yr, 1), end_ym(yr, 6), s1_giventpt)
tpt_s1        <- startedtpt_s1 %>% full_join(giventpt_s1, join_by(central_id))

startedtpt_s2 <- tpt_started(tpt_id, start_ym(yr, 7), end_ym(yr, 12), s2_startedtpt)
giventpt_s2   <- tpt_given(tpt_id, start_ym(yr, 7), end_ym(yr, 12), s2_giventpt)
tpt_s2        <- startedtpt_s2 %>% full_join(giventpt_s2, join_by(central_id))

startedtpt_q1 <- tpt_started(tpt_id, start_ym(yr, 1), end_ym(yr, 3), q1_startedtpt)
giventpt_q1   <- tpt_given(tpt_id, start_ym(yr, 1), end_ym(yr, 3), q1_giventpt)
tpt_q1        <- startedtpt_q1 %>% full_join(giventpt_q1, join_by(central_id))

startedtpt_q2 <- tpt_started(tpt_id, start_ym(yr, 4), end_ym(yr, 6), q2_startedtpt)
giventpt_q2   <- tpt_given(tpt_id, start_ym(yr, 4), end_ym(yr, 6), q2_giventpt)
tpt_q2        <- startedtpt_q2 %>% full_join(giventpt_q2, join_by(central_id))

startedtpt_q3 <- tpt_started(tpt_id, start_ym(yr, 7), end_ym(yr, 9), q3_startedtpt)
giventpt_q3   <- tpt_given(tpt_id, start_ym(yr, 7), end_ym(yr, 9), q3_giventpt)
tpt_q3        <- startedtpt_q3 %>% full_join(giventpt_q3, join_by(central_id))

startedtpt_q4 <- tpt_started(tpt_id, start_ym(yr, 10), end_ym(yr, 12), q4_startedtpt)
giventpt_q4   <- tpt_given(tpt_id, start_ym(yr, 10), end_ym(yr, 12), q4_giventpt)
tpt_q4        <- startedtpt_q4 %>% full_join(giventpt_q4, join_by(central_id))

notb_ever <- notb %>%
   get_cid(id_reg, patient_id) %>%
   distinct(central_id) %>%
   mutate(ever_notb = 1)

# tx_curr <- hs_data("harp_tx", "reg", yr, mo) %>%
#    read_dta(col_select = c(art_id, patient_id, artstart_date)) %>%
#    get_cid(id_reg, patient_id) %>%
#    left_join(
#       y  = hs_data("harp_tx", "outcome", yr, mo) %>%
#          read_dta(col_select = c(art_id, outcome, onart, hub, branch, realhub, realhub_branch, curr_age, sex)),
#       by = join_by(art_id)
#    )

conn   <- connect('mariadb-lw')
tx_reg <- QB$new(conn)$from("harp_tx.reg_{yr}{mo}")$get()
tx_out <- QB$new(conn)$from("harp_tx.outcome_{yr}{mo}")$get()
dbDisconnect(conn)

tx_curr <- tx_reg %>%
   select(art_id, patient_id, artstart_date) %>%
   get_cid(id_reg, patient_id) %>%
   left_join(
      y  = tx_out %>%
         select(art_id, outcome, onart, hub, branch, realhub, realhub_branch, curr_age, sex),
      by = join_by(art_id)
   )

art_ever_tpt <- tx_curr %>%
   mutate(
      curr_age_c = coalesce(gen_agegrp(curr_age, "harp"), "(no data)"),
      sex        = coalesce(str_to_title(sex), "(no data)")
   ) %>%
   left_join(
      y  = tpt_ever,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tpt_year,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tpt_s1,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tpt_s2,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tpt_q1,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tpt_q2,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tpt_q3,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tpt_q4,
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = notb_ever,
      by = join_by(central_id)
   ) %>%
   mutate_at(
      .vars = vars(ever_tpt),
      ~coalesce(., as.integer(0))
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(tx_faci = "hub", tx_sub_faci = "branch")
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(real_faci = "realhub", real_sub_faci = "realhub_branch")
   ) %>%
   mutate_at(
      .vars = vars(tx_faci, real_faci),
      ~if_else(. == "130000", NA_character_, ., .),
   ) %>%
   ohasis$get_faci(
      list("txfaci" = c("tx_faci", "tx_sub_faci")),
      "name",
      c("txfaci_region", "txfaci_province", "txfaci_muncity")
   ) %>%
   ohasis$get_faci(
      list("realfaci" = c("real_faci", "real_sub_faci")),
      "name",
      c("realfaci_region", "realfaci_province", "realfaci_muncity")
   ) %>%
   arrange(art_id) %>%
   distinct(art_id, .keep_all = TRUE)

# write file to local dataset
art_ever_tpt %>%
   format_stata %>%
   select(-contains(".")) %>%
   write_dta("E:/20260227_tbhiv_2025-12.dta")
compress_stata("E:/20260227_tbhiv_2025-12.dta")

## !!! WARNING THIS WILL UPLOAD THE DATA TO THE OLD LW FOR THE tpt.php

# uploading your own data into MariaDB
# 1) open a connection to the server
conn <- connect('old-lw')

# 2) get your data into an object
data <- art_ever_tpt

# 3) define your primary key (unique id)
# note: can be multiple columns
id <- "art_id"

# 4) which schema/db are you using
schema <- "dashboard"

# 5) what is its table name?
table   <- stri_c("tpt_", yr, mo) # "lala.mydata"
tbl_reg <- stri_c("reg_", yr, mo) # "lala.mydata"
tbl_out <- stri_c("outcome_", yr, mo) # "lala.mydata"

# 6) upload data
table_space <- Id(schema = schema, table = table)
if (dbExistsTable(conn, table_space)) {
   dbExecute(conn, glue('TRUNCATE TABLE dashboard.{table}'))
   dbExecute(conn, glue('TRUNCATE TABLE harp_tx.{tbl_reg}'))
   dbExecute(conn, glue('TRUNCATE TABLE harp_tx.{tbl_out}'))
} else {
   dbExecute(conn, glue(r"(
create table dashboard.tpt_{yr}{mo}
(
    central_id        char(18)     null,
    patient_id        char(18)     null,
    art_id            int          not null
        primary key,
    sex               varchar(150) null,
    curr_age          int          null,
    hub               varchar(150) null,
    branch            varchar(150) null,
    realhub           varchar(150) null,
    realhub_branch    varchar(150) null,
    outcome           varchar(150) null,
    onart             int          null,
    curr_age_c        varchar(150) null,
    ever_tpt          int          null,
    year_startedtpt   int          null,
    year_giventpt     int          null,
    s1_startedtpt     int          null,
    s1_giventpt       int          null,
    s2_startedtpt     int          null,
    s2_giventpt       int          null,
    q1_startedtpt     int          null,
    q1_giventpt       int          null,
    q2_startedtpt     int          null,
    q2_giventpt       int          null,
    q3_startedtpt     int          null,
    q3_giventpt       int          null,
    q4_startedtpt     int          null,
    q4_giventpt       int          null,
    ever_notb         int          null,
    txfaci            varchar(150) null,
    realfaci          varchar(150) null,
    txfaci_region     varchar(150) null,
    txfaci_province   varchar(150) null,
    txfaci_muncity    varchar(150) null,
    realfaci_region   varchar(150) null,
    realfaci_province varchar(150) null,
    realfaci_muncity  varchar(150) null
);

create index art_id
    on dashboard.tpt_{yr}{mo} (art_id);

create index central_id
    on dashboard.tpt_{yr}{mo} (central_id);

create index patient_id
    on dashboard.tpt_{yr}{mo} (patient_id);
)"))
   dbExecute(conn, glue(r"(
create table harp_tx.reg_{yr}{mo}
(
    central_id              char(18)     null,
    rec_id                  char(25)     null,
    patient_id              char(18)     null,
    art_id                  int          not null
        primary key,
    idnum                   int          null,
    prep_id                 int          null,
    year                    int          null,
    month                   int          null,
    confirmatory_code       varchar(150) null,
    px_code                 varchar(150) null,
    uic                     varchar(150) null,
    first                   varchar(150) null,
    middle                  varchar(150) null,
    last                    varchar(150) null,
    suffix                  varchar(150) null,
    age                     int          null,
    birthdate               date         null,
    sex                     varchar(150) null,
    initials                varchar(150) null,
    philhealth_no           varchar(150) null,
    philsys_id              varchar(150) null,
    mobile                  varchar(150) null,
    email                   varchar(150) null,
    curr_reg                varchar(150) null,
    curr_prov               varchar(150) null,
    curr_munc               varchar(150) null,
    artstart_hub            varchar(150) null,
    artstart_branch         varchar(150) null,
    artstart_realhub        varchar(150) null,
    artstart_realhub_branch varchar(150) null,
    artstart_reg            varchar(150) null,
    artstart_prov           varchar(150) null,
    artstart_munc           varchar(150) null,
    artstart_stage          int          null,
    visit_type              varchar(150) null,
    tx_status               varchar(150) null,
    artstart_addr           varchar(150) null,
    artstart_date           date         null,
    artstart_nextpickup     datetime     null,
    artstart_regimen        varchar(150) null,
    baseline_cd4            int          null,
    baseline_cd4_date       date         null,
    baseline_cd4_result     int          null,
    pregnant                int          null,
    age_pregnant            int          null,
    mort_id                 int          null,
    artstart_realbranch     varchar(150) null,
    age_dta                 int          null,
    newonart                int          null,
    confirm_result          varchar(150) null,
    artstart_line           int          null,
    artstart_month          int          null,
    artstart_year           int          null,
    confirm_date            date         null,
    confirm_remarks         text         null,
    ref_death_date          date         null
);

create index CENTRAL_ID
    on harp_tx.reg_{yr}{mo} (central_id);

create index PATIENT_ID
    on harp_tx.reg_{yr}{mo} (patient_id);

create index art_id
    on harp_tx.reg_{yr}{mo} (art_id);

create index idnum
    on harp_tx.reg_{yr}{mo} (idnum);

create index mort_id
    on harp_tx.reg_{yr}{mo} (mort_id);

create index prep_id
    on harp_tx.reg_{yr}{mo} (prep_id);
)"))
   dbExecute(conn, glue(r"(
create table harp_tx.outcome_{yr}{mo}
(
    rec_id              char(25)     null,
    central_id          char(18)     null,
    art_id              int          not null
        primary key,
    idnum               int          null,
    prep_id             int          null,
    mort_id             int          null,
    sex                 varchar(150) null,
    curr_age            int          null,
    hub                 varchar(150) null,
    branch              varchar(150) null,
    sathub              varchar(150) null,
    transhub            varchar(150) null,
    tx_reg              varchar(150) null,
    tx_prov             varchar(150) null,
    tx_munc             varchar(150) null,
    realhub             varchar(150) null,
    realhub_branch      varchar(150) null,
    real_reg            varchar(150) null,
    real_prov           varchar(150) null,
    real_munc           varchar(150) null,
    artstart_date       date         null,
    class               varchar(150) null,
    outcome             varchar(150) null,
    latest_ffupdate     date         null,
    latest_nextpickup   date         null,
    previous_ffupdate   date         null,
    previous_nextpickup date         null,
    newonart            int          null,
    onart               int          null,
    vl_date             date         null,
    vl_result           int          null,
    baseline_vl         int          null,
    vl_suppressed       int          null,
    vlp12m              int          null,
    vl_yr               int          null,
    vl_mo               int          null,
    latest_regimen      varchar(150) null,
    regimen             varchar(150) null,
    reg_line            int          null,
    previous_regimen    varchar(150) null,
    latest_regdisagg    varchar(150) null,
    latest_regline      int          null,
    arv_reg             varchar(150) null,
    art_reg             varchar(150) null,
    line                int          null,
    previous_regdisagg  varchar(150) null,
    concat_col          varchar(150) null,
    previous_regline    int          null,
    who_staging         int          null,
    tb_status           varchar(150) null,
    oi_syph             int          null,
    oi_hepb             int          null,
    oi_hepc             int          null,
    oi_pcp              int          null,
    oi_cmv              int          null,
    oi_orocand          int          null,
    oi_herpes           int          null,
    oi_other            varchar(150) null
);

create index art_id
    on harp_tx.outcome_{yr}{mo} (art_id);

create index central_id
    on harp_tx.outcome_{yr}{mo} (central_id);

create index idnum
    on harp_tx.outcome_{yr}{mo} (idnum);

create index mort_id
    on harp_tx.outcome_{yr}{mo} (mort_id);

create index prep_id
    on harp_tx.outcome_{yr}{mo} (prep_id);
)"))
}
# ohasis$upsert(conn, schema, table, data, id)
dbxUpsert(conn, Id(schema = 'dashboard', table = table), art_ever_tpt %>% select(-psgc.x, -psgc.y, -artstart_date), 'art_id', batch_size = 10000)
dbxUpsert(conn, Id(schema = 'harp_tx', table = tbl_reg), tx_reg, 'art_id', batch_size = 10000)
dbxUpsert(conn, Id(schema = 'harp_tx', table = tbl_out), tx_out, 'art_id', batch_size = 10000)

# 7) close connection
dbDisconnect(conn)

# write_dta(format_stata(art_ever_tpt), "H:/20240809_tbhiv-evertpt_2024-06.dta")
# 
# art_ever_tpt %>% tab(onart, no_tb, ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1) %>%
#    tab(ever_no_tb)
# 
# art_ever_tpt %>%
#    tab(ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1) %>%
#    tab(ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1) %>%
#    tab(ever_no_tb, ever_ipt)
# 
# art_ever_tpt %>%
#    filter(onart == 1, ever_no_tb == 1) %>%
#    tab(ever_ipt)
