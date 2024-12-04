Blueprint <- R6Class(
   'Blueprint',
   public  = list(
      table      = NA_character_,
      columns    = list(),
      charset    = NA_character_,
      collation  = NA_character_,
      after      = NA_character_,

      initialize = function(table) self$table <- table,

      addColumn  = function(type, name, length) {
         varname <- stri_c("`", name, "`")
         vartype <- ifelse(!missing(length), stri_c(type, "(", length, ")"), type)

         self$columns[[name]] <- str_c(collapse = " ", c(varname, vartype, "NULL"))

         invisible(self)
      },

      varchar    = function(name, length = NULL) {
         if (missing(length))
            length <- 255

         self$addColumn('varchar', name, length)
         invisible(self)
      },

      char       = function(name, length = NULL) {
         if (missing(length))
            length <- 255

         self$addColumn('varchar', name, length)
         invisible(self)
      },

      text       = function(name) {
         self$addColumn('text', name)
         invisible(self)
      },

      integer    = function(name) {
         self$addColumn('int', name)
         invisible(self)
      },

      date       = function(name) {
         self$addColumn('date', name)
         invisible(self)
      },

      primary    = function(columns) {
         keys <- str_c(collapse = "`, `", columns)
         keys <- str_c("`", keys, "`")
         append(private$commands, "ALTER TABLE ", self$table, " ADD PRIMARY KEY (", keys, ");")

         invisible(self)
      },

      index    = function(name, columns) {
         keys <- str_c(collapse = "`, `", columns)
         keys <- str_c("`", keys, "`")
         append(private$commands, "CREATE INDEX ", name, " ON ", self$table, " (", keys, ");")

         invisible(self)
      }
   ),
   private = list(
      commands = list()
   )
)

try <- Blueprint$new("ohasis_lake.try")
try$char("REC_ID", 25)
try$char("CENTRAL_ID", 18)
try$char("PATIENT_ID", 18)
try$integer("idnum")
try$char("confirm_rec", 25)
try$char("hts_rec", 25)
try$varchar("form", 50)
try$varchar("modality", 50)
try$char("modality", 5)
try$varchar("labcode", 50)
try$varchar("labcode2", 50)
try$integer("year")
try$integer("month")
try$char("uic", 14)
try$varchar("firstname")
try$varchar("middle")
try$varchar("last")
try$date("bdate")
try$varchar("patient_code")
try$varchar("pxcode")
try$varchar("pxcode")
try$integer("age")
try$integer("age_months")
try$char("sex", 6)
try$varchar("philhealth")
try$varchar("philsys_id")
try$varchar("mobile")
try$varchar("email")
try$varchar("muncity", 50)
try$varchar("province", 25)
try$varchar("region", 20)
try$index("final_residence", c("region", "province", "muncity"))
try$varchar("muncity_c", 50)
try$varchar("province_c", 25)
try$varchar("region_c", 20)
try$index("current_residence", c("region_c", "province_c", "muncity_c"))
try$varchar("muncity_p", 50)
try$varchar("province_p", 25)
try$varchar("region_p", 20)
try$index("permanent_residence", c("region_p", "province_p", "muncity_p"))
try$integer("ocw")
try$integer("motherisi1")
try$integer("pregnant")
try$integer("tbpatient1")
try$varchar("region_p", 15)
civilstat
self_identity
self_identity_other
gender_identity
nationality
highest_educ
in_school
current_school_level
with_partner
child_count
sexwithf
sexwithm
sexwithpro
regularlya
injectdrug
chemsex
receivedbt
sti
needlepri1
transmit
sexhow
risk_motherhashiv
risk_sexwithf
risk_sexwithf_nocdm
risk_sexwithm
risk_sexwithm_nocdm
risk_payingforsex
risk_paymentforsex
risk_sexwithhiv
risk_injectdrug
risk_needlestick
risk_bloodtransfuse
risk_illicitdrug
risk_chemsex
risk_tattoo
risk_sti
class
class2022
ahd
baseline_cd4
baseline_cd4_date
baseline_cd4_result
confirm_date
confirmlab
confirm_region
confirm_province
confirm_muncity
confirm_result
confirm_remarks
region01
province01
placefbir
curr_work
prev_work
ocw_based
ocw_country
age_sex
age_inj
howmanymse
yrlastmsex
howmanyfse
yrlastfsex
past12mo_injdrug
past12mo_rcvbt
past12mo_sti
past12mo_sexfnocondom
past12mo_sexmnocondom
past12mo_sexprosti
past12mo_acceptpayforsex
past12mo_needle
past12mo_hadtattoo
history_sex_m
date_lastsex_m
date_lastsex_condomless_m
history_sex_f
date_lastsex_f
date_lastsex_condomless_f
prevtest
prev_test_result
prev_test_faci
prevtest_date
clinicalpicture
recombyph1
recomby_peer_ed
insurance1
recheckpr1
no_test_reason
possible_exposure
emp_local
emp_abroad
other_reason_test
description_symptoms
who_staging
hx_hepb
hx_hepc
hx_cbs
hx_prep
hx_pep
hx_sti
reach_clinical
reach_online
reach_it
reach_ssnt
reach_venue
refer_art
refer_confirm
retest
retest_in_mos
retest_in_wks
retest_date
given_hiv101
given_iec_mats
given_risk_reduce
given_prep_pep
given_ssnt
provider_type
provider_type_other
venue_region
venue_province
venue_muncity
venue_text
px_type
referred_by
hts_date
t0_date
t0_result
test_done
name
t1_date
t1_kit
t1_result
t2_date
t2_kit
t2_result
t3_date
t3_kit
t3_result
final_interpretation
visit_date
blood_extract_date
specimen_receipt_date
rhivda_done
sample_source
dxlab_standard
pubpriv
dx_region
dx_province
dx_muncity
harpid
first
clinichosp
othernat
job
local
travel
labtest
reporttype
deadaids
whendead
otherinfo
bldunit
permanenta
placeofbir
withmultip
reasonstes
sexpartne1
sharednsw1
employmen1
employme21
received21
pregnant21
hepatitis1
noreason1
wantsknow1
addresstes
shc
hub_reg
begda
test_date
nodata_hiv_stage
