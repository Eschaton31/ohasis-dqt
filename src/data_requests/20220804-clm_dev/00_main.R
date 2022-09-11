
ref_faci <- ohasis$ref_faci %>%
   filter(SUB_FACI_ID == "") %>%
   select(
      FACI_ID,
      FACI_NAME,
      NHSSS_CODE = FACI_CODE,
      LAT,
      LONG,
      EMAIL,
      MOBILE,
      LANDLINE,
      PSGC_REG   = FACI_PSGC_REG,
      PSGC_PROV  = FACI_PSGC_PROV,
      PSGC_MUNC  = FACI_PSGC_MUNC,
      ADDRESS    = FACI_ADDR
   )

ref_addr <- ohasis$ref_addr %>%
   select(
      starts_with("PSGC"),
      starts_with("NAME"),
   )

ref_codebook <-list(
   ref_faci = read_sheet("1EmacCXifEOmSdT5iiFwIxjdtdZafEjK-5EhWyOmuyhI", "Sheet2"),
   ref_addr = read_sheet("1EmacCXifEOmSdT5iiFwIxjdtdZafEjK-5EhWyOmuyhI", "Sheet3")
)

write_tsv(ref_faci, "H:/Data Requests/20220804-clm_dev/20220822_ref_faci.tsv")
write_tsv(ref_addr, "H:/Data Requests/20220804-clm_dev/20220822_ref_addr.tsv")
write_xlsx(ref_codebook, "H:/Data Requests/20220804-clm_dev/20220822_Codebook.xlsx")