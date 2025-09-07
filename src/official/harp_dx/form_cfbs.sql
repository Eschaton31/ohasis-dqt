select if(id_registry.central_id is null, hts_data.patient_id, id_registry.central_id) as central_id,
       hts_data.*
from ohasis_warehouse.form_cfbs as hts_data
         left join ohasis_lake.px_hiv_testing on hts_data.rec_id = px_hiv_testing.rec_id
         left join ohasis_lake.id_registry on hts_data.patient_id = id_registry.patient_id
where (px_hiv_testing.t0_result like '1%' or px_hiv_testing.t0_result is null or hts_data.rec_id in (select rec_id from ohasis_warehouse.dx_new))
  and coalesce(id_registry.central_id, hts_data.patient_id) in (select central_id from ohasis_warehouse.dx_new)
  and hts_data.deleted_at is null
