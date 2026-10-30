with -- keep the first line a SQL keyword: ClickHouseHTTP drops the output format when a query starts with a comment
     -- dx_new patients plus every patient_id linked to them in the registry
     dx_pids as (select central_id as patient_id
                 from ohasis_warehouse.dx_new
                 union distinct
                 select patient_id
                 from ohasis_lake.id_registry
                 where central_id in (select central_id from ohasis_warehouse.dx_new)),
     -- their records, resolved on 3 columns before reading the wide table
     dx_recs as (select rec_id
                 from ohasis_warehouse.form_cfbs
                 where deleted_at is null
                   and patient_id in dx_pids)
-- every table is cut down to these patients before joining, so the joins stay small
select if(id_registry.central_id is null, hts_data.patient_id, id_registry.central_id) as central_id,
       hts_data.*
from (select * from ohasis_warehouse.form_cfbs where rec_id in dx_recs) as hts_data
         left join (select * from ohasis_lake.px_hiv_testing where rec_id in dx_recs) as px_hiv_testing
                   on hts_data.rec_id = px_hiv_testing.rec_id
         left join (select * from ohasis_lake.id_registry where patient_id in dx_pids) as id_registry
                   on hts_data.patient_id = id_registry.patient_id
where (px_hiv_testing.t0_result like '1%' or px_hiv_testing.t0_result is null or hts_data.rec_id in (select rec_id from ohasis_warehouse.dx_new))
  and coalesce(id_registry.central_id, hts_data.patient_id) in (select central_id from ohasis_warehouse.dx_new)
  and hts_data.deleted_at is null
