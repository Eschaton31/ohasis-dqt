with -- keep the first line a SQL keyword: ClickHouseHTTP drops the output format when a query starts with a comment
     -- dx_new patients plus every patient_id linked to them in the registry
     dx_pids as (select central_id as patient_id
                 from ohasis_warehouse.dx_new
                 union distinct
                 select patient_id
                 from ohasis_lake.id_registry
                 where central_id in (select central_id from ohasis_warehouse.dx_new))
-- only dx_new patients can get a baseline cd4, so cut the joins down to them
select if(id.central_id is null, pii.patient_id, id.central_id) as central_id,
       cd4_data.lab_cd4_date as cd4_date,
       cd4_data.lab_cd4_result as cd4_result
from ohasis_lake.lab_wide as cd4_data
         join (select rec_id, patient_id from ohasis_lake.px_demographics where patient_id in dx_pids) as pii
              on cd4_data.rec_id = pii.rec_id
         left join (select patient_id, central_id from ohasis_lake.id_registry where patient_id in dx_pids) as id
                   on pii.patient_id = id.patient_id
where cd4_data.deleted_at is null
  and date(lab_cd4_date) <= '{max_date}'
  and coalesce(id.central_id, pii.patient_id) in (select central_id from ohasis_warehouse.dx_new)
