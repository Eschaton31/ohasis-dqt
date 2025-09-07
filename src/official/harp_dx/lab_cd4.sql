select if(id.central_id is null, pii.patient_id, id_registry.central_id) as central_id,
       cd4_data.lab_cd4_date as cd4_date,
       cd4_data.lab_cd4_result as cd4_result
from ohasis_lake.lab_wide as cd4_data
         join ohasis_lake.px_demographics as pii on cd4_data.rec_id = pii.rec_id
         left join ohasis_lake.id_registry as id on pii.patient_id = id.patient_id
where cd4_data.deleted_at is null and date(lab_cd4_date) <= '2025-07-31'