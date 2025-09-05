select coalesce(id.central_id, form.patient_id) as central_id,
       form.*
from ohasis_warehouse.form_d as form
         left join ohasis_lake.id_registry as id using (patient_id)
where coalesce(id.central_id, form.patient_id) not in (select harp_dead_old.central_id from ohasis_warehouse.harp_dead_old);
