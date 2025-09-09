select *
from (select data.central_id,
             data.lab_viral_date,
             data.lab_viral_result,
             row_number() over (partition by central_id order by lab_viral_date desc) as visit_num
      from (select coalesce(reg.central_id, rec.patient_id)      as central_id,
                   coalesce(rec.lab_viral_date, pii.record_date) as lab_viral_date,
                   rec.lab_viral_result
            from ohasis_lake.lab_wide as rec
                     left join ohasis_lake.id_registry as reg on rec.patient_id = reg.patient_id
                     left join ohasis_lake.px_demographics as pii on rec.rec_id = pii.rec_id
            where rec.deleted_at is NULL
              and rec.lab_viral_result is not NULL
              and date(coalesce(rec.lab_viral_date, pii.record_date)) <= ?) as data) as artstart
where visit_num = 1;