select *
from (select data.central_id,
             data.date_confirm,
             data.confirm_code,
             data.confirm_result,
             data.confirm_remarks,
             row_number() over (partition by central_id order by date_confirm desc) as visit_num
      from (select coalesce(reg.central_id, rec.patient_id)                                    as central_id,
                   date(coalesce(data.date_confirm, data.t3_date, data.t2_date, data.t1_date)) as date_confirm,
                   data.confirm_code,
                   data.confirm_result,
                   data.confirm_remarks
            from ohasis_lake.px_demographics as rec
                     left join ohasis_lake.id_registry as reg on rec.patient_id = reg.patient_id
                     left join ohasis_lake.px_hiv_testing as data on rec.rec_id = data.rec_id
            where rec.deleted_at is NULL
              and date(coalesce(data.date_confirm, data.t3_date, data.t2_date, data.t1_date)) <= ?
              and data.confirm_result <> '4_Pending') as data) as artstart
where visit_num = 1;