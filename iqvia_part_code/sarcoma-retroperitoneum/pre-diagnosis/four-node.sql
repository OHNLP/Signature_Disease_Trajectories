with patient_records as (
select C.*, D.gender as gender, 
    CAST(EXTRACT(YEAR FROM cast(diagnose_dt as TIMESTAMP)) AS int) - cast(year_of_birth as int) AS age
    from (
        select * from (
        select
            *, ROW_NUMBER() OVER (PARTITION BY ims_patient_id, 4digit_icd ORDER BY diagnose_dt ASC) AS rn
        from (
            select
                A.ims_patient_id as ims_patient_id,
                CONCAT(SUBSTRING(diagnosis_date, 8, 4), '-', 
                  CASE 
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'JAN' THEN '01'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'FEB' THEN '02'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'MAR' THEN '03'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'APR' THEN '04'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'MAY' THEN '05'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'JUN' THEN '06'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'JUL' THEN '07'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'AUG' THEN '08'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'SEP' THEN '09'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'OCT' THEN '10'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'NOV' THEN '11'
                    WHEN SUBSTRING(diagnosis_date, 4, 3) = 'DEC' THEN '12'
                  END, '-', 
                  SUBSTRING(diagnosis_date, 1, 2)) AS diagnose_dt,
                A.icd_code as original_icd_code,
                CASE
                    WHEN A.icd_code like 'C49.A%' 
                    or A.icd_code in ('C47.4', 'C47.5', 'C48', 'C48.0', 'C48.1', 'C48.2', 'C48.8', 'C49.4', 'C49.5', 'C22.3', 'C22.4', 'C54.2', 'C55') THEN 'sarcoma-retroperitoneum'
                    WHEN length(A.icd_code)>3 THEN CONCAT(SUBSTRING(A.icd_code, 1, 3), '.', SUBSTRING(A.icd_code,5,1))
                    ELSE A.icd_code
                END as 4digit_icd,
                A.icd_type as icd_type,
                A.tumor_sub_type as tumor_sub_type,
                A.stage_id as stage_id,
                A.diagnosis_date_prescn as diagnosis_date_prescn
            from
                IQVIA.IQVIA_ONCEMR_EMR_PATIENT_DIAGNOSIS as A
            where
                A.icd_code is not null and length(A.icd_code)>=3 and A.icd_type='2'
                and (A.icd_code not like 'O%' and A.icd_code not like 'P%' and A.icd_code not like 'R%' and A.icd_code not like 'S%' 
                    and A.icd_code not like 'T%' and A.icd_code not like 'V%' and A.icd_code not like 'W%' and A.icd_code not like 'X%' 
                    and A.icd_code not like 'Y%' and A.icd_code not like 'Z%')
        ) as B ) as tmp
        where rn=1
    ) as C
    left join IQVIA.IQVIA_ONCEMR_EMR_PATIENT as D
    on C.ims_patient_id=D.ims_patient_id
),
cancer_patient_records as (
    select B.*
    from (
        select distinct ims_patient_id
        from patient_records
        where icd_type='2' and tumor_sub_type like '%SARCOMA' and 4digit_icd='sarcoma-retroperitoneum'
        ) as A
    inner join patient_records as B
    on A.ims_patient_id=B.ims_patient_id
),
frequent_pair as (
    select 
        D1, D2
    from(
        select
            D1, 
            D2,
            count(distinct ims_patient_id) as num_patient
        from (
            select
                A.ims_patient_id,
                A.4digit_icd as D1,
                B.4digit_icd as D2
            from
                cancer_patient_records as A
            join
                cancer_patient_records as B
            on
                A.ims_patient_id=B.ims_patient_id and A.4digit_icd<>B.4digit_icd 
                and DATEDIFF(B.diagnose_dt, A.diagnose_dt)>0 and DATEDIFF(B.diagnose_dt, A.diagnose_dt)<=365*3
        ) as A
        group by
            D1, D2
    ) as B
    where num_patient>7
)
select * from (
select
    A.D1, A.D2, A.D3, A.D4, num_patient
from (
    select
        D1, 
        D2,
        D3,
        D4,
        count(distinct ims_patient_id) as num_patient
    from (
        select *
        from (
            select
                A.ims_patient_id,
                A.4digit_icd as D1,
                B.4digit_icd as D2,
                C.4digit_icd as D3,
                D.4digit_icd as D4
            from
                cancer_patient_records as A
            join
                cancer_patient_records as B
            on
                A.ims_patient_id=B.ims_patient_id and A.4digit_icd<>B.4digit_icd 
                and DATEDIFF(B.diagnose_dt, A.diagnose_dt)>0 and DATEDIFF(B.diagnose_dt, A.diagnose_dt)<=365*3
            join
                cancer_patient_records as C
            on
                B.ims_patient_id=C.ims_patient_id and B.4digit_icd<>C.4digit_icd 
                and DATEDIFF(C.diagnose_dt, B.diagnose_dt)>0 and DATEDIFF(C.diagnose_dt, B.diagnose_dt)<=365*3
            join
                cancer_patient_records as D
            on
                C.ims_patient_id=D.ims_patient_id and C.4digit_icd<>D.4digit_icd 
                and DATEDIFF(D.diagnose_dt, C.diagnose_dt)>0 and DATEDIFF(D.diagnose_dt, C.diagnose_dt)<=365*3
                and D.4digit_icd='sarcoma-retroperitoneum'
            ) as A
        where D1<>D2 and D2<>D3 and D1<>D3 and D1<>D4 and D2<>D4 and D3<>D4 and D1<>'sarcoma-retroperitoneum' and D2<>'sarcoma-retroperitoneum'
        and D3<>'sarcoma-retroperitoneum' and D4='sarcoma-retroperitoneum'
        ) as A
    group by
        D1, D2, D3, D4
) as A
inner join 
    frequent_pair as B
on A.D1=B.D1 and A.D2=B.D2
inner join
    frequent_pair as C
on A.D2=C.D1 and A.D3=C.D2
inner join
    frequent_pair as D
on A.D3=D.D1 and A.D4=D.D2) as A
order by num_patient desc
