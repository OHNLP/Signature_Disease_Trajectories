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
                    WHEN A.icd_code like 'C47.1%' or A.icd_code like 'C47.2%' or A.icd_code like 'C49.1%' or  A.icd_code like 'C49.2%' or A.icd_code like 'C50.9%' or A.icd_code in ('C47', 'C47.3', 'C47.6', 'C47.8', 'C47.9', 'C49', 'C49.3', 'C49.6', 'C49.8', 'C49.9') THEN 'sarcoma-trunk_extremities'
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
        where icd_type='2' and tumor_sub_type like '%SARCOMA' and 4digit_icd='sarcoma-trunk_extremities'
        ) as A
    inner join patient_records as B
    on A.ims_patient_id=B.ims_patient_id
),
first_cancer_records as (
    select ims_patient_id, min(diagnose_dt) as first_dt
    from cancer_patient_records as A
    where icd_type='2' and tumor_sub_type like '%SARCOMA' and 4digit_icd='sarcoma-trunk_extremities'
    group by ims_patient_id
),
pre_post_table as (
    select ims_patient_id, min(diagnose_dt) as earliest, max(diagnose_dt) as latest
    from cancer_patient_records as A
    group by ims_patient_id
)
select
    avg(pre_days), STDDEV(pre_days), avg(post_days), STDDEV(post_days)
from (
    select
        ims_patient_id, DATEDIFF(first_dt, earliest) as pre_days, DATEDIFF(latest, first_dt) as post_days
    from (
        select * 
        from (
            select
                A.ims_patient_id, earliest, latest, first_dt
            from pre_post_table as A
            left join first_cancer_records as B
            on A.ims_patient_id = B.ims_patient_id
        ) as tmp
        where earliest is not NUll and latest is not NULL and first_dt is not NULL
    ) as A
) as B