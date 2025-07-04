#!/bin/bash

# Source CSV file path
SOURCE_CSV="within_cohort_distribution.csv"

# Target CSV file path
TARGET_CSV="matched_cohort_P_D2.csv"


# Clear the target file or create it if it doesn't exist
> "$TARGET_CSV"

# Read the source CSV file line by line
while IFS=$'\t', read -r d1 d2 num_d1 num_d1_d2; do
    SQL_QUERY="with patient_records as (
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
                    WHEN A.icd_code like 'C25%' THEN 'pancreatic_cancer'
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
        where icd_type='2' and 4digit_icd='pancreatic_cancer'
        ) as A
    inner join patient_records as B
    on A.ims_patient_id=B.ims_patient_id
),
exposed_group as (
    select *
    from cancer_patient_records
    where 4digit_icd='$d1'
),
3_year_table as (
    select exp_patient, exp_diagnose_dt, B.*
    from (
        select distinct E.ims_patient_id as exp_patient, E.diagnose_dt as exp_diagnose_dt, C.ims_patient_id as ims_patient_id
        from exposed_group as E
        inner join patient_records as C
        on E.age=C.age and E.gender=C.gender and E.ims_patient_id<>C.ims_patient_id
    ) as A
    inner join patient_records as B
    on A.ims_patient_id=B.ims_patient_id and DATEDIFF(B.diagnose_dt, A.exp_diagnose_dt)>0 and DATEDIFF(B.diagnose_dt, A.exp_diagnose_dt)<=365*3
),
total_3_year_table as (
    select exp_patient, count(distinct ims_patient_id) as total_3_year
    from 3_year_table
    group by exp_patient
),
D2_3_year_table as (
    select exp_patient, count(distinct ims_patient_id) as D2_3_year
    from 3_year_table
    where 4digit_icd='$d2'
    group by exp_patient
),
P_D2_3_year_table as (
    select avg(conditional_probability) as P_D2
    from (
        select *
        from (
            select A.exp_patient, isnull(total_3_year, 0) as total_3_year, isnull(D2_3_year, 0) as D2_3_year, isnull(1.0*D2_3_year/total_3_year, 0) as conditional_probability
            from total_3_year_table as A
            left join D2_3_year_table as B
            on A.exp_patient=B.exp_patient) as tmp
        where total_3_year>=2000
    ) as tmp
)
SELECT
    T1.P_D2 AS P_D2_3_year
FROM
    (SELECT P_D2 FROM P_D2_3_year_table) AS T1;"
    QUERY_RESULT=$(impala-shell -i sbhadaplpn01:21000 -q "$SQL_QUERY" -B)
    echo "$QUERY_RESULT"
    QUERY_RESULT_CSV=$(echo "$QUERY_RESULT" | tr '\n' ',')
    QUERY_RESULT_CSV=$(echo "$QUERY_RESULT" | tr '\t' ',')
    echo "$d1,$d2,${QUERY_RESULT_CSV%,}" >> "$TARGET_CSV"
done < "$SOURCE_CSV"
echo "Process completed. Results saved in $TARGET_CSV"