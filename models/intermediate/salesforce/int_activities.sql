{{ config(
    materialized='table',
    tags=['activities', 'intermediate', 'salesforce', 'detail_level', 'business_logic']
) }}


select
a.state,
a.district,
a.block,
a.village,
a.other_block,
a.other_village,
a.activity_start_date,
a.activity_end_date,
CASE 
    WHEN EXTRACT(MONTH FROM activity_end_date) >= 4 
        THEN EXTRACT(YEAR FROM activity_end_date)::text || '-' || RIGHT((EXTRACT(YEAR FROM activity_end_date) + 1)::text, 2)
    ELSE (EXTRACT(YEAR FROM activity_end_date) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM activity_end_date)::text, 2)
END AS annual_year,
CASE 
    WHEN EXTRACT(MONTH FROM activity_end_date) BETWEEN 4 AND 6 THEN 'Q1'
    WHEN EXTRACT(MONTH FROM activity_end_date) BETWEEN 7 AND 9 THEN 'Q2'
    WHEN EXTRACT(MONTH FROM activity_end_date) BETWEEN 10 AND 12 THEN 'Q3'
    WHEN EXTRACT(MONTH FROM activity_end_date) BETWEEN 1 AND 3 THEN 'Q4'
END AS quarter,
TO_CHAR(activity_end_date, 'Mon') as month,
EXTRACT(MONTH FROM activity_end_date) as monthnum,
type_of_initiative,
activity_type,
activity_category,
activity_sub_type,
objective_of_cfw_work,
target_community,
type_of_educational_entity,
measurement_type,
activity_id,
activity_name,
a.created_date,
a.last_modified_date,
a.created_by_id,
remarks,
is_education_and_health,
case when type_of_initiative='CFW' then number_of_activities else 1 end as number_of_activities,
school_name,
account.account_name,
case when account.account_name like '%Goonj%' then 'Self' else 'Partner' end as account_type,
num_working_days,
num_cfw_female,
num_cfw_male,
num_cfw_others,
actvity_conducted_with_students,
num_s2s_participants,
num_s2s_days,
num_njpc_days,
num_njpc_female,
num_njpc_male,
num_njpc_others,
created_by

from {{ ref('staging_activity') }} a
left join
{{ref('staging_account')}} account on a.account_name = account.account_id

where a.is_deleted=False 