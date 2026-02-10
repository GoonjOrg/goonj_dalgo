{{ config(
    materialized='table',
    tags=['activities', 'intermediate', 'salesforce', 'detail_level', 'business_logic']
) }}


WITH base_dates AS (
    SELECT 
        *,
        EXTRACT(MONTH FROM activity_end_date) as m_num,
        EXTRACT(YEAR FROM activity_end_date) as y_num,
        TO_CHAR(activity_end_date, 'Mon') as month_name
    FROM {{ ref('staging_activity') }}
),
activity_calculated_dates AS (
    SELECT 
        *,
        CASE 
            WHEN m_num >= 4 THEN y_num::text || '-' || RIGHT((y_num + 1)::text, 2)
            ELSE (y_num - 1)::text || '-' || RIGHT(y_num::text, 2)
        END AS annual_year,
        CASE 
            WHEN m_num BETWEEN 4 AND 6 THEN 'Q1'
            WHEN m_num BETWEEN 7 AND 9 THEN 'Q2'
            WHEN m_num BETWEEN 10 AND 12 THEN 'Q3'
            WHEN m_num BETWEEN 1 AND 3 THEN 'Q4'
        END AS quarter
    FROM base_dates
)


select 
acd.state,
acd.district,
acd.block,
acd.village,
acd.other_block,
acd.other_village,
activity_id,
activity_name,
activity_start_date,
activity_end_date,
annual_year,
quarter,
month_name as month,
m_num as monthnum,

type_of_initiative,
activity_type,
activity_category,
activity_sub_type,
other_sub_type,
objective_of_cfw_work,
other_objective,
target_community,
type_of_educational_entity,
measurement_type,
length,
breadth,
numbers,
diameter,
depth_height,
    
acd.is_created_from_avni,
acd.created_date,
acd.last_modified_date,
acd.created_by_id,
acd.created_by,
acd.last_modified_by_id,
COALESCE(acd.created_by, users.name) as new_created_by,
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
CASE WHEN acd.before_photo IS NULL OR acd.before_photo = '' THEN 0 
    ELSE LENGTH(acd.before_photo) - LENGTH(REPLACE(acd.before_photo, ';', '')) + 1 END AS beforephotocount,
CASE WHEN acd.during_photo IS NULL OR acd.during_photo = '' THEN 0 
    ELSE LENGTH(acd.during_photo) - LENGTH(REPLACE(acd.during_photo, ';', '')) + 1 END AS duringphotocount,
CASE WHEN acd.after_photo IS NULL OR acd.after_photo = '' THEN 0 
    ELSE LENGTH(acd.after_photo) - LENGTH(REPLACE(acd.after_photo, ';', '')) + 1 END AS afterphotocount

FROM activity_calculated_dates acd
LEFT JOIN {{ref('staging_account')}} account 
    ON acd.account_name = account.account_id
LEFT JOIN {{ref('staging_users')}} users
    ON acd.created_by_id=users.id   