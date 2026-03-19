{{ config(
    materialized='table',
    tags=['cfw_activities', 'prod', 'salesforce']
) }}

WITH
country_avg_working_days as (
select
    annual_year,
    activity_type,
    activity_sub_type,
    activity_category,
    avg((num_working_days)) as avg_country_num_working_days
from
    {{ ref('int_activities') }}
where type_of_initiative='CFW'
group by annual_year, activity_type, activity_sub_type, activity_category
),

country_avg_participants as (
select
    annual_year,
    activity_type,
    activity_sub_type,
    activity_category,
    avg(num_cfw_female+ num_cfw_male + num_cfw_others) as avg_country_num_participants
from
    {{ ref('int_activities') }}
where type_of_initiative='CFW'
group by annual_year, activity_type, activity_sub_type, activity_category
),

state_avg_participants as (
select
    annual_year,
    state,
    activity_type,
    activity_sub_type,
    activity_category,
    avg(num_cfw_female+ num_cfw_male + num_cfw_others) as avg_state_num_participants
from
    {{ ref('int_activities') }}
where type_of_initiative='CFW'
group by annual_year, state, activity_type, activity_sub_type, activity_category

),

state_avg_working_days as (
select
    annual_year,
    state,
    activity_type,
    activity_sub_type,
    activity_category,
    avg((num_working_days)) as avg_state_num_working_days
from
    {{ ref('int_activities') }}
where type_of_initiative='CFW'
group by annual_year, state,activity_type, activity_sub_type, activity_category
)

select distinct
    annual_year,
    month,
    monthnum,
    quarter,
    state,
    district,
    account_name,
    account_type,
    type_of_initiative,
    activity_type,
    activity_category,
    activity_sub_type,
    objective_of_cfw_work,
    avg_country_num_working_days,
    avg_state_num_working_days,
    avg_country_num_participants,
    avg_state_num_participants,
    sum(number_of_activities) as num_activities,
    count(distinct activity_id) as activity_count,
    sum(num_cfw_female) as num_cfw_female,
    sum(num_cfw_male) as num_cfw_male,
    sum(num_cfw_others) as num_cfw_others,
    sum(num_working_days) as num_working_days
from 
{{ ref('int_activities') }}
left join country_avg_working_days using (annual_year, activity_type, activity_sub_type, activity_category)
left join country_avg_participants using (annual_year, activity_type, activity_sub_type, activity_category)
left join state_avg_working_days using (annual_year, state, activity_type, activity_sub_type, activity_category)
left join state_avg_participants using (annual_year, state, activity_type, activity_sub_type, activity_category)
where type_of_initiative='CFW'
Group by annual_year, month, monthnum,quarter, state, district, account_name, account_type, type_of_initiative, activity_type, activity_category, activity_sub_type, objective_of_cfw_work,avg_country_num_participants,avg_country_num_working_days,avg_state_num_participants,avg_state_num_working_days