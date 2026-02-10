{{ config(
    materialized='table',
    tags=['activities_disasterlocations', 'prod', 'salesforce']
) }}


select distinct
disasterboundaries.annual_year as disaster_annual_year,
disasterboundaries.quarter as disaster_quarter,
disasterboundaries.month as disaster_month,
disasterboundaries.monthnum as disaster_monthnum,
disasterboundaries.disaster_type as disaster_disaster_type,
disasterboundaries.distribution_date as disaster_date,
disasterboundaries.type_of_initiative as disaster_type_of_initiative,
disasterboundaries.distribution_name as disaster_distribution_name,
activities.annual_year,
activities.month,
activities.monthnum,
activities.quarter,
activities.state,
activities.district,
activities.block,
activities.other_block,
activities.village,
activities.other_village,
account_name,
account_type,
activity_type_of_initiative,
created_by,
created_date,
activity_name,
activity_type,
activity_category,
activity_sub_type,
activity_start_date,
activity_end_date,
objective_of_cfw_work
activity_id,
is_education_and_health,
number_of_activities,
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
distribution_id,
activities.distribution_name,
activities.distribution_date,
activities.disaster_type,
activities.distribution_type_of_initiative,

case when activities.distribution_name=disasterboundaries.distribution_name 
then True else False end as disaster_related_activity
FROM 
{{ ref('prod_activities_distribution') }} as activities,
{{ref('prod_disaster_boundaries')}} as disasterboundaries   
where activities.state=disasterboundaries.state
and activities.district=disasterboundaries.district 
and activities.block=disasterboundaries.block
and (activities.other_block is null or activities.other_block=  disasterboundaries.other_block) 
and activities.village=disasterboundaries.village
and (activities.other_village is null or activities.other_village=disasterboundaries.other_village)