{{ config(
    materialized='table',
    tags=['disaster_boundaries', 'prod', 'salesforce']
) }}


select distinct
annual_year,
quarter,
month,
monthnum,
state,
district,
block,
other_block,
village,
other_village,
disaster_type,
type_of_initiative,
distribution_date,
distribution_name
FROM 
{{ ref('int_distributions') }} as distributions 
where is_deleted=False and disaster_type != 'None'



