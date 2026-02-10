{{ config(
    materialized='table',
    tags=['distributions_disasterlocations', 'prod', 'salesforce']
) }}


select distinct
disasterboundaries.disaster_type as disaster_disaster_type,
disasterboundaries.distribution_date as disaster_date,
disasterboundaries.type_of_initiative as disaster_type_of_initiative,
disasterboundaries.distribution_name as disaster_distribution_name,
distributions.*
FROM 
{{ ref('int_distributions') }} as distributions,
{{ref('prod_disaster_boundaries')}} as disasterboundaries   
where distributions.is_deleted=False 
and distributions.state=disasterboundaries.state
and distributions.district=disasterboundaries.district 
and distributions.block=disasterboundaries.block
and (distributions.other_block IS  NULL or distributions.other_block=  disasterboundaries.other_block) 
and distributions.village=disasterboundaries.village
and (distributions.other_village IS  NULL or distributions.other_village=disasterboundaries.other_village)
and distributions.distribution_date >   disasterboundaries.distribution_date
and distributions.disaster_type IS NULL
