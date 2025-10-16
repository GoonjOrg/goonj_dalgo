{{ config(
    materialized='table',
    tags=['distribution_info', 'prod', 'salesforce']
) }}


select
annual_year,
quarter,
month,
state,
district,
block,
other_block,
village,
other_village,
tola_mohalla,
disaster_type,
distribution_name,
date_of_distribution,
type_of_community,
type_of_initiative,
account_name,
distributor_account_type,
school_name,
school_type,
reached_to,
is_rahat,
no_of_families_reached,
no_of_individuals_reached,
quantity,
unit,
kit_type,
sub_type,
material_type,
material_sub_category,
other_material_name,
purchase_kit_name

FROM 
{{ ref('int_distributions') }} as distributions 
where is_deleted=False


