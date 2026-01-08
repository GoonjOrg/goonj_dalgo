{{ config(
    materialized='table',
    tags=['disaster_distribution_receiver', 'prod', 'salesforce']
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
is_rahat,
no_of_families_reached,
no_of_individuals_reached,
is_created_from_avni,
sum(case when kit_type='CFW' AND sub_type in ('A','B','C','X')
            then distributions.quantity else null end) as num_family_kit_receivers,
sum( case when kit_type='CFW' and sub_type= 'L' then
            distributions.quantity else null end) as num_individual_kit_receivers,
sum(case when kit_type='S2S' then distributions.quantity else null end) as num_school_kit_receivers,
sum(case when kit_type='S2S-AW' then distributions.quantity else null end) as num_aw_school_kit_receivers,
max(case when kit_type is null then quantity else null end) as num_other_material_receivers
FROM 
{{ ref('int_distributions') }} as distributions 
where is_deleted=False and disaster_type != 'None'
group by annual_year, quarter, month, monthnum, state, district, block, other_block, village, other_village, tola_mohalla, disaster_type, distribution_name, date_of_distribution, type_of_community, type_of_initiative, account_name, distributor_account_type, school_name, school_type, is_rahat, no_of_families_reached, no_of_individuals_reached, is_created_from_avni



