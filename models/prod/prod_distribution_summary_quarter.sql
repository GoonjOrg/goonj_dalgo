{{ config(
    materialized='table',
    tags=['distribution_summary_quarter', 'prod', 'salesforce']
) }}


SELECT
    annual_year,
    quarter,
    state,
    type_of_initiative,
    disaster_type,
    kit_type,
    count(distinct (district,block)) as block_count,
    count(distinct (district,block,village)) as village_count,
    count(distinct (district, other_block)) as other_block_count,
    count(distinct (district,block,other_block,other_village))as other_village_count,
    count(distinct distribution_id) as distribution_count,
    count(distinct case distributor_account_type when 'Self' then distribution_id else null end) as num_self_distributions,
    count(distinct case distributor_account_type when 'Partner' then distribution_id else null end) as num_external_distributions,
    sum(case when kit_type::text is not NULL then quantity else 0 end) as kit_count
    
FROM 
{{ ref('int_distributions') }} as distributions 
where is_deleted=False
Group by annual_year, quarter, state, type_of_initiative, disaster_type, kit_type
