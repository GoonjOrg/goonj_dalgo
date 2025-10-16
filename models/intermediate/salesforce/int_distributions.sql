{{ config(
    materialized='table',
    tags=['distributions', 'intermediate', 'salesforce']
) }}


SELECT 
    d.distribution_id,
    d.distribution_name,
    d.state,
    d.district,
    d.block,
    d.other_block,
    d.village,
    d.other_village,
    d.tola_mohalla,
    d.date_of_distribution,
    d.type_of_community,
    d.type_of_initiative,
    account.account_name,
    case when account.account_name like '%Goonj%' then 'Self' else 'Partner' end as distributor_account_type,    
    d.remarks,
    d.reports_cross_checked,
    d.is_created_from_avni,
    case when d.disclaimer_photographs is not NULL then 'Yes' else 'No' end as disclaimer_photograph_present,
    case when d.receiver_list_photographs is not NULL then 'Yes' else 'No' end as receiver_list_photograph_present,
    case when d.photograph_information is not NULL then 'Yes' else 'No' end as photograph_information_present,
    d.is_deleted,
    --S2S
    d.school_name,
    d.school_type,
    --Rahat & SI
    d.disaster_type,
    --Vaapsi
    d.reached_to,
    d.team_or_external,
    --specific initiative
    d.centre_name,
    d.is_rahat,
    d.brief_material_desc,
    d.how_material_diff,
    d.material_given_for,
    d.no_of_families_reached,
    d.no_of_individuals_reached,
    CASE 
        WHEN EXTRACT(MONTH FROM d.date_of_distribution) >= 4 
            THEN EXTRACT(YEAR FROM d.date_of_distribution)::text || '-' || RIGHT((EXTRACT(YEAR FROM date_of_distribution) + 1)::text, 2)
        ELSE (EXTRACT(YEAR FROM d.date_of_distribution) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM date_of_distribution)::text, 2)
    END AS annual_year,
    CASE 
    WHEN EXTRACT(MONTH FROM date_of_distribution) BETWEEN 4 AND 6 THEN 'Q1'
    WHEN EXTRACT(MONTH FROM date_of_distribution) BETWEEN 7 AND 9 THEN 'Q2'
    WHEN EXTRACT(MONTH FROM date_of_distribution) BETWEEN 10 AND 12 THEN 'Q3'
    WHEN EXTRACT(MONTH FROM date_of_distribution) BETWEEN 1 AND 3 THEN 'Q4'
    END AS quarter,
    TO_CHAR(date_of_distribution, 'Mon') as month,
    
    --Timestamps
    d.created_date,
    d.last_modified_date,

    dl.distribution_line_id,
    dl.distribution_line_name,
    dl.quantity,
    dl.unit,
    dl.distributed_to,
    dl.implementation_inventory_id,
    dl.is_created_from_avni as line_is_created_from_avni,

    i.implementation_inventory_name,
    i.center_field_office as inventory_office,
    i.material_kit_name,
    i.dispatch_id,
    i.dispatch_line_item_id,
    i.purchased_created_received,
    i.source_of_material,
    i.bill_name,
    i.kit_type,
    i.sub_type,
    i.material_type,
    i.material_sub_category,
    i.other_material_name,
    i.purchase_kit_name


FROM {{ ref('staging_distribution') }} d  
LEFT JOIN {{ ref('staging_distribution_line') }} dl 
            ON d.distribution_id = dl.distribution_id   
LEFT JOIN {{ ref('staging_implementation_inventory') }} i 
            ON dl.implementation_inventory_id = i.implementation_inventory_id
left join
{{ref('staging_account')}} account on d.account_name = account.account_id

where d.is_deleted=False