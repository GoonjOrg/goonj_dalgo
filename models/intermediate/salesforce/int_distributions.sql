{{ config(
    materialized='table',
    tags=['distributions', 'intermediate', 'salesforce']
) }}

WITH base_dates AS (
    SELECT 
        *,
        EXTRACT(MONTH FROM distribution_date) as m_num,
        EXTRACT(YEAR FROM distribution_date) as y_num,
        TO_CHAR(distribution_date, 'Mon') as month_name
    FROM {{ ref('staging_distribution') }}
),

distribution_calculated_dates AS (
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

SELECT 
    dcd.distribution_id,
    dcd.distribution_name,
    dcd.state,
    dcd.district,
    dcd.block,
    dcd.other_block,
    dcd.village,
    dcd.other_village,
    dcd.tola_mohalla,
    dcd.distribution_date,
    dcd.type_of_community,
    dcd.type_of_initiative,
    account.account_name,
    case when account.account_name like '%Goonj%' then 'Self' else 'Partner' end as distributor_account_type,    
    dcd.remarks,
    dcd.reports_cross_checked,
    dcd.is_created_from_avni,
    case when dcd.disclaimer_photographs is not NULL then 'Yes' else 'No' end as disclaimer_photograph_present,
    case when dcd.receiver_list_photographs is not NULL then 'Yes' else 'No' end as receiver_list_photograph_present,
    case when dcd.photograph_information is not NULL then 'Yes' else 'No' end as photograph_information_present,
    dcd.photograph_information as distribution_photos,
    dcd.receiver_list_photographs as receiver_photos,
    dcd.disclaimer_photographs as disclaimer_photos,
    dcd.is_deleted,
    --S2S
    dcd.school_name,
    dcd.school_type,
    --Rahat & SI
    dcd.disaster_type,
    --Vaapsi
    dcd.reached_to,
    dcd.team_or_external,
    --specific initiative
    dcd.centre_name,
    dcd.is_rahat,
    dcd.brief_material_desc,
    dcd.how_material_diff,
    dcd.material_given_for,
    dcd.no_of_families_reached,
    dcd.no_of_individuals_reached,
    
    --Timestamps
    dcd.created_date,
    dcd.last_modified_date,
    dcd.last_modified_by_id,
    dcd.entered_by,
    dcd.created_by_id,
    dcd.created_by,

    -- Date Info
    dcd.annual_year,
    dcd.quarter,
    dcd.month_name as month,
    dcd.m_num as monthnum,

    case when dcd.created_by is null then users.name else dcd.created_by end as new_created_by, 
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
    i.dispatch_received_status_line_item,
    i.dispatch_received_status,
    i.purchased_created_received,
    i.source_of_material,
    i.bill_name,
    i.kit_type,
    i.sub_type,
    i.material_type,
    i.material_sub_category,
    i.other_material_name,
    i.purchase_kit_name,
    i.current_quantity,
    da.activity as activity_id,
    da.distribution_activity_name,
    da.distribution_activity_id,
    case when dcd.village !='Other' then 
        concat(dcd.state,'_',dcd.district,'_',dcd.block,'_',dcd.village) 
    end as geo_village,
    case when dcd.village ='Other' then 
        concat(dcd.state,'_',dcd.district,'_',dcd.block,'_',dcd.other_block,'_',dcd.other_village)
    end as geo_othervillage

FROM distribution_calculated_dates dcd  
LEFT JOIN {{ ref('staging_distribution_line') }} dl 
            ON dcd.distribution_id = dl.distribution_id   
LEFT JOIN {{ ref('staging_implementation_inventory') }} i 
            ON dl.implementation_inventory_id = i.implementation_inventory_id
LEFT JOIN {{ref('staging_account')}} account 
            ON dcd.account_name = account.account_id
LEFT JOIN {{ref('staging_distribution_activities')}} da
            ON dcd.distribution_id=da.distribution
LEFT JOIN {{ref('staging_users')}} users
            ON dcd.created_by_id=users.id