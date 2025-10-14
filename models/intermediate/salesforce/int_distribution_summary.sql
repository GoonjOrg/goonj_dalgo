{{ config(
    materialized='table',
    tags=['distribution_summary', 'intermediate', 'salesforce']
) }}

WITH financial_year_data AS (
    SELECT *,
        CASE 
            WHEN EXTRACT(MONTH FROM date_of_distribution) >= 4 
            THEN EXTRACT(YEAR FROM date_of_distribution)::text || '-' || RIGHT((EXTRACT(YEAR FROM date_of_distribution) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM date_of_distribution) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM date_of_distribution)::text, 2)
        END AS financial_year
        -- Combine all village information including other villages
        --COALESCE(original_village_name, other_block, 'Unknown') as village_name
    FROM (
        SELECT 
            -- Distribution header information
            d.distribution_id,
            d.distribution_name,
            --d.age,
            --d.name_c,
            --d.gender,
            d.state,
            d.district,
            d.block,
            d.other_block,
            d.tola_mohalla,
            d.village,
            d.other_village,
            
            -- Personal information
            --d.father_mother_name,
            --d.phone_number,
            --d.monthly_income,
            --d.present_occupation,
            --d.no_of_family_members,
            
            -- Distribution specifics
            d.date_of_distribution,
            --d.distributed_to,
            d.material_given_for,
            --d.number_of_distributions,
            
            -- Community context
            d.type_of_community,
            d.type_of_initiative,
            --d.group_s_name,
            d.centre_name,
            
            -- Educational context (fields not available in staging_distribution)
            -- d.type_of_educational_entity,
            -- d.any_other_educational_subtype,
            -- d.school_aanganwadi_learning_center_name,
            
            -- Process tracking
            d.entered_by,
            d.surveyed_by,
            d.cross_checked_by,
            d.approved_verified_by,
            d.team_or_external,
            
            -- Impact metrics
            --d.count_of_da,
            d.no_of_families_reached,
            d.no_of_individuals_reached,
            --d.reached_to,
            
            -- Documentation
            d.picture_status,
            d.photograph_information,
            d.disclaimer_photographs,
            d.receiver_list_photographs,
            
            -- Integration
            d.is_created_from_avni,
            --d.da_indicator_for_avni,
            
            -- Quality control
            d.reports_cross_checked,
            d.monitored_by_distributor,
            
            -- Additional info
            d.remarks,
            --d.any_other_relevant_details,
            -- d.share_a_brief_provided_material,
            -- d.how_the_material_makes_a_difference,
            
            -- Point of contact
            --d.name_of_poc,
            --d.poc_contact_no,
            
            -- Distribution context (fields not available in staging_distribution)
            -- d.disaster_type,
            -- d.emergency_type,
            -- d.beneficiary_count,
            -- d.families_served,
            -- d.individuals_served,
            
            -- Distribution line item information
            dl.distribution_line_id,
            dl.distribution_line_name,
            dl.quantity,
            dl.unit,
            dl.distributed_to,
            dl.implementation_inventory_id,
            --dl.avni_implementation_inventory,
            dl.is_created_from_avni as line_is_created_from_avni,
            
            -- System fields
            --d."OwnerId" as distribution_owner_id,
            --d."IsDeleted" as distribution_is_deleted,
            --d.distribution_created_by_id,
            d.created_date as distribution_created_date,
            dl.created_date as distribution_line_created_date,
            d.last_modified_date as distribution_last_modified_date,
            dl.last_modified_date as distribution_line_last_modified_date

        FROM {{ ref('staging_distribution') }} d
        LEFT JOIN {{ ref('staging_distribution_line') }} dl 
            ON d.distribution_id = dl.distribution_id
        WHERE d.date_of_distribution >= '2021-04-01'
            AND d.date_of_distribution IS NOT NULL
    ) base_data
)

SELECT 
    distribution_id,
    distribution_name,
    state,
    district,
    block,
    other_block,
    village,
    other_village
    tola_mohalla,
    --father_mother_name,
    --phone_number,
    --monthly_income,
    --present_occupation,
    --no_of_family_members,
    date_of_distribution,
    --distributed_to,
    material_given_for,
    --number_of_distributions,
    type_of_community,
    type_of_initiative,
    --group_s_name,
    centre_name,
    --entered_by,
    --surveyed_by,
    --cross_checked_by,
    --approved_verified_by,
    team_or_external,
    --count_of_da,
    no_of_families_reached,
    no_of_individuals_reached,
    --reached_to,
    picture_status,
    photograph_information,
    disclaimer_photographs,
    receiver_list_photographs,
    is_created_from_avni,
    --da_indicator_for_avni,
    reports_cross_checked,
    --monitored_by_distributor,
    remarks,
    --any_other_relevant_details,
    --name_of_poc,
    --poc_contact_no,
    distribution_line_id,
    distribution_line_name,
    quantity,
    unit,
    distributed_to,
    implementation_inventory_id,
    --avni_implementation_inventory,
    line_is_created_from_avni,
    --distribution_owner_id,
    --distribution_is_deleted,
    --distribution_created_by_id,
    distribution_created_date,
    distribution_line_created_date,
    distribution_last_modified_date,
    distribution_line_last_modified_date,
    financial_year
FROM financial_year_data