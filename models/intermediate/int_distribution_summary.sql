{{ config(
    materialized='table',
    tags=['distribution_summary', 'intermediate', 'salesforce']
) }}

SELECT 
    -- Distribution header information
    d.distribution_id,
    d."Name" as distribution_name,
    d.age,
    d.name_c,
    d.gender,
    d.state,
    d.district,
    d.block,
    d.other_block,
    d.tola_mohalla,
    d.locality_village_name,
    
    -- Personal information
    d.father_mother_name,
    d.phone_number,
    d.monthly_income,
    d.present_occupation,
    d.no_of_family_members,
    
    -- Distribution specifics
    d.date_of_distribution,
    d.distributed_to,
    d.material_given_for,
    d.number_of_distributions,
    
    -- Community context
    d.type_of_community,
    d.type_of_initiative,
    d.group_s_name,
    d.centre_s_name,
    
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
    d.count_of_da,
    d.no_of_families_reached,
    d.no_of_individuals_reached,
    d.reached_to,
    
    -- Documentation
    d.picture_status,
    d.photograph_information,
    d.disclaimer_photographs,
    d.receiver_list_photographs,
    
    -- Integration
    d.is_created_from_avni,
    d.da_indicator_for_avni,
    
    -- Quality control
    d.reports_cross_checked,
    d.monitored_by_distributor,
    
    -- Additional info
    d.remarks,
    d.any_other_relevant_details,
    -- d.share_a_brief_provided_material,
    -- d.how_the_material_makes_a_difference,
    
    -- Point of contact
    d.name_of_poc,
    d.poc_contact_no,
    
    -- Distribution context (fields not available in staging_distribution)
    -- d.disaster_type,
    -- d.emergency_type,
    -- d.beneficiary_count,
    -- d.families_served,
    -- d.individuals_served,
    
    -- Distribution line item information
    dl.distribution_line_id,
    dl."Name" as distribution_line_name,
    dl.quantity,
    dl.unit,
    dl.dispatched_to,
    dl.implementation_inventory,
    dl.avni_implementation_inventory,
    dl.is_created_from_avni as line_is_created_from_avni,
    
    -- System fields
    d."OwnerId" as distribution_owner_id,
    d."IsDeleted" as distribution_is_deleted,
    d."CreatedById" as distribution_created_by_id,
    d."CreatedDate" as distribution_created_date,
    dl."CreatedDate" as distribution_line_created_date,
    d."LastModifiedDate" as distribution_last_modified_date,
    dl."LastModifiedDate" as distribution_line_last_modified_date

FROM {{ ref('staging_distribution') }} d
LEFT JOIN {{ ref('staging_distribution_line') }} dl 
    ON d.distribution_id = dl.distribution_id