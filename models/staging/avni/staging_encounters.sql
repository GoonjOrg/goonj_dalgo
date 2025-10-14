-- Village Assessment Form encounters data extraction from Avni
{{ config(
    materialized='table',
    tags=['encounters', 'staging', 'avni', 'village_assessment', 'data_extraction']
) }}

SELECT
    -- Basic encounter information
    "ID" AS encounter_id,
    "Voided" AS is_voided,
    "Subject_ID" AS subject_id,
    "External_ID" AS external_id,
    "Subject_type" AS subject_type,
    "Encounter_type" AS encounter_type,
    "Cancel_date_time" AS cancel_date_time,
    "last_modified_at" AS last_modified_at,
    "Max_scheduled_date" AS max_scheduled_date,
    "Encounter_date_time" AS encounter_date_time,
    "Subject_external_ID" AS subject_external_id,
    "Earliest_scheduled_date" AS earliest_scheduled_date,
    
    -- Extract audit information
    "audit" ->> 'Created at' AS audit_created_at,
    "audit" ->> 'Created by' AS audit_created_by,
    "audit" ->> 'Last modified at' AS audit_last_modified_at,
    "audit" ->> 'Last modified by' AS audit_last_modified_by,
    
    -- Extract observations information
    "observations" ->> 'Households' AS observations_households,
    "observations" ->> 'Population' AS observations_population,
    "observations" ->> 'Date of Survey' AS observations_date_of_survey,
    "observations" -> 'Source of Data' AS observations_source_of_data,
    "observations" -> 'Community Types' AS observations_community_types,
    "observations" ->> 'Name of Surveyor' AS observations_name_of_surveyor,
    "observations" ->> 'Presence of SHGs' AS observations_presence_of_shgs,
    "observations" -> 'Common Health Issues' AS observations_common_health_issues,
    "observations" ->> 'Any Migration Patterns' AS observations_any_migration_patterns,
    "observations" ->> 'Emergency Contact Name' AS observations_emergency_contact_name,
    "observations" ->> 'Nearest Primary School' AS observations_nearest_primary_school,
    "observations" ->> 'Detail of previous work' AS observations_detail_of_previous_work,
    "observations" ->> 'Disaster Vulnerability?' AS observations_disaster_vulnerability,
    "observations" -> 'Nearest Health Facility' AS observations_nearest_health_facility,
    "observations" ->> 'Emergency Contact Number' AS observations_emergency_contact_number,
    "observations" ->> 'Any NGO is working there?' AS observations_any_ngo_is_working_there,
    "observations" ->> 'Partner Organisation Name' AS observations_partner_organisation_name,
    "observations" ->> 'Type of Road Connectivity' AS observations_type_of_road_connectivity,
    "observations" -> 'What Kind of Work is Needed?' AS observations_what_kind_of_work_is_needed,
    "observations" ->> 'Community Culture and Aspects' AS observations_community_culture_and_aspects,
    "observations" ->> 'Preferred quarter for working' AS observations_preferred_quarter_for_working,
    "observations" ->> 'Public Transport Availability' AS observations_public_transport_availability,
    "observations" ->> 'Previous Work in This Village?' AS observations_previous_work_in_this_village,
    "observations" -> 'Available Drinking Water Sources' AS observations_available_drinking_water_sources,
    "observations" ->> 'Details of Disaster Vulnerability' AS observations_details_of_disaster_vulnerability,
    "observations" ->> 'Availability of ASHA/Health Worker' AS observations_availability_of_asha_health_worker,
    "observations" -> 'Biggest Challenges Faced by the Village' AS observations_biggest_challenges_faced_by_the_village,
    "observations" ->> 'Is drinking water available year-round?' AS observations_is_drinking_water_available_year_round,
    "observations" -> 'Select the top three occupations in this village' AS observations_select_the_top_three_occupations_in_this_village,
    "observations" ->> 'Dispatch Status Id' AS observations_dispatch_status_id,
    "observations" ->> 'Dispatch Received Date' AS observations_dispatch_received_date,
    
    -- Location information
    "Cancel_location" AS cancel_location,
    "Encounter_location" AS encounter_location,
    "cancelObservations" AS cancel_observations,
    
    -- Material information (unnested) - using COALESCE to handle cases where material_item might be NULL
    COALESCE(ROW_NUMBER() OVER(PARTITION BY "ID" ORDER BY material_item.ordinality) - 1, 0) AS received_material_item_index,
    material_item.value ->> 'Kit Name' AS received_material_kit_name,
    material_item.value ->> 'Kit Type' AS received_material_kit_type,
    material_item.value ->> 'Sub type' AS received_material_kit_sub_type,
    material_item.value ->> 'Type Of Material' AS received_material_type_of_material,
    material_item.value ->> 'Quantity matching' AS received_material_quantity_matching,
    material_item.value ->> 'Dispatch Line Item Id' AS received_material_dispatch_line_item_id,
    CASE 
        WHEN material_item.value ->> 'Quantity as per dispatch' IS NOT NULL 
        THEN (material_item.value ->> 'Quantity as per dispatch')::INTEGER 
        ELSE NULL 
    END AS received_material_quantity_as_per_dispatch

FROM {{ source('staging_avni', 'encounters') }}
LEFT JOIN LATERAL jsonb_array_elements("observations" -> 'Received Material') WITH ORDINALITY AS material_item(value, ordinality) ON TRUE

WHERE 
    "Encounter_type" = 'Village Assessment Form'
    AND "ID" IS NOT NULL

ORDER BY "Encounter_date_time" DESC, "ID", received_material_item_index