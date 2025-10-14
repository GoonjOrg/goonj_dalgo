-- Village subjects data extraction from Avni
{{ config(
    materialized='table',
    tags=['subjects', 'staging', 'avni', 'villages', 'data_extraction']
) }}

SELECT
    -- Basic subject information
    "ID" AS subject_id,
    "Voided" AS is_voided,
    "External_ID" AS external_id,
    "Location_ID" AS location_id,
    "Subject_type" AS subject_type,
    "last_modified_at" AS last_modified_at,
    "Registration_date" AS registration_date,
    
    -- JSON fields (keeping as JSON for complex structures)
    "Groups" AS groups,
    "relatives" AS relatives,
    "catchments" AS catchments,
    "encounters" AS encounters,
    "enrolments" AS enrolments,
    "Registration_location" AS registration_location,
    
    -- Extract audit information
    "audit" ->> 'Created at' AS audit_created_at,
    "audit" ->> 'Created by' AS audit_created_by,
    "audit" ->> 'Last modified at' AS audit_last_modified_at,
    "audit" ->> 'Last modified by' AS audit_last_modified_by,
    
    -- Extract location information
    "location" ->> 'State' AS location_state,
    "location" ->> 'District' AS location_district,
    "location" ->> 'Block' AS location_block,
    "location" ->> 'Village' AS location_village,
    "location" ->> 'State External ID' AS location_state_external_id,
    "location" ->> 'District External ID' AS location_district_external_id,
    "location" ->> 'Block External ID' AS location_block_external_id,
    "location" ->> 'Village External ID' AS location_village_external_id,
    
    -- Extract observations information
    "observations" ->> 'DemandId' AS observations_demand_id,
    "observations" ->> 'AccountId' AS observations_account_id,
    "observations" ->> 'First name' AS observations_first_name,
    "observations" ->> 'Other Village' AS observations_other_village,
    "observations" ->> 'Account  name' AS observations_account_name,
    "observations" ->> 'Demand Status' AS observations_demand_status,
    "observations" ->> 'Dispatch Status' AS observations_dispatch_status,
    ("observations" ->> 'Number of people')::INTEGER AS observations_number_of_people,
    "observations" ->> 'Type of Disaster' AS observations_type_of_disaster,
    
    -- Extract target community (keeping as JSON array)
    "observations" -> 'Target Community' AS observations_target_community

FROM {{ source('staging_avni', 'subjects') }}

WHERE 
    "Subject_type" = 'Village'
    AND "ID" IS NOT NULL
    
ORDER BY "Registration_date" DESC, "ID"