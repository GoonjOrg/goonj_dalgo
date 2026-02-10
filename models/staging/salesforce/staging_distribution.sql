{{ config(
    materialized='table',
    tags=['distribution', 'staging', 'salesforce']
) }}

SELECT
    -- Standard Salesforce fields
    "Id" AS distribution_id,
    "Name" AS distribution_name,
    "OwnerId" AS owner_id,
    "IsDeleted" AS is_deleted,
    "CreatedById" AS created_by_id,
    "Created_By__c" AS created_by,
    "CreatedDate" AS created_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "SystemModstamp" AS system_mod_stamp,
    "RecordTypeId" AS record_type_id,
    
    -- Location fields
    "State__c" AS state,
    "District__c" AS district,
    "Block__c" AS block,
    "Other_Block__c" AS other_block,
    "Locality_Village_Name__c" AS village,
    "Other__c" AS other_village,
    "Tola_Mohalla__c" AS tola_mohalla,
    
    -- Basic details
    "Date_Of_Distribution__c" AS distribution_date,
    "Type_of_Initiative__c" AS type_of_initiative,
    "Name_of_Account__c" AS account_name,
    "Type_of_Community__c" AS type_of_community,

    -- Process and verification fields
    "Cross_checked_by__c" AS cross_checked_by,
    "Entered_by__c" AS entered_by,
    

    -- Documentation and photos
    "Remarks__c" AS remarks,
    "Picture_Status__c" AS picture_status,
    "Reports_Cross_checked__c" AS reports_cross_checked,
    "Disclaimer_Photographs__c" AS disclaimer_photographs,
    "Photograph_information__c" AS photograph_information,
    "Receiver_List_Photographs__c" AS receiver_list_photographs,

    --S2S & Education and Health
    "School_Aanganwadi_Learning_Center_Name__c" AS school_name,
    "Type_of_educational_entity__c" AS school_type,

    --Rahat & SI
    "Disaster_Type__c" AS disaster_type,

    --Vapsi
    "Surveyed_By__c" AS surveyed_by,
    "Monitored_By_Distributor__c" AS monitored_by_distributor,
    "Approved_Verified_By__c" AS approved_verified_by,
    "Team_or_External__c" AS team_or_external,
    "Name_of_POC__c" AS poc_name,
    "POC_Contact_No__c" AS poc_number,
    "Reached_To__c" AS reached_to,
    "Any_Other_Document_Submitted__c" AS other_documents,

    --specific initiative
    "Centre_s_Name__c" AS centre_name,
    "Share_a_Brief_Provided_Material__c" AS brief_material_desc,
    "How_the_Material_Makes_a_Difference__c" AS how_material_diff,
    "The_material_provided_as_part_of_Rahat__c" AS is_rahat,
    "Material_given_for__c" AS material_given_for,
    "No_of_Families_Reached__c" AS no_of_families_reached,
    "No_Of_Individual_Reached__c" AS no_of_individuals_reached,
    "Other_Details__c" AS other_si_details,


    -- System integration fields
    "Is_Created_from_Avni__c" AS is_created_from_avni,
    
    -- Airbyte metadata
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
FROM {{ source('staging_salesforce', 'distribution') }}
WHERE 
        "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False
