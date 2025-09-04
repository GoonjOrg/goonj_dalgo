{{ config(
    materialized='table',
    tags=['distribution', 'staging', 'salesforce']
) }}

SELECT
    -- Standard Salesforce fields
    "Id" AS distribution_id,
    "Name",
    "OwnerId",
    "IsDeleted",
    "CreatedById",
    "CreatedDate",
    "LastModifiedById",
    "LastModifiedDate",
    "SystemModstamp",
    "RecordTypeId",
    
    -- Custom fields - Personal Information
    "Age__c" AS age,
    "Name__c" AS name_c,
    "Gender__c" AS gender,
    "Father_Mother_Name__c" AS father_mother_name,
    "Monthly_Income__c" AS monthly_income,
    "Present_Occupation__c" AS present_occupation,
    "No_of_family_members__c" AS no_of_family_members,
    "Phone_Number__c" AS phone_number,
    
    -- Location fields
    "State__c" AS state,
    "District__c" AS district,
    "Block__c" AS block,
    "Other_Block__c" AS other_block,
    "Locality_Village_Name__c" AS locality_village_name,
    "Tola_Mohalla__c" AS tola_mohalla,
    
    -- Distribution details
    "Date_Of_Distribution__c" AS date_of_distribution,
    "Distributed_To__c" AS distributed_to,
    "Material_given_for__c" AS material_given_for,
    "Type_of_Initiative__c" AS type_of_initiative,
    "Type_of_Community__c" AS type_of_community,
    "Reached_To__c" AS reached_to,
    
    -- Process and verification fields
    "Entered_by__c" AS entered_by,
    "Surveyed_By__c" AS surveyed_by,
    "Cross_checked_by__c" AS cross_checked_by,
    "Approved_Verified_By__c" AS approved_verified_by,
    "Team_or_External__c" AS team_or_external,
    "Monitored_By_Distributor__c" AS monitored_by_distributor,
    
    -- Contact and organization fields
    "Name_of_POC__c" AS name_of_poc,
    "POC_Contact_No__c" AS poc_contact_no,
    "Name_of_Account__c" AS name_of_account,
    "Group_s_Name__c" AS group_s_name,
    "Centre_s_Name__c" AS centre_s_name,
    
    -- Metrics and counts
    "Count_of_DA__c" AS count_of_da,
    "No_of_Families_Reached__c" AS no_of_families_reached,
    "No_Of_Individual_Reached__c" AS no_of_individuals_reached,
    "Number_of_distributions__c" AS number_of_distributions,
    
    -- Documentation and photos
    "Picture_Status__c" AS picture_status,
    "Reports_Cross_checked__c" AS reports_cross_checked,
    "Disclaimer_Photographs__c" AS disclaimer_photographs,
    "Photograph_information__c" AS photograph_information,
    "Receiver_List_Photographs__c" AS receiver_list_photographs,
    
    -- Additional details
    "Remarks__c" AS remarks,
    "Any_Other_Relevant_Details__c" AS any_other_relevant_details,
    
    -- System integration fields
    "Is_Created_from_Avni__c" AS is_created_from_avni,
    "DA_Indicator_For_Avni__c" AS da_indicator_for_avni,
    
    -- Airbyte metadata
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
FROM {{ source('staging_salesforce', 'distribution') }}