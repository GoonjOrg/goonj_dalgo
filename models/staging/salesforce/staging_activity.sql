/*
WHAT THIS DOES: This script extracts raw activity data from Salesforce with minimal transformation:
- Pulls all activity records from the Salesforce Activity__c object
- Applies basic data quality filters to remove invalid records
- Preserves original field names and data structure
- Organizes data by creation date for logical processing
- Sets up the foundation for all intermediate and production models



*/
{{ config(
    materialized='table',
    tags=['activities', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

-- Get raw activity data from Salesforce with minimal changes
-- This is like carefully picking up papers without crumpling them
SELECT
    --Common
    "Id" as activity_id,                                    -- Unique number for each activity
    "Name" as activity_name,                                  -- What the activity is called
    "Name_of_Account__c" AS account_name,    -- Associated account name
    "Type_of_Initiative__c" AS type_of_initiative,                 -- What kind of initiative
    "Activity_End_Date__c" AS activity_end_date,
    "Activity_Start_Date__c" AS activity_start_date,
    "School_Aanganwadi_Learning_Center_Name__c" as school_name,

    
    -- Location information (where the activity happened)
    "Block__c" AS block,                     -- Administrative block level
    "Other_Block__c" AS other_block,         -- Additional block details
    "State__c" AS state,                     -- Which state (like Maharashtra, Bihar)
    "District__c" AS district,               -- Which district within the state
    "Locality_Village_Name__c" AS village,                 -- Which village or locality (column not available)
    "Tola_Mohalla__c" AS tola_mohalla,       -- Neighborhood or community level
    "Other__c" AS other_village,                     -- Other location details

    "Is_Created_from_Avni__c" AS is_created_from_avni,              -- Yes/No: was this created via mobile app?
    "Remarks__c" AS remarks,                 -- Notes and comments



    "OwnerId" as owner_id,                               -- Who is responsible for this activity
    "CreatedById" as created_by_id,                           -- Who created this record
    "Created_By__c" as created_by,
    "CreatedDate" as created_date,                           -- When this was first recorded
    "LastModifiedById" as last_modified_by_id,                      -- Who last changed this record
    "LastModifiedDate" as last_modified_date,                      -- When it was last changed
    "LastViewedDate" as last_viewed_date,                        -- When someone last looked at it
    "LastReferencedDate" as last_referenced_date,                    -- When it was last mentioned
    "SystemModstamp" as system_mod_stamp,                        -- System timestamp for tracking changes
    "IsDeleted" as is_deleted,                             -- Is this record still valid? (Yes/No)


    -- CFW
    "Activity_Type__c" AS activity_type,                      -- Main type (like "education" or "health")
    "Activity_Category__c" AS activity_category,                  -- Main category
    "Activity_Sub_Type__c" AS activity_sub_type,  
    "Other_Sub_Type__c" AS other_sub_type,                
    "Objective_of_DFW_work__c" AS objective_of_cfw_work,             -- Development Field Worker objective
    "Other_Objective__c" AS other_objective,
    "Target_Community__c" AS target_community,
    "Number_of_Activities__c" as number_of_activities,               -- How many smaller activities
    "Measurement_Type__c" AS measurement_type,    
    "Length__c" as length,
    "Breadth__c" as breadth,
    "Nos__c" as numbers,
    "Diameter__c" as diameter,
    "Depth_Height__c" as depth_height,
    "No_of_Working_Days__c" as num_working_days,
    "No_of_participants_Female_DFW__c" as num_cfw_female,
    "No_of_participants_Male_DFW__c" as num_cfw_male,
    "No_of_participants_Others_CFW__c" as num_cfw_others,

    -- S2S
    "Type_of_educational_entity__c" AS type_of_educational_entity,         -- What kind of educational institution
    "Activity_Conducted_With_Students__c" AS actvity_conducted_with_students,   -- Yes/No: were students involved?
    "No_of_participants_S2S__c" As num_s2s_participants,
    "No_of_days_of_Participation_S2S__c" AS num_s2s_days,

    --NJPC
    "Education_and_Health__c" as is_education_and_health, 
    "No_of_days_of_Participation_NJPC__c" as num_njpc_days,
    "No_of_participants_Female_NJPC__c" as num_njpc_female,
    "No_of_participants_Male_NJPC__c" as num_njpc_male,
    "No_of_participants_Others_NJPC__c" as num_njpc_others,

    -- Photo documentation (visual records)
    "S2S_Photograph_Information__c" AS s2s_photo_info,     -- Student-to-student photo details
    "NJPC_Photograph_Information__c" AS njpc_photo_info,   -- Not Just A Piece of Cloth photo documentation
    "Before_Implementation_Photograph__c" AS before_photo,  -- Photos before the activity
    "During_Implementation_Photograph__c" AS during_photo,  -- Photos during the activity
    "After_Implementation_Photograph__c" AS after_photo,    -- Photos after the activity

    -- Quality and compliance (did they follow rules?)
    "Form_Cross_Checked__c" AS form_cross_checked,                 -- Yes/No: were forms verified?
    "Was_Disclaimer_Form_Filled__c" AS was_disclaimer_form_filled,        -- Yes/No: was disclaimer completed?

    -- Additional counts and metrics
    "Count_of_DA__c" as count_of_da,                        -- How many Distribution Activities involved

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id      -- Identifier from our data integration system

FROM {{ source('staging_salesforce', 'activity') }}

-- Basic quality checks to ensure we have good data to work with
-- This is like checking that papers aren't torn or missing important information
WHERE
    -- Don't include deleted records (like not counting torn-up papers)
   "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False