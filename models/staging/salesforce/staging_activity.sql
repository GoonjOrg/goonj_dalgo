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
    -- Basic system information (like file headers)
    "Id",                                    -- Unique number for each activity
    "Name",                                  -- What the activity is called
    "OwnerId",                               -- Who is responsible for this activity
    "CreatedById",                           -- Who created this record
    "CreatedDate",                           -- When this was first recorded
    "LastModifiedById",                      -- Who last changed this record
    "LastModifiedDate",                      -- When it was last changed
    "LastViewedDate",                        -- When someone last looked at it
    "LastReferencedDate",                    -- When it was last mentioned
    "SystemModstamp",                        -- System timestamp for tracking changes
    "IsDeleted",                             -- Is this record still valid? (Yes/No)

    -- Location information (where the activity happened)
    "Block__c" AS block,                     -- Administrative block level
    "Other_Block__c" AS other_block,         -- Additional block details
    "State__c" AS state,                     -- Which state (like Maharashtra, Bihar)
    "District__c" AS district,               -- Which district within the state
    -- "Village__c" AS village,                 -- Which village or locality (column not available)
    "Tola_Mohalla__c",                       -- Neighborhood or community level
    "Other__c" AS other,                     -- Other location details

    -- Activity classification (what type of activity)
    "Activity_Type__c",                      -- Main type (like "education" or "health")
    "Activity_Category__c",                  -- Main category
    "Activity_Sub_Type__c",                  -- Specific type (like "math class")
    "Type_of_Initiative__c",                 -- What kind of initiative
    "Objective_of_DFW_work__c",             -- Development Field Worker objective
    "Target_Community__c",                   -- Which community is targeted
    "Type_of_educational_entity__c",         -- What kind of educational institution
    "Measurement_Type__c",                   -- How success is measured

    -- Student involvement (did students participate?)
    "Activity_Conducted_With_Students__c",   -- Yes/No: were students involved?
    "Number_of_students_Male__c" AS male_students,    -- How many boys participated
    "Number_of_students_Female__c" AS female_students, -- How many girls participated

    -- Photo documentation (visual records)
    "S2S_Photograph_Information__c" AS s2s_photo_info,     -- Student-to-student photo details
    "NJPC_Photograph_Information__c" AS njpc_photo_info,   -- Not Just A Piece of Cloth photo documentation
    "Before_Implementation_Photograph__c" AS before_photo,  -- Photos before the activity
    "During_Implementation_Photograph__c" AS during_photo,  -- Photos during the activity
    "After_Implementation_Photograph__c" AS after_photo,    -- Photos after the activity

    -- Quality and compliance (did they follow rules?)
    "Form_Cross_Checked__c",                 -- Yes/No: were forms verified?
    "Was_Disclaimer_Form_Filled__c",        -- Yes/No: was disclaimer completed?
    "Is_Created_from_Avni__c",              -- Yes/No: was this created via mobile app?

    -- Additional counts and metrics
    "Count_of_DA__c",                        -- How many Distribution Activities involved
    "Number_of_Activities__c",               -- How many smaller activities
    "No_of_participants_Others_CFW__c" AS cfw_participants,  -- Cloth For Work participants
    "No_of_participants_Others_NJPC__c" AS njpc_participants, -- Not Just A Piece of Cloth participants

    -- Additional information
    "Remarks__c" AS remarks,                 -- Notes and comments
    "Education_and_Health__c",               -- Education and health details
    "Name_of_Account__c" AS account_name,    -- Associated account name

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id      -- Identifier from our data integration system

FROM {{ source('staging_salesforce', 'activity') }}

-- Basic quality checks to ensure we have good data to work with
-- This is like checking that papers aren't torn or missing important information
WHERE
    -- Don't include deleted records (like not counting torn-up papers)
    "IsDeleted" = FALSE

    -- Make sure we have the basic information we need
    AND "Id" IS NOT NULL
    AND "CreatedDate" IS NOT NULL

    -- Don't include completely empty or invalid records
    AND "Name" IS NOT NULL
    AND "Name" != ''

-- Organize by creation date (newest first) and then by ID
-- This is like organizing papers by date, newest on top
ORDER BY "CreatedDate" DESC, "Id"
