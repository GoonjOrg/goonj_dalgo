{{ config(
    materialized='table',
    tags=['kit', 'intermediate', 'salesforce']
) }}

SELECT DISTINCT
    "Id" AS kit_id,
    "Name" AS kit_name,
    "OwnerId" AS ownerid,
    type,
    "IsDeleted" AS isdeleted,
    remarks,
    "CreatedById" AS createdbyid,
    "CreatedDate" AS createddate,
    quantity,
    kit_source,
    kit_status,
    depreciated,
    "LastViewedDate" AS lastvieweddate,
    "SystemModstamp" AS systemmodstamp,
    kit_sub_type,
    "LastModifiedById" AS lastmodifiedbyid,
    "LastModifiedDate" AS lastmodifieddate,
    "LastReferencedDate" AS lastreferenceddate,
    current_quantity,
    kit_creation_date,
    original_quantity,
    processing_center,
    number_of_people_involved
FROM {{ ref('staging_kit') }}
ORDER BY kit_id