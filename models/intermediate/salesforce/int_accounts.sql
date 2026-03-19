-- Account data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['accounts', 'intermediate', 'salesforce']
) }}

SELECT
    -- Basic system information
    account_id,
    account_name,
    account_code,
    state,
    district,
    block,
    owner_id,
    created_by_id,
    created_date,
    last_modified_by_id,
    last_modified_date,
    last_viewed_date,
    last_referenced_date,
    system_modstamp,
    is_deleted,

    -- Account classification
    account_type,
    industry,
    account_source,
    parent_id,
    record_type_id,

    -- Contact information
    phone,
    fax,
    website,
    email,

    -- Business information
    annual_revenue,
    number_of_employees,
    description,
    rating,

    --For Sanjha Partners
    registered_with,
    contacted_date,
    date_of_registration,
    date_of_visit,
    description_of_scope_of_work,
    funding_information,
    has_fcra,
    last_fvr_notified,
    last_opf_notified,
    onboard_stage
    --amount,
    --partnership_type,


    -- Custom fields (common in Salesforce) - commented out if not available
    -- "Account_Status__c" AS account_status,
    -- "Organization_Type__c" AS organization_type,
    -- "Registration_Number__c" AS registration_number,
    -- "PAN_Number__c" AS pan_number,
    -- "GST_Number__c" AS gst_number,

FROM {{ ref('staging_account') }}