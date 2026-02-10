{{ config(
    materialized='table',
    tags=['dispatch_status', 'staging', 'salesforce']
) }}

SELECT
    "Id" as dispatch_id,
    "Name" as dispatch_name,
    "POC__c" AS poc_id,
    "OwnerId" AS owner_id,
    "Rate__c" AS rate,
    "Block__c" AS block,
    "Demand__c" AS demand_id,
    "IsDeleted" AS is_deleted,
    "Account__c" AS receiving_account_id,
    "Contact__c" AS contact_id,
    "Remarks__c" AS remarks,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Driver_name__c" AS driver_name,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_mod_stamp,
    "Transporter__c" AS transporter,
    "Goonj_office__c" AS goonj_office,
    "Local_Demand__c" AS local_demand,
    "Disaster_Type__c" AS disaster_type,
    "Dispatch_City__c" AS city,
    "Dispatch_Date__c" AS dispatch_date,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Dispatch_State__c" AS state,
    "Vehicle_Number__c" AS vehicle_number,
    "Dispatch_Street__c" AS street,
    "Internal_Demand__c" AS internal_demand,
    "LastReferencedDate" AS last_referenced_date,
    "Name_of_the_POC__c" AS name_of_poc,
    "Dispatch_Address__c" AS address_id,
    "Dispatch_Country__c" AS country,
    "Dispatch_Pincode__c" AS pincode,
    "E_Waybill_Number__c" AS e_waybill_number,
    "Contact_no_of_POC__c" AS contact_no_of_poc,
    "Dispatch_District__c" AS district,
    "Dispatch_Entered_by__c" AS entered_by,
    "POC_Contact_Details__c" AS poc_contact_details,
    "Driver_Contact_Number__c" AS driver_contact_number,
    "Demand_Post_Validation__c" AS demand_post_validation_id,
    "Truck_Vehicle_Capacity__c" AS truck_vehicle_capacity,
    "Total_No_Of_Bags_Packages__c" AS total_no_of_bags_packages,
    "Payments_Updates_With_Date__c" AS payments_updates_with_date,
    "Transporter_Consignment_No__c" AS transporter_consignment_no,
    "From_which_Processing_Center__c" AS from_which_processing_center,
    "Loading_and_Truck_Images_Link__c" AS loading_and_truck_images_link
FROM {{ source('staging_salesforce', 'dispatch_status') }}

WHERE
    -- Don't include deleted records
       "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False
