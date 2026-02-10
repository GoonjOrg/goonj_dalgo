{{ config(
    materialized='table',
    tags=['proj_dispatch_distributions_cy', 'prod', 'salesforce']
) }}


WITH current_fy AS (
    SELECT 
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4 
            THEN EXTRACT(YEAR FROM CURRENT_DATE)::text || '-' || RIGHT((EXTRACT(YEAR FROM CURRENT_DATE) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM CURRENT_DATE) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM CURRENT_DATE)::text, 2)
        END AS current_financial_year
),

filtered_distributions AS (
    -- Step 1: Filter early. 
    -- This ensures we only process distributions that have the required types.
    SELECT * FROM {{ ref('int_distributions') }}
    WHERE kit_type IS NOT NULL 
       OR material_type IS NOT NULL
),

filtered_dispatches AS (
    SELECT * FROM {{ ref('int_dispatches') }} dispatches
    CROSS JOIN current_fy cfy
    WHERE dispatches.annual_year=cfy.current_financial_year
    AND dispatches.internal_demand != 'Internal' 
)

select distinct
distributions.annual_year,
distributions.quarter,
distributions.month,
distributions.monthnum,
distributions.state,
distributions.district,
distributions.block,
distributions.other_block,
distributions.village,
distributions.other_village,
distributions.tola_mohalla,
distributions.disaster_type,
distribution_id,
distribution_name,
distribution_date,
type_of_community,
type_of_initiative,
account_name,
distributor_account_type,
school_name,
school_type,
is_rahat,
no_of_families_reached,
no_of_individuals_reached,
distribution_line_name,
distribution_line_id,
distributed_to,
implementation_inventory_name,
distributions.quantity as distributionquantity,
distributions.unit,
distributions.kit_type,
distributions.sub_type,
distributions.material_type,
distributions.material_sub_category,
other_material_name,
distributions.purchase_kit_name,
current_quantity,
is_created_from_avni,

distributions.created_date,
distribution_date-distributions.created_date as date_diff,
distributions.last_modified_date,
distributions.new_created_by,
distributions.last_modified_by_id,

dispatches.dispatch_name,
dispatches.dispatch_date,
dispatches.dispatch_id,
distributions.dispatch_line_item_id,
dispatches.dispatch_line_item_name,
dispatches.quantity as dispatchedquantity,
processing_center_name,
processing_center_type,
processing_state,
procssing_district,
receiver_center_name,
receiver_center_type,
receiver_state,
receiver_district,
demand_id,
demand_post_validation_id,
local_demand,
internal_demand,
dispatches.remarks as dispatchremarks,
distributions.remarks as distributionremarks,
distribution_photos,
receiver_photos,
disclaimer_photos

FROM 
filtered_dispatches as dispatches left join
filtered_distributions as distributions 
on distributions.dispatch_line_item_id = dispatches.dispatch_line_item_id 
