{{ config(
    materialized='table',
    tags=['account_reporting', 'prod', 'salesforce']
) }}


WITH current_fy AS (
    SELECT 
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4 
            THEN EXTRACT(YEAR FROM CURRENT_DATE)::text || '-' || RIGHT((EXTRACT(YEAR FROM CURRENT_DATE) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM CURRENT_DATE) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM CURRENT_DATE)::text, 2)
        END AS current_financial_year
),


filtered_demands AS (
    SELECT demands.* FROM {{ ref('int_demands') }} demands 
    left join {{ ref('int_dispatches') }} dispatches 
    on demands.demand_id = dispatches.demand_id
    WHERE 
    (demands.annual_year=(SELECT current_financial_year FROM current_fy)
    OR (dispatches.demand_id = demands.demand_id and dispatches.annual_year=(SELECT current_financial_year FROM current_fy)))
    and demands.demand_status!='Closed' and demands.dpv_status!='Closed'

    --AND demands.internal_demand != 'Internal' 
),

filtered_dispatches AS (
    SELECT * FROM {{ ref('int_dispatches') }} dispatches
    WHERE dispatches.annual_year=(SELECT current_financial_year FROM current_fy)
    --AND dispatches.internal_demand != 'Internal' 
),

distribution_summary AS (
    SELECT 
        implementation_inventory_id,
        SUM(quantity) as distributed_quantity
    FROM {{ ref('int_distributions') }} 
    WHERE implementation_inventory_id IS NOT NULL
    GROUP BY implementation_inventory_id
)

select distinct

dispatches.annual_year as annual_year,
dispatches.quarter as quarter,
dispatches.month as month,
dispatches.monthnum as monthnum,
dispatches.dispatch_name,
dispatches.dispatch_date,
dispatches.dispatch_id,
dispatches.remarks as dispatchremarks,

dispatches.dispatch_line_item_id,
dispatches.dispatch_line_item_name,
dispatches.quantity as dispatchedquantity,

demands.demand_id,
demands.demand_name,
demands.dpv_status,
demands.dispatch_stage,
demands.receiver_center_name as account_name,
demands.receiver_account_type as account_type,
demands.receiver_center_id,
demands.receiver_state,
demands.receiver_district,
demands.processing_center_name,
demands.processing_center_type,
demands.processing_center_id,
demands.processing_state,
demands.processing_district,
demands.annual_year as demand_annualyear,
demands.demand_date,
demands.internal_demand,

ii.implementation_inventory_name,
ii.implementation_inventory_id,
ii.original_quantity,
ii.current_quantity,
ii.material_name,
ii.material_type,
ii.original_name,
ii.kit_type,
ii.sub_type,
ii.material_sub_category,



distributions.annual_year as distribution_year,
distributions.quarter as distribution_quarter,
distributions.month as distribution_month,
distributions.monthnum as distribution_monthnum,
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
distributions.type_of_initiative,
school_name,
school_type,
is_rahat,
no_of_families_reached,
no_of_individuals_reached,
distribution_line_name,
distribution_line_id,
distributions.quantity as distributionquantity,
distributions.purchase_kit_id,
is_created_from_avni,

distributions.created_date,
distribution_date-distributions.created_date as date_diff,
distributions.last_modified_date,
distributions.new_created_by,
distributions.last_modified_by_id,

distributions.remarks as distributionremarks,    

COALESCE(ds.distributed_quantity, 0) as distributed_quantity,
    
-- Add pending quantity calculation
(ii.original_quantity - COALESCE(ds.distributed_quantity, 0)) as pending_quantity,
    

CASE 
    WHEN ii.original_quantity > COALESCE(ds.distributed_quantity, 0) THEN 'Pending' 
    WHEN ii.original_quantity = COALESCE(ds.distributed_quantity, 0) THEN 'Complete' 
    WHEN ii.original_quantity < COALESCE(ds.distributed_quantity, 0) THEN 'Over Distributed' 
    ELSE 'Unknown'
END as distribution_status
 

FROM 
filtered_demands as demands
left join filtered_dispatches as dispatches
ON demands.demand_id=dispatches.demand_id
left join {{ ref('int_implementationinventory') }}  as ii 
ON ii.dispatch_line_item_id = dispatches.dispatch_line_item_id 
left join distribution_summary as ds 
ON ds.implementation_inventory_id = ii.implementation_inventory_id 
left join {{ ref('int_distributions') }}  as distributions
ON distributions.dispatch_line_item_id = dispatches.dispatch_line_item_id 
