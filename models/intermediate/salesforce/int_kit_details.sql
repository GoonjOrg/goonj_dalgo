{{ config(
    materialized='table',
    tags=['kit', 'intermediate', 'salesforce', 'other_items']
) }}

WITH base_kits AS (
    SELECT 
        "Id" AS id,
        "Name" AS name,
        "CreatedById" AS createdbyid,
        "CreatedDate" AS createddate,
        "LastModifiedById" AS lastmodifiedbyid,
        "LastModifiedDate" AS lastmodifieddate,
        processing_center,
        other_kit_detail
    FROM {{ ref('staging_kit') }}
    WHERE other_kit_detail IS NOT NULL 
    AND LENGTH(TRIM(other_kit_detail)) > 0
),

split_lines AS (
    SELECT 
        base_kits.id,
        base_kits.name,
        base_kits.createdbyid,
        base_kits.createddate,
        base_kits.lastmodifiedbyid,
        base_kits.lastmodifieddate,
        base_kits.processing_center,
        base_kits.other_kit_detail,
        TRIM(lines.line_detail) AS line_detail
    FROM base_kits,
    LATERAL (
        SELECT UNNEST(STRING_TO_ARRAY(base_kits.other_kit_detail, CHR(10))) AS line_detail
    ) AS lines
    WHERE TRIM(lines.line_detail) != ''
),

parsed_kit_details AS (
    SELECT 
        id,
        name,
        createdbyid,
        createddate,
        lastmodifiedbyid,
        lastmodifieddate,
        processing_center,
        other_kit_detail,
        line_detail,
        -- Extract prefix (everything before " - ")
        CASE 
            WHEN split_lines.line_detail LIKE '%-%' 
            THEN TRIM(SPLIT_PART(split_lines.line_detail, ' - ', 1))
            ELSE NULL 
        END AS kit_category,
        
        -- Extract items part (everything after " - ", or whole string if no " - ")
        CASE 
            WHEN split_lines.line_detail LIKE '%-%' 
            THEN TRIM(SPLIT_PART(split_lines.line_detail, ' - ', 2))
            ELSE TRIM(split_lines.line_detail)
        END AS items_string
    FROM split_lines
    WHERE split_lines.line_detail IS NOT NULL 
    AND LENGTH(TRIM(split_lines.line_detail)) > 0
),

split_items AS (
    SELECT 
        id,
        name,
        createdbyid,
        createddate,
        lastmodifiedbyid,
        lastmodifieddate,
        processing_center,
        other_kit_detail,
        kit_category,
        TRIM(item_detail) AS item_detail
    FROM parsed_kit_details,
    LATERAL (
        SELECT UNNEST(STRING_TO_ARRAY(items_string, ',')) AS item_detail
    ) AS items
    WHERE TRIM(item_detail) != ''
),

final_parsed AS (
    SELECT 
        id,
        name,
        createdbyid,
        createddate,
        lastmodifiedbyid,
        lastmodifieddate,
        processing_center,
        other_kit_detail,
        kit_category,
        
        -- Extract item name (everything before the last number/dash)
        CASE 
            WHEN item_detail ~ '.*-[0-9]+$' 
            THEN TRIM(REGEXP_REPLACE(item_detail, '-[0-9]+$', ''))
            WHEN item_detail ~ '.*[0-9]+\s*(Jodi|jodi)$'
            THEN TRIM(REGEXP_REPLACE(item_detail, '\s*-?\s*[0-9]+\s*(Jodi|jodi)$', ''))
            ELSE TRIM(item_detail)
        END AS item_name,
        
        -- Extract quantity
        CASE 
            WHEN item_detail ~ '.*-([0-9]+)$' 
            THEN REGEXP_REPLACE(item_detail, '.*-([0-9]+)$', '\1')::INTEGER
            WHEN item_detail ~ '.*([0-9]+)\s*(Jodi|jodi)$'
            THEN REGEXP_REPLACE(item_detail, '.*([0-9]+)\s*(Jodi|jodi)$', '\1')::INTEGER
            ELSE NULL
        END AS item_quantity,
        
        -- Extract unit
        CASE 
            WHEN item_detail ~ '.*[0-9]+\s*(Jodi|jodi)$' THEN 'Jodi'
            WHEN item_detail ~ '.*-[0-9]+$' THEN 'pieces'
            ELSE NULL
        END AS item_unit
        
    FROM split_items
)

SELECT 
    id AS kit_id,
    name AS kit_name,
    createdbyid,
    createddate,
    lastmodifiedbyid,
    lastmodifieddate,
    processing_center,
    kit_category,
    item_name,
    item_quantity,
    item_unit,
    other_kit_detail AS original_kit_detail
FROM final_parsed
ORDER BY kit_id, kit_category, item_name