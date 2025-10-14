{{ config(
    materialized='table',
    tags=['distribution_geography', 'intermediate', 'salesforce']
) }}


with year_geography as(
select 
    distinct 
    CASE 
        WHEN EXTRACT(MONTH FROM date_of_distribution) >= 4 
            THEN EXTRACT(YEAR FROM date_of_distribution)::text || '-' || RIGHT((EXTRACT(YEAR FROM date_of_distribution) + 1)::text, 2)
        ELSE (EXTRACT(YEAR FROM date_of_distribution) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM date_of_distribution)::text, 2)
    END AS annual_year,
    state,
    district,
    block,
    village,
    other_block,
    other_village,
    count(distinct distribution_id) as distribution_count
from 
{{ ref('staging_distribution') }}
where is_deleted=False
group by annual_year,state,district,block,village,other_block,other_village
),

new_states as (
    -- States appearing for the first time in each year
    select 
        yg.annual_year,
        yg.state
    from year_geography yg
    where not exists (
        select 1 
        from year_geography prev 
        where prev.annual_year < yg.annual_year 
            and prev.state = yg.state
    )
),

new_districts as (
    -- Districts appearing for the first time in each year
    select 
        yg.annual_year,
        yg.state,
        yg.district
    from year_geography yg
    where not exists (
        select 1 
        from year_geography prev 
        where prev.annual_year < yg.annual_year 
            and prev.state = yg.state 
            and prev.district = yg.district
    )
)

select 
        yg.annual_year,
        yg.state,
        yg.district,
        yg.block,
        yg.village,
        yg.other_block,
        yg.other_village,
        yg.distribution_count,
        case when ns.state is not null then true else false end as new_state,
        case when nd.district is not null then true else false end as new_district
    from year_geography yg
    left join new_states ns 
        on yg.annual_year = ns.annual_year 
        and yg.state = ns.state
    left join new_districts nd 
        on yg.annual_year = nd.annual_year 
        and yg.state = nd.state 
        and yg.district = nd.district
