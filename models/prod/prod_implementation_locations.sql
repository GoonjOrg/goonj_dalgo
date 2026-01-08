{{ config(
    materialized='table',
    tags=['implementation_locations', 'prod', 'salesforce']
) }}

select distinct
    case when a.annual_year is not null then a.annual_year else d.annual_year end as annual_year,
    case when a.month is not null then a.month else d.month end as month,
    case when a.monthnum is not null then a.monthnum else d.monthnum end as monthnum,
    case when a.quarter is not null then a.quarter else d.quarter end as quarter,
    case when a.state is not null then a.state else d.state end as state,
    case when a.district is not null then a.district else d.district end as district,
    case when a.block is not null then a.block else d.block end as block,
    case when a.other_block is not null then a.other_block else d.other_block end as other_block,
    case when a.village is not null then a.village else d.village end as village,
    case when a.other_village is not null then a.other_village else d.other_village end as other_village,
    case when a.account_type is not null then a.account_type else d.distributor_account_type end as activity_account_type,
    a.type_of_initiative as activity_initiative,
    count(distinct a.activity_id) as activity_count,
    sum(a.number_of_activities) as num_activities,
    d.kit_type,
    d.sub_type,
    d.disaster_type,
    d.type_of_initiative as distribution_initiative,
    count(d.distribution_id) as num_distributions,
    sum(d.quantity) 

from 
{{ ref('int_activities') }} a full outer join {{ ref('int_distributions') }} d on a.annual_year=d.annual_year and a.month=d.month and a.monthnum=d.monthnum and a.quarter=d.quarter and a.state=d.state and a.district=d.district and a.block=d.block and a.village=d.village and a.other_block=d.other_block and a.other_village=d.other_village
group by a.annual_year, a.month, a.monthnum, a.quarter, a.state, a.district, a.block, a.village, a.account_type, d.annual_year, d.month, d.monthnum, d.quarter, d.state, d.district, d.block, d.village, d.distributor_account_type, d.kit_type, d.sub_type, d.disaster_type, a.type_of_initiative, d.type_of_initiative, a.other_block , a.other_village, d.other_block,d.other_village    

