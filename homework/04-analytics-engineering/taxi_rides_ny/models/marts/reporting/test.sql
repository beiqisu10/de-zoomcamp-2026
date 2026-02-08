select count(1)
from {{ ref('fct_monthly_zone_revenue') }} 

select
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_revenue
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by total_revenue desc
limit 1;

select sum(total_monthly_trips) as total_trips
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and revenue_month = '2019-10-01'

select count(1)
from {{ ref('stg_fhv_tripdata') }} 