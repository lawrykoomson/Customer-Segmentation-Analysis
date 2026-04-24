/*
  Mart Model: mart_segments_by_region
  =====================================
  Aggregates customer segment distribution by Ghana region.
  Powers regional segment heatmap in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from "customer_segments"."customer_dw_staging"."stg_customer_segments"

),

region_segments as (

    select
        region,
        segment_name,

        count(customer_id)                              as customer_count,
        round(avg(total_spend_ghs), 2)                  as avg_spend_ghs,
        round(sum(total_spend_ghs), 2)                  as total_revenue_ghs,
        round(avg(clv_score), 4)                        as avg_clv_score,
        round(avg(recency_days), 1)                     as avg_recency_days,
        round(avg(frequency), 2)                        as avg_frequency,

        count(case when momo_user then 1 end)           as momo_users,
        count(case when spend_tier = 'Platinum'
                   then 1 end)                          as platinum_customers

    from staged
    group by region, segment_name

)

select * from region_segments
order by region, avg_clv_score desc