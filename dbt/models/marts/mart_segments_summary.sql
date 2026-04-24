/*
  Mart Model: mart_segments_summary
  ===================================
  Aggregates KPIs per customer segment.
  Powers segment comparison in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_customer_segments') }}

),

segment_summary as (

    select
        segment_name,

        count(customer_id)                              as total_customers,
        round(
            count(customer_id)::numeric
            / sum(count(customer_id)) over () * 100
        , 2)                                            as customer_share_pct,

        round(avg(recency_days), 1)                     as avg_recency_days,
        round(avg(frequency), 2)                        as avg_frequency,
        round(avg(total_spend_ghs), 2)                  as avg_total_spend_ghs,
        round(sum(total_spend_ghs), 2)                  as total_revenue_ghs,
        round(avg(avg_order_value_ghs), 2)              as avg_order_value_ghs,
        round(avg(clv_score), 4)                        as avg_clv_score,
        round(avg(loyalty_points), 0)                   as avg_loyalty_points,
        round(avg(num_complaints), 2)                   as avg_complaints,

        count(case when momo_user then 1 end)           as momo_users,
        round(
            count(case when momo_user then 1 end)::numeric
            / nullif(count(customer_id), 0) * 100
        , 2)                                            as momo_adoption_pct,

        count(case when spend_tier = 'Platinum'
                   then 1 end)                          as platinum_customers,
        count(case when spend_tier = 'Gold'
                   then 1 end)                          as gold_customers,

        round(
            sum(total_spend_ghs)
            / sum(sum(total_spend_ghs)) over () * 100
        , 2)                                            as revenue_share_pct,

        max(retention_action)                           as recommended_action

    from staged
    group by segment_name

)

select * from segment_summary
order by avg_clv_score desc