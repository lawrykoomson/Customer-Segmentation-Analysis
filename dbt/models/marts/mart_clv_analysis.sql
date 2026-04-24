/*
  Mart Model: mart_clv_analysis
  ================================
  Aggregates Customer Lifetime Value metrics
  by segment, region, channel and spend tier.
  Powers CLV analysis in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_customer_segments') }}

),

clv_analysis as (

    select
        segment_name,
        clv_tier,
        spend_tier,
        preferred_channel,
        region,
        age_group,
        gender,

        count(customer_id)                              as customer_count,
        round(avg(clv_score), 4)                        as avg_clv_score,
        round(min(clv_score), 4)                        as min_clv_score,
        round(max(clv_score), 4)                        as max_clv_score,
        round(avg(total_spend_ghs), 2)                  as avg_spend_ghs,
        round(sum(total_spend_ghs), 2)                  as total_revenue_ghs,
        round(avg(frequency), 2)                        as avg_frequency,
        round(avg(recency_days), 1)                     as avg_recency_days,
        round(avg(loyalty_points), 0)                   as avg_loyalty_points,
        count(case when momo_user then 1 end)           as momo_users

    from staged
    group by
        segment_name, clv_tier, spend_tier,
        preferred_channel, region, age_group, gender

)

select * from clv_analysis
order by avg_clv_score desc