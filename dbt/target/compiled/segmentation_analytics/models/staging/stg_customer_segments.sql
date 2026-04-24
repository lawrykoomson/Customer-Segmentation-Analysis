/*
  Staging Model: stg_customer_segments
  ======================================
  Cleans and standardises raw customer segmentation data.
  Single source of truth for all downstream segment models.

  Source: customer_dw.customer_segments
  Author: Lawrence Koomson
*/

with source as (

    select * from "customer_segments"."customer_dw"."customer_segments"

),

staged as (

    select
        customer_id,
        upper(trim(gender))                             as gender,
        age_group,
        initcap(trim(region))                           as region,
        initcap(trim(preferred_channel))                as preferred_channel,
        last_purchase_date,
        recency_days,
        frequency,
        total_spend_ghs,
        avg_order_value_ghs,
        num_complaints,
        momo_user,
        loyalty_points,

        rfm_recency,
        rfm_frequency,
        rfm_monetary,
        cluster_id,
        initcap(trim(segment_name))                     as segment_name,
        retention_action,
        clv_score,
        processed_at,

        case
            when recency_days <= 30  then 'Active'
            when recency_days <= 90  then 'Warm'
            when recency_days <= 180 then 'Cooling'
            else 'Dormant'
        end                                             as recency_status,

        case
            when total_spend_ghs >= 10000 then 'Platinum'
            when total_spend_ghs >= 5000  then 'Gold'
            when total_spend_ghs >= 1000  then 'Silver'
            else 'Bronze'
        end                                             as spend_tier,

        case
            when clv_score >= 0.20 then 'High CLV'
            when clv_score >= 0.10 then 'Medium CLV'
            else 'Low CLV'
        end                                             as clv_tier

    from source
    where customer_id is not null

)

select * from staged