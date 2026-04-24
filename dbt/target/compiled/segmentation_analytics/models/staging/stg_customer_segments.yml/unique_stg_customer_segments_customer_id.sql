
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from "customer_segments"."customer_dw_staging"."stg_customer_segments"
where customer_id is not null
group by customer_id
having count(*) > 1


