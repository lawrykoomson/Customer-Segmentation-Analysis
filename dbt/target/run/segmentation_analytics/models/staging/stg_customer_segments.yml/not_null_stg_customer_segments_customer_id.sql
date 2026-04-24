
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from "customer_segments"."customer_dw_staging"."stg_customer_segments"
where customer_id is null



  
  
      
    ) dbt_internal_test