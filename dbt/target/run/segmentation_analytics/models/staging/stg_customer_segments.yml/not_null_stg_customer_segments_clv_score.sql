
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select clv_score
from "customer_segments"."customer_dw_staging"."stg_customer_segments"
where clv_score is null



  
  
      
    ) dbt_internal_test