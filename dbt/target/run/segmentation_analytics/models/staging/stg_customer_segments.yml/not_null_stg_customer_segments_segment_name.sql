
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select segment_name
from "customer_segments"."customer_dw_staging"."stg_customer_segments"
where segment_name is null



  
  
      
    ) dbt_internal_test