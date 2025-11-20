
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_ts
from "tfl"."main_staging"."stg_arrivals"
where event_ts is null



  
  
      
    ) dbt_internal_test