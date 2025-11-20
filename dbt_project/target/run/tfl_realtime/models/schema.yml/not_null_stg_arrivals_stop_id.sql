
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stop_id
from "tfl"."main_staging"."stg_arrivals"
where stop_id is null



  
  
      
    ) dbt_internal_test