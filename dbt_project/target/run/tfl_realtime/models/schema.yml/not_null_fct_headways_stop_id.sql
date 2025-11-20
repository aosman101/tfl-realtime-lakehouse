
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stop_id
from "tfl"."main_marts"."fct_headways"
where stop_id is null



  
  
      
    ) dbt_internal_test