
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select p90_headway_s
from "tfl"."main_marts"."fct_headways"
where p90_headway_s is null



  
  
      
    ) dbt_internal_test