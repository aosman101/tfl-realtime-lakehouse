
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour
from "tfl"."main_marts"."fct_headways"
where hour is null



  
  
      
    ) dbt_internal_test