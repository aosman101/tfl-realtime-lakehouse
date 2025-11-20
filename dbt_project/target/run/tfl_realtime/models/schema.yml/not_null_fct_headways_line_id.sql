
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select line_id
from "tfl"."main_marts"."fct_headways"
where line_id is null



  
  
      
    ) dbt_internal_test