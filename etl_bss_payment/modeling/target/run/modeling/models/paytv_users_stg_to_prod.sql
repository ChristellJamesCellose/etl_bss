
      
  
    

  create  table
    "vplus_db_premium"."analytics"."paytv_users_stg_to_prod"
    
    
    
  as (
    

select getdate(),* from "vplus_db_premium"."raw_data"."bss_paytv_users"
  );
  
  