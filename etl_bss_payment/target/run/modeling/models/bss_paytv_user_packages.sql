
      
        
            delete from "vplus_db_premium"."analytics"."bss_paytv_user_packages"
            where (
                id) in (
                select (id)
                from "bss_paytv_user_packages__dbt_tmp090810218358"
            );

        
    

    insert into "vplus_db_premium"."analytics"."bss_paytv_user_packages" ("upload_date", "id", "paytv_user_id", "provider_package_id", "name", "start_date", "end_date", "status", "created_at", "updated_at", "deleted_at")
    (
        select "upload_date", "id", "paytv_user_id", "provider_package_id", "name", "start_date", "end_date", "status", "created_at", "updated_at", "deleted_at"
        from "bss_paytv_user_packages__dbt_tmp090810218358"
    )
  