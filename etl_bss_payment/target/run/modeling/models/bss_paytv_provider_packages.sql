
      
        
            delete from "vplus_db_premium"."analytics"."bss_paytv_provider_packages"
            where (
                id) in (
                select (id)
                from "bss_paytv_provider_packages__dbt_tmp090800879290"
            );

        
    

    insert into "vplus_db_premium"."analytics"."bss_paytv_provider_packages" ("id", "provider_id", "provider_package_code", "package_id", "created_at", "updated_at", "deleted_at")
    (
        select "id", "provider_id", "provider_package_code", "package_id", "created_at", "updated_at", "deleted_at"
        from "bss_paytv_provider_packages__dbt_tmp090800879290"
    )
  