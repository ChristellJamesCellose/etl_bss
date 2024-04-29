
      
        
            delete from "vplus_db_premium"."analytics"."bss_paytv_providers"
            where (
                id) in (
                select (id)
                from "bss_paytv_providers__dbt_tmp152206692293"
            );

        
    

    insert into "vplus_db_premium"."analytics"."bss_paytv_providers" ("upload_date", "id", "code", "name", "web_image_url", "ios_image_url", "android_image_url", "created_at", "updated_at", "deleted_at")
    (
        select "upload_date", "id", "code", "name", "web_image_url", "ios_image_url", "android_image_url", "created_at", "updated_at", "deleted_at"
        from "bss_paytv_providers__dbt_tmp152206692293"
    )
  