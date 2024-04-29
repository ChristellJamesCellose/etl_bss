
      
        
            delete from "vplus_db_premium"."analytics"."bss_paytv_users"
            where (
                id) in (
                select (id)
                from "bss_paytv_users__dbt_tmp090820246592"
            );

        
    

    insert into "vplus_db_premium"."analytics"."bss_paytv_users" ("upload_date", "id", "provider_id", "subscriber_id", "joined_at", "status", "is_blocked", "user_id", "phone", "email", "state", "created_at", "updated_at", "deleted_at", "ref_code")
    (
        select "upload_date", "id", "provider_id", "subscriber_id", "joined_at", "status", "is_blocked", "user_id", "phone", "email", "state", "created_at", "updated_at", "deleted_at", "ref_code"
        from "bss_paytv_users__dbt_tmp090820246592"
    )
  