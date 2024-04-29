{{ 
    config(
        materialized='incremental',
        unique_key = 'id'
        )
}}

select getdate() as upload_date,* from {{source('rawdata_users','bss_payment_transactions')}}