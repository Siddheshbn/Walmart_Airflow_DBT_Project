{{
    config(
        materialized = 'incremental',
        unique_key = 'employee_id'
    )
}}

SELECT 
    *,
    current_timestamp() AS processed_at
FROM 
    {{ source('walmart_databricks', 'employees') }}


{% if is_incremental() %}
  WHERE updated_timestamp >= (select COALESCE(max(updated_timestamp), '1900-01-01') from {{ this }})
{% endif %}
