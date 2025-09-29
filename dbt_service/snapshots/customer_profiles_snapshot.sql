{% snapshot customer_profiles_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=[
      'current_segment',
      'value_tier', 
      'activity_status',
      'conversion_profile'
    ]
  )
}}

select * from {{ ref('stg_customer_profiles') }}

{% endsnapshot %}