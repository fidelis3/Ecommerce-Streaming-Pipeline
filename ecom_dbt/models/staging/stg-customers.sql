-- Parse customer JSON and drop duplicate deliveries (same event_id).
with parsed as (
    select
        record_content:event_id::string                                  as event_id,
        record_content:event_type::string                                as event_type,
        record_content:customer_id::string                               as customer_id,
        record_content:name::string                                      as customer_name,
        record_content:email::string                                     as email,
        record_content:city::string                                      as city,
        try_to_timestamp_ntz(record_content:event_ts::string)            as event_ts,
        kafka_partition,
        kafka_offset,
        loaded_at
    from {{ source('raw', 'customers_raw') }}
)

select *
from parsed
where event_id is not null
qualify row_number() over (
    partition by event_id
    order by kafka_partition, kafka_offset
) = 1