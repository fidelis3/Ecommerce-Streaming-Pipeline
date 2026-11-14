-- Parse payment JSON and drop duplicate deliveries (same event_id).
with parsed as (
    select
        record_content:event_id::string                                  as event_id,
        record_content:event_type::string                                as event_type,
        record_content:payment_id::string                                as payment_id,
        record_content:order_id::string                                  as order_id,
        record_content:amount::number(10, 2)                             as amount,
        record_content:method::string                                    as payment_method,
        try_to_timestamp_ntz(record_content:event_ts::string)            as event_ts,
        kafka_partition,
        kafka_offset,
        loaded_at
    from {{ source('raw', 'payments_raw') }}
)

select *
from parsed
where event_id is not null
qualify row_number() over (
    partition by event_id
    order by kafka_partition, kafka_offset
) = 1

