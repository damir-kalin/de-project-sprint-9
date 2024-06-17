insert into stg.order_events (object_id, object_type, sent_dttm, payload)
values (%(object_id)s, %(object_type)s, %(sent_dttm)s, %(payload)s)
on conflict (object_id) do update
set
    object_type = EXCLUDED.object_type,
    sent_dttm = EXCLUDED.sent_dttm,
    payload = EXCLUDED.payload;