insert into dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
values (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
on conflict (order_id, order_dt) do nothing;