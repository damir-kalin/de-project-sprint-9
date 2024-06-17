insert into dds.h_product (h_product_pk, product_id, load_dt, load_src)
values (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
on conflict (product_id) do nothing;