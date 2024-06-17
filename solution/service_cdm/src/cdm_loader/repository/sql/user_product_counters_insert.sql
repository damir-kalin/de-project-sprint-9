insert into cdm.user_product_counters as c (user_id, product_id, product_name, order_cnt)
values (%(user_id)s, %(product_id)s, %(product_name)s, 1)
on conflict (user_id, product_id) do update
set
    product_name = EXCLUDED.product_name,
    order_cnt = c.order_cnt + EXCLUDED.order_cnt;