insert into cdm.user_category_counters as c (user_id, category_id, category_name, order_cnt)
values (%(user_id)s, %(category_id)s, %(category_name)s, 1)
on conflict (user_id, category_id) do update
set
    category_name = EXCLUDED.category_name,
    order_cnt = c.order_cnt + EXCLUDED.order_cnt;