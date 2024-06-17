insert into dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
values (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
on conflict (restaurant_id) do nothing;