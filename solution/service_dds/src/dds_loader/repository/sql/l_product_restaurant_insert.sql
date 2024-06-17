insert into dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
values (%(hk_product_restaurant_pk)s, 
        (select h_restaurant_pk from dds.h_restaurant where restaurant_id=%(restaurant_id)s limit 1), 
        (select h_product_pk from dds.h_product where product_id=%(product_id)s limit 1), 
        %(load_dt)s, 
        %(load_src)s)
on conflict (h_restaurant_pk, h_product_pk) do nothing;