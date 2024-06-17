insert into dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
values (%(hk_order_product_pk)s, 
        (select h_order_pk from dds.h_order where order_id=%(order_id)s limit 1), 
        (select h_product_pk from dds.h_product where product_id=%(product_id)s limit 1), 
        %(load_dt)s, 
        %(load_src)s)
on conflict (h_order_pk, h_product_pk) do nothing;