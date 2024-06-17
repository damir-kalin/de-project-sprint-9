insert into dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
values (%(hk_order_user_pk)s, 
        (select h_order_pk from dds.h_order where order_id=%(order_id)s limit 1), 
        (select h_user_pk from dds.h_user where user_id=%(user_id)s limit 1), 
        %(load_dt)s, 
        %(load_src)s)
on conflict (h_order_pk, h_user_pk) do nothing;