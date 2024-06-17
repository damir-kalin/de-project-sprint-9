insert into dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
values ((select h_order_pk from dds.h_order where order_id=%(order_id)s limit 1),
        %(cost)s,
        %(payment)s,
        %(load_dt)s, 
        %(load_src)s,
        %(hk_order_cost_hashdiff)s
        )
on conflict (h_order_pk) do update
set
    cost = EXCLUDED.cost,
    payment = EXCLUDED.payment,
    load_dt = EXCLUDED.load_dt,
    load_src = EXCLUDED.load_src,
    hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff;