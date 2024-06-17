insert into dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
values ((select h_product_pk from dds.h_product where product_id=%(product_id)s limit 1),
        %(name)s,
        %(load_dt)s, 
        %(load_src)s,
        %(hk_product_names_hashdiff)s
        )
on conflict (h_product_pk) do update
set
    name = EXCLUDED.name,
    load_dt = EXCLUDED.load_dt,
    load_src = EXCLUDED.load_src,
    hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff;