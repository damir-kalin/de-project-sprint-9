insert into dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
values ((select h_restaurant_pk from dds.h_restaurant where restaurant_id=%(restaurant_id)s),
        %(name)s,
        %(load_dt)s, 
        %(load_src)s,
        %(hk_restaurant_names_hashdiff)s
        )
on conflict (h_restaurant_pk) do update
set
    name = EXCLUDED.name,
    load_dt = EXCLUDED.load_dt,
    load_src = EXCLUDED.load_src,
    hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff;