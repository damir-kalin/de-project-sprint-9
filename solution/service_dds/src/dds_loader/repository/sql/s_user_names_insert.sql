insert into dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
values ((select h_user_pk from dds.h_user where user_id=%(user_id)s limit 1),
        %(username)s,
        %(userlogin)s,
        %(load_dt)s, 
        %(load_src)s,
        %(hk_user_names_hashdiff)s
        )
on conflict (h_user_pk) do update
set
    username = EXCLUDED.username,
    userlogin = EXCLUDED.userlogin,
    load_dt = EXCLUDED.load_dt,
    load_src = EXCLUDED.load_src,
    hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff;