insert into dds.h_user (h_user_pk, user_id, load_dt, load_src)
values (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
on conflict (user_id) do nothing;