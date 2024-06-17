insert into dds.h_category (h_category_pk, category_name, load_dt, load_src)
values (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
on conflict (category_name) do nothing;