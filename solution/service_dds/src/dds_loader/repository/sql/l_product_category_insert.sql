insert into dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
values (%(hk_product_category_pk)s, 
        (select h_category_pk from dds.h_category where category_name=%(category_name)s limit 1), 
        (select h_product_pk from dds.h_product where product_id=%(product_id)s limit 1), 
        %(load_dt)s, 
        %(load_src)s)
on conflict (h_category_pk, h_product_pk) do nothing;