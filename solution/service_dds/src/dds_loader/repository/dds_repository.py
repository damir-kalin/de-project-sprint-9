import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    
    def h_user_insert(self,
                        user_id:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_user (h_user_pk, user_id, load_dt, load_src)
                        values (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                        on conflict (user_id) do nothing;
                    """,
                    {
                        'h_user_pk': uuid.uuid3(uuid.NAMESPACE_DNS, user_id),
                        'user_id': user_id,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )
    
    def h_product_insert(self,
                        product_id:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_product (h_product_pk, product_id, load_dt, load_src)
                        values (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                        on conflict (product_id) do nothing;
                    """,
                    {
                        'h_product_pk': uuid.uuid3(uuid.NAMESPACE_DNS, product_id),
                        'product_id': product_id,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )
    
    def h_category_insert(self,
                        category_name:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_category (h_category_pk, category_name, load_dt, load_src)
                        values (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                        on conflict (category_name) do nothing;
                    """,
                    {
                        'h_category_pk': uuid.uuid3(uuid.NAMESPACE_DNS, category_name),
                        'category_name': category_name,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )

    def h_restaurant_insert(self,
                        restaurant_id:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                        values (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        on conflict (restaurant_id) do nothing;
                    """,
                    {
                        'h_restaurant_pk': uuid.uuid3(uuid.NAMESPACE_DNS, restaurant_id),
                        'restaurant_id': restaurant_id,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )
    
    def h_order_insert(self,
                        order_id:int,
                        order_dt:datetime
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                        values (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                        on conflict (order_id, order_dt) do nothing;
                    """,
                    {
                        'h_order_pk': uuid.uuid3(uuid.NAMESPACE_DNS, str(order_id) + str(order_dt)),
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )
    
    def l_order_product_insert(self,
                        order_id:str,
                        product_id:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                        values (%(hk_order_product_pk)s, 
                                (select h_order_pk from dds.h_order where order_id=%(order_id)s), 
                                (select h_product_pk from dds.h_product where product_id=%(product_id)s), 
                                %(load_dt)s, 
                                %(load_src)s)
                        on conflict (h_order_pk, h_product_pk) do nothing;
                    """,
                    {
                        'hk_order_product_pk': uuid.uuid3(uuid.NAMESPACE_DNS, str(order_id) + str(product_id)),
                        'order_id': order_id,
                        'product_id': product_id,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )

    def l_product_restaurant_insert(self,
                        restaurant_id:str,
                        product_id:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
                        values (%(hk_product_restaurant_pk)s, 
                                (select h_restaurant_pk from dds.h_restaurant where restaurant_id=%(restaurant_id)s), 
                                (select h_product_pk from dds.h_product where product_id=%(product_id)s), 
                                %(load_dt)s, 
                                %(load_src)s)
                        on conflict (h_restaurant_pk, h_product_pk) do nothing;
                    """,
                    {
                        'hk_product_restaurant_pk': uuid.uuid3(uuid.NAMESPACE_DNS, str(restaurant_id) + str(product_id)),
                        'restaurant_id': restaurant_id,
                        'product_id': product_id,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )
    
    def l_product_category_insert(self,
                        category_name:str,
                        product_id:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
                        values (%(hk_product_category_pk)s, 
                                (select h_category_pk from dds.h_category where category_name=%(category_name)s), 
                                (select h_product_pk from dds.h_product where product_id=%(product_id)s), 
                                %(load_dt)s, 
                                %(load_src)s)
                        on conflict (h_category_pk, h_product_pk) do nothing;
                    """,
                    {
                        'hk_product_category_pk': uuid.uuid3(uuid.NAMESPACE_DNS, str(category_name) + str(product_id)),
                        'category_name': category_name,
                        'product_id': product_id,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )
    
    def l_order_user_insert(self,
                        order_id:int,
                        user_id:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                        values (%(hk_order_user_pk)s, 
                                (select h_order_pk from dds.h_order where order_id=%(order_id)s), 
                                (select h_user_pk from dds.h_user where user_id=%(user_id)s), 
                                %(load_dt)s, 
                                %(load_src)s)
                        on conflict (h_order_pk, h_user_pk) do nothing;
                    """,
                    {
                        'hk_order_user_pk': uuid.uuid3(uuid.NAMESPACE_DNS, str(order_id) + str(user_id)),
                        'order_id': order_id,
                        'user_id': user_id,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka'
                    }
                )

    def s_user_names_insert(self,
                        user_id:str,
                        username:str,
                        userlogin:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                        values ((select h_user_pk from dds.h_user where user_id=%(user_id)s),
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
                    """,
                    {
                        'user_id': user_id,
                        'username': username,
                        'userlogin': userlogin,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka',
                        'hk_user_names_hashdiff': uuid.uuid3(uuid.NAMESPACE_DNS, username + userlogin)
                    }
                )
    
    def s_product_names_insert(self,
                        product_id:str,
                        name:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                        values ((select h_product_pk from dds.h_product where product_id=%(product_id)s),
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
                    """,
                    {
                        'product_id': product_id,
                        'name': name,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka',
                        'hk_product_names_hashdiff': uuid.uuid3(uuid.NAMESPACE_DNS, name)
                    }
                )

    def s_restaurant_names_insert(self,
                        restaurant_id:str,
                        name:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    """,
                    {
                        'restaurant_id': restaurant_id,
                        'name': name,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka',
                        'hk_restaurant_names_hashdiff': uuid.uuid3(uuid.NAMESPACE_DNS, name)
                    }
                )

    def s_order_cost_insert(self,
                        order_id:str,
                        cost:float,
                        payment:float
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                        values ((select h_order_pk from dds.h_order where order_id=%(order_id)s),
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
                    """,
                    {
                        'order_id': order_id,
                        'cost': cost,
                        'payment': payment,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka',
                        'hk_order_cost_hashdiff': uuid.uuid3(uuid.NAMESPACE_DNS, str(cost) + str(payment))
                    }
                )

    def s_order_status_insert(self,
                        order_id:str,
                        status:str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                        values ((select h_order_pk from dds.h_order where order_id=%(order_id)s),
                                %(status)s,
                                %(load_dt)s, 
                                %(load_src)s,
                                %(hk_order_status_hashdiff)s
                                )
                        on conflict (h_order_pk) do update
                        set
                            status = EXCLUDED.status,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src,
                            hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff;
                    """,
                    {
                        'order_id': order_id,
                        'status': status,
                        'load_dt': datetime.now(),
                        'load_src': 'orders-system-kafka',
                        'hk_order_status_hashdiff': uuid.uuid3(uuid.NAMESPACE_DNS, status + str(order_id))
                    }
                )