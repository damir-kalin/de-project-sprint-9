import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    
    def user_product_counters_insert(self,
                        user_id:str,
                        product_id:str,
                        product_name:str,
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into cdm.user_product_counters as c (user_id, product_id, product_name, order_cnt)
                        values (%(user_id)s, %(product_id)s, %(product_name)s, 1)
                        on conflict (user_id, product_id) do update
                        set
                            product_name = EXCLUDED.product_name,
                            order_cnt = c.order_cnt + EXCLUDED.order_cnt;
                    """,
                    {
                        'user_id': uuid.uuid3(uuid.NAMESPACE_DNS, user_id),
                        'product_id': uuid.uuid3(uuid.NAMESPACE_DNS, product_id),
                        'product_name': product_name
                    }
                )

    def user_category_counters_insert(self,
                        user_id:str,
                        category_name:str,
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into cdm.user_category_counters as c (user_id, category_id, category_name, order_cnt)
                        values (%(user_id)s, %(category_id)s, %(category_name)s, 1)
                        on conflict (user_id, category_id) do update
                        set
                            category_name = EXCLUDED.category_name,
                            order_cnt = c.order_cnt + EXCLUDED.order_cnt;
                    """,
                    {
                        'user_id': uuid.uuid3(uuid.NAMESPACE_DNS, user_id),
                        'category_id': uuid.uuid3(uuid.NAMESPACE_DNS, category_name),
                        'category_name': category_name
                    }
                )