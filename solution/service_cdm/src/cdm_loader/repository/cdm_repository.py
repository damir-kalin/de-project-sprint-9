import os
import uuid
from typing import Any, Dict

from lib.pg import PgConnect
from pydantic import BaseModel


class UserProductCounters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str

class UserCategoryCounters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str

class OrderCdmBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.order_ns_uuid = uuid.UUID('8e884ace-bee4-11e4-8dfc-aa07a5b093db')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def user_product_counters(self) -> UserProductCounters:
        user_id = self._dict["user_id"]
        product_id = self._dict["product_id"]
        product_name = self._dict["product_name"]
        return UserProductCounters(
            user_id=self._uuid(user_id),
            product_id=self._uuid(product_id),
            product_name=product_name
        )
    
    def user_category_counters(self) -> UserCategoryCounters:
        user_id = self._dict["user_id"]
        category_name = self._dict["category_name"]
        return UserCategoryCounters(
            user_id=self._uuid(user_id),
            category_id=self._uuid(category_name),
            category_name=category_name
        )


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self._path_sql_files = os.path.dirname(os.path.abspath(__file__)) + "/sql"
    
    def user_product_counters_insert(self,
                        user_product_counters: UserProductCounters
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                with open(self._path_sql_files + "/user_product_counters_insert.sql", "r") as script:
                    cur.execute(
                        script.read(),
                        {
                            'user_id': user_product_counters.user_id,
                            'product_id': user_product_counters.product_id,
                            'product_name': user_product_counters.product_name
                        }
                    )

    def user_category_counters_insert(self,
                        user_category_counters: UserCategoryCounters
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                with open(self._path_sql_files + "/user_category_counters_insert.sql", "r") as script:
                    cur.execute(
                        script.read(),
                        {
                            'user_id': user_category_counters.user_id,
                            'category_id': user_category_counters.category_id,
                            'category_name': user_category_counters.category_name
                        }
                    )