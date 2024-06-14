from datetime import datetime
import uuid
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            self._logger.info(msg)
            if msg is None:
                break
            if msg.get("object_id"):
                self._dds_repository.h_user_insert(msg["payload"]["user"]["id"])
                self._dds_repository.h_restaurant_insert(msg["payload"]["restaurant"]["id"])
                self._dds_repository.h_order_insert(msg["payload"]["id"], msg["payload"]["date"])
                for product in msg["payload"]["products"]:
                    self._dds_repository.h_product_insert(product["id"])
                    self._dds_repository.h_category_insert(product["category"])
                    self._dds_repository.l_order_product_insert(msg["payload"]["id"], product["id"])
                    self._dds_repository.l_product_restaurant_insert(msg["payload"]["restaurant"]["id"], product["id"])
                    self._dds_repository.l_product_category_insert(product["category"], product["id"])
                    self._dds_repository.s_product_names_insert(product["id"], product["name"])
                    
                    message_user_product_counters = {
                        "type": "user_product_counters",
                        "user_id": msg["payload"]["user"]["id"],
                        "product_id": product["id"],
                        "product_name": product["name"]
                    }
                    self._producer.produce(message_user_product_counters)

                    message_user_category_counters = {
                        "type": "user_category_counters",
                        "user_id": msg["payload"]["user"]["id"],
                        "category_name": product["category"]
                    }
                    self._producer.produce(message_user_category_counters)

                self._dds_repository.l_order_user_insert(msg["payload"]["id"], msg["payload"]["user"]["id"])
                self._dds_repository.s_user_names_insert(msg["payload"]["user"]["id"], msg["payload"]["user"]["name"], msg["payload"]["user"]["login"])
                self._dds_repository.s_restaurant_names_insert(msg["payload"]["restaurant"]["id"], msg["payload"]["restaurant"]["name"])
                self._dds_repository.s_order_cost_insert(msg["payload"]["id"], msg["payload"]["cost"], msg["payload"]["payment"])
                self._dds_repository.s_order_status_insert(msg["payload"]["id"], msg["payload"]["status"])
                
                

        self._logger.info(f"{datetime.utcnow()}: FINISH")
