from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:

        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            self._logger.info(msg)
            if msg is None:
                break
            if msg.get("type") == "user_product_counters":
                self._cdm_repository.user_product_counters_insert(msg["user_id"], msg["product_id"], msg["product_name"])
            if msg.get("type") == "user_category_counters":
                self._cdm_repository.user_category_counters_insert(msg["user_id"], msg["category_name"])

        self._logger.info(f"{datetime.utcnow()}: FINISH")
