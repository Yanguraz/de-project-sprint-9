from datetime import datetime
from logging import Logger
from typing import Any, Dict, List, Tuple

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger,
                 batch_size: int = 100) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size

    def _process_message(self, msg: Dict[str, Any]) -> None:
        user_id = msg['user_id']
        products_info = list(zip(
            msg['product_id'], 
            msg['product_name'], 
            msg['category_id'], 
            msg['category_name'], 
            msg['order_cnt']
        ))

        for product in products_info:
            self._process_product_info(user_id, product)

    def _process_product_info(self, user_id: str, product: Tuple[str, str, str, str, int]) -> None:
        self._cdm_repository.user_product_counters_insert(user_id, product[0], product[1], product[4])
        self._cdm_repository.user_category_counters_insert(user_id, product[2], product[3])

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            self._process_message(msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
