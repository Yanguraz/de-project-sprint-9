from datetime import datetime
from logging import Logger
import uuid

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository
from dds_loader.builder import OrderDdsBuilder

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            payload = msg['payload']

            # Обрабатываем только сообщения со статусом 'CLOSED'
            if payload['status'] != 'CLOSED':
                continue

            load_dt = datetime.now() 
            load_src = self._consumer.topic

            builder = OrderDdsBuilder(payload)
            
            h_user = builder.h_user()
            h_products = builder.h_product()
            h_categories = builder.h_category()
            h_restaurant = builder.h_restaurant()
            h_order = builder.h_order()
            
            l_order_product_links = builder.l_order_product(h_order.h_order_pk, h_products)
            l_product_restaurant_links = builder.l_product_restaurant(h_products, h_restaurant.h_restaurant_pk)
            l_product_category_links = builder.l_product_category(h_products, h_categories)
            l_order_user_link = builder.l_order_user(h_order.h_order_pk, h_user)
            
            s_user_names = builder.s_user_names(h_user)
            s_product_names = builder.s_product_names(h_products)
            s_restaurant_names = builder.s_restaurant_names(h_restaurant)
            s_order_cost = builder.s_order_cost(h_order)
            s_order_status = builder.s_order_status(h_order)
            
            # Вставка данных в DdsRepository
            self._dds_repository.insert_h_user(h_user)
            self._dds_repository.insert_h_product(h_products)
            self._dds_repository.insert_h_category(h_categories)
            self._dds_repository.insert_h_restaurant(h_restaurant)
            self._dds_repository.insert_h_order(h_order)
            
            self._dds_repository.insert_l_order_product(l_order_product_links)
            self._dds_repository.insert_l_product_restaurant(l_product_restaurant_links)
            self._dds_repository.insert_l_product_category(l_product_category_links)
            self._dds_repository.insert_l_order_user(l_order_user_link)
            
            self._dds_repository.insert_s_user_names(s_user_names)
            self._dds_repository.insert_s_product_names(s_product_names)
            self._dds_repository.insert_s_restaurant_names(s_restaurant_names)
            self._dds_repository.insert_s_order_cost(s_order_cost)
            self._dds_repository.insert_s_order_status(s_order_status)

             # Формирование и отправка итогового сообщения в топик
            dst_msg = {
                "user_id": h_user.h_user_pk,
                "product_id": [p.h_product_pk for p in h_products],
                "product_name": [p.name for p in h_products],
                "category_id": [c.h_category_pk for c in h_categories],
                "category_name": [c.category_name for c in h_categories],
                "order_cnt": [p.quantity for p in h_products]
            }

            self._producer.produce(dst_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
