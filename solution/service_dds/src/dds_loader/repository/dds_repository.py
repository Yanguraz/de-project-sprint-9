from typing import List
from datetime import datetime
from dds_models import (H_User, H_Product, H_Category, H_Restaurant, H_Order,  L_Order_Product,
                        L_Product_Restaurant, L_Product_Restaurant, L_Order_User, S_User_Names, S_Product_Names,
                        S_Restaurant_Names, S_Order_Cost, S_Order_Status
                        )


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def _execute_query(self, query: str, params: dict) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()

    def insert_h_user(self, user: H_User) -> None:
        query = """
            INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
            VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_user_pk) DO NOTHING;
        """
        self._execute_query(query, user.dict())

    def insert_h_product(self, products: List[H_Product]) -> None:
        query = """
            INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
            VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_product_pk) DO NOTHING;
        """
        for product in products:
            self._execute_query(query, product.dict())

    def insert_h_category(self, categories: List[H_Category]) -> None:
        query = """
            INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
            VALUES (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_category_pk) DO NOTHING;
        """
        for category in categories:
            self._execute_query(query, category.dict())

    def insert_h_restaurant(self, restaurant: H_Restaurant) -> None:
        query = """
            INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
            VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_restaurant_pk) DO NOTHING;
        """
        self._execute_query(query, restaurant.dict())

    def insert_h_order(self, order: H_Order) -> None:
        query = """
            INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
            VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_order_pk) DO NOTHING;
        """
        self._execute_query(query, order.dict())

    def insert_l_order_product(self, links: List[L_Order_Product]) -> None:
        query = """
            INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
            VALUES (%(hk_order_product_pk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_order_product_pk) DO NOTHING;
        """
        for link in links:
            self._execute_query(query, link.dict())

    def insert_l_product_restaurant(self, links: List[L_Product_Restaurant]) -> None:
        query = """
            INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
            VALUES (%(hk_product_restaurant_pk)s, %(h_product_pk)s, %(h_restaurant_pk)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
        """
        for link in links:
            self._execute_query(query, link.dict())

    def insert_l_product_category(self, links: List[L_Product_Category]) -> None:
        query = """
            INSERT INTO dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
            VALUES (%(hk_product_category_pk)s, %(h_product_pk)s, %(h_category_pk)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_product_category_pk) DO NOTHING;
        """
        for link in links:
            self._execute_query(query, link.dict())

    def insert_l_order_user(self, link: L_Order_User) -> None:
        query = """
            INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
            VALUES (%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_order_user_pk) DO NOTHING;
        """
        self._execute_query(query, link.dict())

    def insert_s_user_names(self, user_names: S_User_Names) -> None:
        query = """
            INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
            VALUES (%(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s, %(hk_user_names_hashdiff)s)
            ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
        """
        self._execute_query(query, user_names.dict())

    def insert_s_product_names(self, product_names: List[S_Product_Names]) -> None:
        query = """
            INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
            VALUES (%(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_product_names_hashdiff)s)
            ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
        """
        for product_name in product_names:
            self._execute_query(query, product_name.dict())

    def insert_s_restaurant_names(self, restaurant_names: S_Restaurant_Names) -> None:
        query = """
            INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
            VALUES (%(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_restaurant_names_hashdiff)s)
            ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
        """
        self._execute_query(query, restaurant_names.dict())

    def insert_s_order_cost(self, order_cost: S_Order_Cost) -> None:
        query = """
            INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
            VALUES (%(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s, %(hk_order_cost_hashdiff)s)
            ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;
        """
        self._execute_query(query, order_cost.dict())

    def insert_s_order_status(self, order_status: S_Order_Status) -> None:
        query = """
            INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
            VALUES (%(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s, %(hk_order_status_hashdiff)s)
            ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
        """
        self._execute_query(query, order_status.dict())


