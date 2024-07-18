from typing import List
from datetime import datetime
from dds_models import (H_User, H_Product, H_Category, H_Restaurant, H_Order,  L_Order_Product,
                        L_Product_Restaurant, L_Product_Restaurant, L_Order_User, S_User_Names, S_Product_Names,
                        S_Restaurant_Names, S_Order_Cost, S_Order_Status
                        )


class OrderDdsBuilder:
    def __init__(self, data: dict) -> None:
        self._data = data
        self.source_system = ""
        self.order_ns_uuid = uuid.UUID('12345678-1234-5678-1234-567812345678')
    def _uuid(self, obj: any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))

    def h_user(self) -> H_User:
        user_id = self._data['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_product(self) -> List[H_Product]:
        products = []
        for product in self._data['products']:
            product_id = product['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(product_id),
                    product_id=product_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products

    def h_category(self) -> List[H_Category]:
        categories = []
        for category in self._data['categories']:
            category_name = category['name']
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return categories

    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = self._data['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_order(self) -> H_Order:
        order_id = self._data['order']['id']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=self._data['order']['date'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def l_order_product(self, h_order_pk: uuid.UUID, products: List[H_Product]) -> List[L_Order_Product]:
        links = []
        for product in products:
            links.append(
                L_Order_Product(
                    hk_order_product_pk=self._uuid(f"{h_order_pk}{product.h_product_pk}"),
                    h_order_pk=h_order_pk,
                    h_product_pk=product.h_product_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return links

    def l_product_restaurant(self, products: List[H_Product], h_restaurant_pk: uuid.UUID) -> List[L_Product_Restaurant]:
        links = []
        for product in products:
            links.append(
                L_Product_Restaurant(
                    hk_product_restaurant_pk=self._uuid(f"{product.h_product_pk}{h_restaurant_pk}"),
                    h_product_pk=product.h_product_pk,
                    h_restaurant_pk=h_restaurant_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return links

    def l_product_category(self, products: List[H_Product], categories: List[H_Category]) -> List[L_Product_Category]:
        links = []
        for product in products:
            for category in categories:
                links.append(
                    L_Product_Category(
                        hk_product_category_pk=self._uuid(f"{product.h_product_pk}{category.h_category_pk}"),
                        h_product_pk=product.h_product_pk,
                        h_category_pk=category.h_category_pk,
                        load_dt=datetime.utcnow(),
                        load_src=self.source_system
                    )
                )
        return links

    def l_order_user(self, h_order_pk: uuid.UUID, user: H_User) -> L_Order_User:
        return L_Order_User(
            hk_order_user_pk=self._uuid(f"{h_order_pk}{user.h_user_pk}"),
            h_order_pk=h_order_pk,
            h_user_pk=user.h_user_pk,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def s_user_names(self, user: H_User) -> S_User_Names:
        return S_User_Names(
            h_user_pk=user.h_user_pk,
            username=self._data['user']['name'],
            userlogin=self._data['user']['login'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid(f"{user.h_user_pk}{self._data['user']['name']}{self._data['user']['login']}")
        )

    def s_product_names(self, products: List[H_Product]) -> List[S_Product_Names]:
        names = []
        for product in products:
            names.append(
                S_Product_Names(
                    h_product_pk=product.h_product_pk,
                    name=product.product_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(f"{product.h_product_pk}{product.product_id}")
                )
            )
        return names

    def s_restaurant_names(self, restaurant: H_Restaurant) -> S_Restaurant_Names:
        return S_Restaurant_Names(
            h_restaurant_pk=restaurant.h_restaurant_pk,
            name=self._data['restaurant']['name'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid(f"{restaurant.h_restaurant_pk}{self._data['restaurant']['name']}")
        )

    def s_order_cost(self, order: H_Order) -> S_Order_Cost:
        return S_Order_Cost(
            h_order_pk=order.h_order_pk,
            cost=self._data['order']['cost'],
            payment=self._data['order']['payment'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=self._uuid(f"{order.h_order_pk}{self._data['order']['cost']}{self._data['order']['payment']}")
        )

    def s_order_status(self, order: H_Order) -> S_Order_Status:
        return S_Order_Status(
            h_order_pk=order.h_order_pk,
            status=self._data['status'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid(f"{order.h_order_pk}{self._data['status']}")
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


