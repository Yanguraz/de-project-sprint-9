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
