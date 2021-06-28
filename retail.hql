# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

-- create hive tables
create external table departments(
 department_id int,
 department_name string)
 row format delimited fields terminated by ','
 stored as textfile location 'gs://${WAREHOUSE_BUCKET}/retail/departments';

create external table categories(
 category_id int,
 category_department_id int,
 category_name string)
 row format delimited fields terminated by ','
 stored as textfile location 'gs://${WAREHOUSE_BUCKET}/retail/categories';

create external table products(
  product_id int,
  product_category_id int,
  product_name string,
  product_description string,
  product_price float,
  product_image string)
  row format delimited fields terminated by ','
  stored as textfile location 'gs://${WAREHOUSE_BUCKET}/retail/products';

create external table order_items(
 order_item_id int,
 order_item_order_id int,
 order_item_product_id int,
 order_item_quantity int,
 order_item_subtotal float,
 order_item_product_price float)
 row format delimited fields terminated by ','
 stored as textfile location 'gs://${WAREHOUSE_BUCKET}/retail/order_items';

create external table orders(
 order_id int,
 order_date string,
 order_customer_id int,
 order_status string)
 row format delimited fields terminated by ','
 stored as textfile location 'gs://${WAREHOUSE_BUCKET}/retail/orders';

create external table customers(
 customer_id int,
 customer_fname string,
 customer_lname string,
 customer_email string,
 customer_password string,
 customer_street string,
 customer_city string,
 customer_state string,
 customer_zipcode string)
 row format delimited fields terminated by ','
 stored as textfile location 'gs://${WAREHOUSE_BUCKET}/retail/customers';
