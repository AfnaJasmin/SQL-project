CREATE TABLE sales_raw (
    Row_id INT,
    Order_id VARCHAR(20),
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    product_id VARCHAR(20),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(255),
    sales NUMERIC(10,2),
    custom VARCHAR(20)
);

DROP TABLE IF EXISTS sales_raw;

CREATE TABLE sales_raw (
    "Order ID" VARCHAR(20),
    "Order Date" DATE,
    "Ship Date" DATE,
    "Ship Mode" VARCHAR(50),
    "Customer ID" VARCHAR(20),
    "Customer Name" VARCHAR(100),
    "Segment" VARCHAR(50),
    "Country" VARCHAR(50),
    "City" VARCHAR(100),
    "State" VARCHAR(100),
    "Postal Code" VARCHAR(20),
    "Region" VARCHAR(50),
    "Product ID" VARCHAR(20),
    "Category" VARCHAR(50),
    "Sub-Category" VARCHAR(50),
    "Product Name" VARCHAR(255),
    "Sales" NUMERIC(10,2),
    "Custom" VARCHAR(20)
);



COPY sales_raw
FROM 'C:\Program Files\PostgreSQL\18\data\SALES RAWWW.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    QUOTE '"',
    ESCAPE '"',
    NULL ''
);

select * from sales_raw;


----normalized table customers

CREATE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(50)
);

INSERT INTO customers (customer_id, customer_name, segment, country, city, state, postal_code, region)
SELECT DISTINCT
    "Customer ID",
    "Customer Name",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region"
FROM sales_raw;

drop table customers;

CREATE TABLE customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(50)
);

INSERT INTO customers (customer_id, customer_name, segment, country, city, state, postal_code, region)
SELECT DISTINCT
    "Customer ID",
    "Customer Name",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region"
FROM sales_raw;

select * from customers;



---normalized  table customers 


CREATE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY,
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(255)
);

drop table products;

CREATE TABLE products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(255)
);

INSERT INTO products (product_id, category, sub_category, product_name)
SELECT DISTINCT
    "Product ID",
    "Category",
    "Sub-Category",
    "Product Name"
FROM sales_raw;

select * from products;


---normalized table orders 


CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_key INT,
    FOREIGN KEY (customer_key) REFERENCES customers(customer_key)
);

INSERT INTO orders (order_id, order_date, ship_date, ship_mode, customer_key)
SELECT DISTINCT
    s."Order ID",
    s."Order Date",
    s."Ship Date",
    s."Ship Mode",
    c.customer_key
FROM sales_raw s
JOIN customers c
  ON s."Customer ID" = c.customer_id
 AND s."Customer Name" = c.customer_name;


drop table orders;

CREATE TABLE orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(20),
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_key INT,
    FOREIGN KEY (customer_key) REFERENCES customers(customer_key)
);

INSERT INTO orders (order_id, order_date, ship_date, ship_mode, customer_key)
SELECT DISTINCT
    s."Order ID",
    s."Order Date",
    s."Ship Date",
    s."Ship Mode",
    c.customer_key
FROM sales_raw s
JOIN customers c
  ON s."Customer ID" = c.customer_id
 AND s."Customer Name" = c.customer_name;

 select * from orders;


---normalized table orderdetails 


CREATE TABLE order_details (
    order_detail_key SERIAL PRIMARY KEY,
    order_key INT,
    product_key INT,
    sales NUMERIC(10,2),
    custom_status VARCHAR(50),
    FOREIGN KEY (order_key) REFERENCES orders(order_key),
    FOREIGN KEY (product_key) REFERENCES products(product_key)
);

INSERT INTO order_details (order_key, product_key, sales, custom_status)
SELECT
    o.order_key,
    p.product_key,
    s."Sales",
    s."Custom"
FROM sales_raw s
JOIN orders o
  ON s."Order ID" = o.order_id
JOIN products p
  ON s."Product ID" = p.product_id
 AND s."Product Name" = p.product_name;

 select * from order_details;


1---What is the total sales amount for each product category?
SELECT 
    p.category,
    ROUND(SUM(od.sales), 2) AS total_sales
FROM order_details od
JOIN products p 
    ON od.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;


2---How many orders were placed each year?
SELECT 
    EXTRACT(YEAR FROM o.order_date) AS order_year,
    COUNT(o.order_key) AS total_orders
FROM orders o
GROUP BY EXTRACT(YEAR FROM o.order_date)
ORDER BY order_year;

3---Which customer has placed the highest total order value?
SELECT 
    c.customer_id,
    c.customer_name,
    ROUND(SUM(od.sales), 2) AS total_order_value
FROM customers c
JOIN orders o 
    ON c.customer_key = o.customer_key
JOIN order_details od 
    ON o.order_key = od.order_key
GROUP BY c.customer_id, c.customer_name
ORDER BY total_order_value DESC
LIMIT 1;


4---Rank all customer segments based on total sales to identify the highest contributing segment?
SELECT 
    c.segment,
    ROUND(SUM(od.sales), 2) AS total_sales,
    RANK() OVER (ORDER BY SUM(od.sales) DESC) AS rank_no
FROM customers c
JOIN orders o 
    ON c.customer_key = o.customer_key
JOIN order_details od 
    ON o.order_key = od.order_key
GROUP BY c.segment
ORDER BY total_sales DESC;


5---Find the total number of orders and total sales per customer?
SELECT 
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT o.order_key) AS total_orders,
    ROUND(SUM(od.sales), 2) AS total_sales
FROM customers c
JOIN orders o 
    ON c.customer_key = o.customer_key
JOIN order_details od 
    ON o.order_key = od.order_key
GROUP BY c.customer_id, c.customer_name
ORDER BY total_sales DESC;


6---Which region generated the most revenue?
SELECT 
    c.region,
    ROUND(SUM(od.sales), 2) AS total_revenue
FROM customers c
JOIN orders o 
    ON c.customer_key = o.customer_key
JOIN order_details od 
    ON o.order_key = od.order_key
GROUP BY c.region
ORDER BY total_revenue DESC
LIMIT 1;




7---What are the top 5 cities contributing to total revenue?
SELECT
    c.city,
    c.state,
    c.region,
    ROUND(SUM(od.sales), 2) AS total_revenue
FROM customers c
JOIN orders o 
    ON c.customer_key = o.customer_key
JOIN order_details od 
    ON o.order_key = od.order_key
GROUP BY c.city, c.state, c.region
ORDER BY total_revenue DESC
LIMIT 5;



8---Which shipping mode is most commonly used and gives highest sales?
SELECT 
    o.ship_mode,
    COUNT(o.order_key) AS total_orders,
    ROUND(SUM(od.sales), 2) AS total_sales
FROM orders o
JOIN order_details od 
    ON o.order_key = od.order_key
GROUP BY o.ship_mode
ORDER BY total_sales DESC
LIMIT 1;


9---List top 10 products by total sales?
WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.sub_category,
        ROUND(SUM(od.sales), 2) AS total_sales,
        RANK() OVER (ORDER BY SUM(od.sales) DESC) AS sales_rank
    FROM products p
    JOIN order_details od 
        ON p.product_key = od.product_key
    GROUP BY p.product_id, p.product_name, p.category, p.sub_category
)
SELECT 
    product_id,
    product_name,
    category,
    sub_category,
    total_sales,
    sales_rank
FROM product_sales
WHERE sales_rank <= 10
ORDER BY sales_rank;



10--- Find the average delivery time (Ship_Date - Order_Date) for each shipping mode?
SELECT 
    ship_mode,
    ROUND(AVG(ship_date - order_date), 2) AS avg_delivery_days
FROM 
    orders
GROUP BY 
    ship_mode
ORDER BY 
    avg_delivery_days;





