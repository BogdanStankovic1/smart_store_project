USE smart_store;
CREATE TABLE dim_calendar (
    date_id      INT          PRIMARY KEY,
    full_date    DATE         NOT NULL,
    year         INT          NOT NULL,
    quarter      INT          NOT NULL,
    month_number INT          NOT NULL,
    month_name   VARCHAR(20)  NOT NULL,
    week_number  INT          NOT NULL
);
CREATE TABLE dim_customers (
    customer_key  INT          PRIMARY KEY AUTO_INCREMENT,
    customer_id   INT          NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    UNIQUE KEY uq_customer_id (customer_id)
);
CREATE TABLE dim_product (
    product_key          INT           PRIMARY KEY AUTO_INCREMENT,
    product_name         VARCHAR(200)  NOT NULL,
    product_category     VARCHAR(100)  NOT NULL,
    product_sub_category VARCHAR(100)  NOT NULL,
    product_container    VARCHAR(50)   NOT NULL,
    product_base_margin  DECIMAL(5,2),
    UNIQUE KEY uq_product_name (product_name)
);
CREATE TABLE dim_geography (
    geography_id      INT          PRIMARY KEY AUTO_INCREMENT,
    country           VARCHAR(100) NOT NULL,
    region            VARCHAR(50)  NOT NULL,
    state_or_province VARCHAR(100) NOT NULL,
    city              VARCHAR(100) NOT NULL,
    postal_code       CHAR(5)      NOT NULL
);
CREATE TABLE dim_manager (
    manager_id   INT          PRIMARY KEY AUTO_INCREMENT,
    manager_name VARCHAR(100) NOT NULL,
    geography_id INT,
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id)
);
CREATE TABLE dim_segment (
    segment_id   INT         PRIMARY KEY AUTO_INCREMENT,
    segment_name VARCHAR(50) NOT NULL,	
    UNIQUE KEY uq_segment_name (segment_name)
);
CREATE TABLE dim_shipmode (
    shipmode_id INT         PRIMARY KEY AUTO_INCREMENT,
    ship_mode   VARCHAR(50) NOT NULL,
    UNIQUE KEY uq_ship_mode (ship_mode)
);
CREATE TABLE dim_orderpriority (
    orderpriority_id INT         PRIMARY KEY AUTO_INCREMENT,
    order_priority   VARCHAR(50) NOT NULL,
    UNIQUE KEY uq_order_priority (order_priority)
);
CREATE TABLE fact_orders (
    row_id           INT            PRIMARY KEY,
    customer_id      INT            NOT NULL,
    product_id       INT            NOT NULL,
    geography_id     INT            NOT NULL,
    order_date_id    INT            NOT NULL,
    ship_date_id     INT            NOT NULL,
    segment_id       INT            NOT NULL,
    orderpriority_id INT            NOT NULL,
    manager_id       INT            NOT NULL,
    shipmode_id      INT            NOT NULL,
    order_id         INT,
    sales            DECIMAL(10,2),
    profit           DECIMAL(10,2),
    quantity         INT,
    discount         DECIMAL(5,2),
    unit_price       DECIMAL(10,2),
    shipping_cost    DECIMAL(10,2),
    is_returned      BOOLEAN,

    FOREIGN KEY (customer_id)      REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id)       REFERENCES dim_product(product_key),
    FOREIGN KEY (geography_id)     REFERENCES dim_geography(geography_id),
    FOREIGN KEY (order_date_id)    REFERENCES dim_calendar(date_id),
    FOREIGN KEY (ship_date_id)     REFERENCES dim_calendar(date_id),
    FOREIGN KEY (segment_id)       REFERENCES dim_segment(segment_id),
    FOREIGN KEY (orderpriority_id) REFERENCES dim_orderpriority(orderpriority_id),
    FOREIGN KEY (manager_id)       REFERENCES dim_manager(manager_id),
    FOREIGN KEY (shipmode_id)      REFERENCES dim_shipmode(shipmode_id)
);