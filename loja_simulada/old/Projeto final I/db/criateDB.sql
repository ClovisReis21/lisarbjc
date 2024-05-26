CREATE DATABASE vendas;

USE vendas;

CREATE TABLE categories (
    category_id smallint NOT NULL AUTO_INCREMENT,
    category_name varchar(15) NOT NULL,
    pictury varchar(50) NULL,
    description varchar(1000),
    PRIMARY KEY (category_id)
);

CREATE TABLE customers (
    customer_id char(5) NOT NULL,
    company_name varchar(40) NOT NULL,
    contact_name varchar(30),
    contact_title varchar(30),
    address varchar(60),
    city varchar(15),
    region varchar(15),
    postal_code varchar(10),
    country varchar(15),
    phone varchar(24),
    PRIMARY KEY (customer_id)
);

CREATE TABLE employees (
    employee_id smallint NOT NULL AUTO_INCREMENT,
    last_name varchar(20) NOT NULL,
    first_name varchar(10) NOT NULL,
    title varchar(30),
    title_of_courtesy varchar(25),
    birth_date date,
    hire_date date,
    address varchar(60),
    city varchar(15),
    region varchar(15),
    postal_code varchar(10),
    country varchar(15),
    home_phone varchar(24),
    extension varchar(4),
    photo varchar(4) null,
    notes varchar(1000),
    reports_to smallint,
    photo_path varchar(255),
	salary real,
    PRIMARY KEY (employee_id)
);

CREATE TABLE shippers (
    shipper_id smallint NOT NULL AUTO_INCREMENT,
    company_name varchar(40) NOT NULL,
    phone varchar(24),
    PRIMARY KEY (shipper_id)
);

CREATE TABLE suppliers (
    supplier_id smallint NOT NULL AUTO_INCREMENT,
    company_name varchar(40) NOT NULL,
    contact_name varchar(30),
    contact_title varchar(30),
    address varchar(60),
    city varchar(15),
    region varchar(15),
    postal_code varchar(10),
    country varchar(15),
    phone varchar(24),
    fax varchar(24),
    homepage varchar(1000),
    PRIMARY KEY (supplier_id)
);

CREATE TABLE products (
    product_id smallint NOT NULL AUTO_INCREMENT,
    product_name varchar(40) NOT NULL,
    supplier_id smallint,
    category_id smallint,
    quantity_per_unit varchar(20),
    unit_price decimal(10,2),
    units_in_stock smallint,
    units_on_order smallint,
    reorder_level smallint,
    discontinued integer NOT NULL,
    PRIMARY KEY (product_id),
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

CREATE TABLE orders (
    order_id smallint NOT NULL AUTO_INCREMENT,
    customer_id char(5),
    employee_id smallint,
    shipper_id smallint,
    order_date date,
    required_date date,
    shipped_date date,
    ship_via smallint,
    freight decimal(10,2),
    ship_name varchar(40),
    ship_address varchar(60),
    ship_city varchar(15),
    ship_region varchar(15),
    ship_postal_code varchar(10),
    ship_country varchar(15),
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (shipper_id) REFERENCES shippers(shipper_id)
);

CREATE TABLE order_details (
    order_detail_id smallint NOT NULL AUTO_INCREMENT,
    order_id smallint NOT NULL,
    product_id smallint NOT NULL,
    unit_price decimal(10,2) NOT NULL,
    quantity smallint NOT NULL,
    discount decimal(10,2) NOT NULL,
    PRIMARY KEY (order_detail_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)    
);
