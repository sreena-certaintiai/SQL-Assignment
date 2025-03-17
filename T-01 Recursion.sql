-- ------------------------------------------------------------------------------------------------------------------
-- TASK 1
-- ------------------------------------------------------------------------------------------------------------------
CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    location VARCHAR(255) NOT NULL,
    manager_id INT NULL 
)

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(100) NOT NULL,
	--cascade is to delete the store id if the stores are deleted 
    store_id INT REFERENCES stores(store_id) ON DELETE CASCADE, 
    salary DECIMAL(10,2) CHECK (salary > 0),
    manager_id INT NULL, 
	--make the emp_id null if mg_id is deleted
    FOREIGN KEY (manager_id) REFERENCES employees(employee_id) ON DELETE SET NULL
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL CHECK (email LIKE '%@%.%'), --mostly like doesnt support
    phone VARCHAR(15) UNIQUE NOT NULL,
    city VARCHAR(100) NOT NULL
);


CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255) NOT NULL,
    phone VARCHAR(15) UNIQUE NOT NULL,
    city VARCHAR(100) NOT NULL
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) CHECK (price > 0),
    stock INT CHECK (stock >= 0),
    supplier_id INT REFERENCES suppliers(supplier_id) ON DELETE CASCADE
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
    store_id INT REFERENCES stores(store_id) ON DELETE CASCADE,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2) CHECK (total_amount >= 0)
);


CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INT REFERENCES products(product_id) ON DELETE CASCADE,
    quantity INT CHECK (quantity > 0),
    price DECIMAL(10,2) CHECK (price > 0)
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
    amount DECIMAL(10,2) CHECK (amount > 0),
    payment_method VARCHAR(50) CHECK (payment_method IN ('Cash', 'Credit Card', 'UPI', 'Net Banking')),
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 2
-- ------------------------------------------------------------------------------------------------------------------

CREATE INDEX idx_product_name ON products(name);

SELECT * FROM products WHERE name ILIKE '%laptop%';

CREATE INDEX idx_customer_order ON orders(customer_id, order_date);

SELECT * FROM orders WHERE customer_id = 5 ORDER BY order_date DESC;

-- ------------------------------------------------------------------------------------------------------------------
SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public';

-- to ensure the search is done through index explain analyze is used
EXPLAIN ANALYZE SELECT * FROM products WHERE name ILIKE '%laptop%';

-- ------------------------------------------------------------------------------------------------------------------
--TASK 3
-- ------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW top_selling_products AS 
SELECT 
    oi.product_id, 
    p.name AS product_name, 
    SUM(oi.quantity) AS total_sold
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY oi.product_id, p.name
ORDER BY total_sold DESC;

-- SELECT * FROM top_selling_products LIMIT 10;

CREATE OR REPLACE VIEW store_revenue AS 
SELECT 
    o.store_id, 
    s.name AS store_name, 
    SUM(o.total_amount) AS total_revenue
FROM orders o
JOIN stores s ON o.store_id = s.store_id
GROUP BY o.store_id, s.name
ORDER BY total_revenue DESC;

-- SELECT * FROM store_revenue;

-- SELECT table_name FROM information_schema.views WHERE table_schema = 'public';

-- ------------------------------------------------------------------------------------------------------------------
--TASK 4
-- ------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION prevent_out_of_stock() 
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the ordered quantity exceeds available stock
    IF (SELECT stock FROM products WHERE product_id = NEW.product_id) < NEW.quantity THEN
        RAISE EXCEPTION 'Cannot place order: Product is out of stock!';
    END IF;
    RETURN NEW;
END;   
$$ LANGUAGE plpgsql;

-- Attach the trigger to the order_items table
CREATE OR REPLACE TRIGGER trg_prevent_out_of_stock
BEFORE INSERT ON order_items
FOR EACH ROW
EXECUTE FUNCTION prevent_out_of_stock();

-- Step 1: Create an audit table to store deleted employee records
CREATE TABLE employee_audit (
    audit_id SERIAL PRIMARY KEY,
    employee_id INT,
    name VARCHAR(255),
    role VARCHAR(100),
    store_id INT,
    salary DECIMAL(10,2),
    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

Step 2: Create a function to log deleted employees
CREATE OR REPLACE FUNCTION log_deleted_employee()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO employee_audit (employee_id, name, role, store_id, salary)
    VALUES (OLD.employee_id, OLD.name, OLD.role, OLD.store_id, OLD.salary);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Step 3: Attach the trigger to the employees table
CREATE OR REPLACE TRIGGER trg_log_deleted_employee
BEFORE DELETE ON employees
FOR EACH ROW
EXECUTE FUNCTION log_deleted_employee();

DELETE FROM employees WHERE employee_id = 5;
SELECT * FROM employee_audit;

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 5
-- ------------------------------------------------------------------------------------------------------------------

INSERT INTO stores (name, location, manager_id) VALUES 
('ShopEase - New York', 'New York, USA', 1),
('ShopEase - Los Angeles', 'Los Angeles, USA', 2),
('ShopEase - Chicago', 'Chicago, USA', 3),
('ShopEase - Houston', 'Houston, USA', 4),
('ShopEase - Miami', 'Miami, USA', 5);

INSERT INTO employees (name, role, store_id, salary, manager_id) VALUES 
('Alice Johnson', 'Manager', 1, 70000, NULL),  -- Store Manager (No manager)
('Bob Smith', 'Cashier', 1, 35000, 1),
('Charlie Brown', 'Sales Associate', 1, 32000, 1),
('David Wilson', 'Manager', 2, 72000, NULL),
('Emma Davis', 'Cashier', 2, 36000, 4),
('Frank White', 'Manager', 3, 71000, NULL),
('Grace Miller', 'Sales Associate', 3, 31000, 6),
('Hannah Lewis', 'Manager', 4, 70000, NULL),
('Isaac Thomas', 'Cashier', 4, 34000, 8),
('Jack Moore', 'Manager', 5, 73000, NULL);

INSERT INTO suppliers (name, contact_person, phone, city) VALUES 
('Tech World', 'John Carter', '1234567890', 'San Francisco'),
('Home Supplies Ltd.', 'Sarah Green', '2345678901', 'Dallas'),
('Fashion Hub', 'Emma White', '3456789012', 'New York'),
('Sports Gear Inc.', 'Michael Brown', '4567890123', 'Chicago'),
('Gourmet Foods', 'Oliver Smith', '5678901234', 'Houston'),
('Furniture Masters', 'Sophia Miller', '6789012345', 'Los Angeles'),
('Health Essentials', 'Liam Johnson', '7890123456', 'San Diego'),
('Toy Universe', 'Ethan Lewis', '8901234567', 'Miami'),
('Pet Care Co.', 'Mia Wilson', '9012345678', 'Seattle'),
('Automotive Parts', 'Lucas Anderson', '0123456789', 'Boston');

INSERT INTO customers (name, email, phone, city) VALUES 
('John Doe', 'john.doe@example.com', '1112223333', 'New York'),
('Jane Smith', 'jane.smith@example.com', '2223334444', 'Los Angeles'),
('Robert Brown', 'robert.brown@example.com', '3334445555', 'Chicago'),
('Emily Johnson', 'emily.johnson@example.com', '4445556666', 'Houston'),
('William Davis', 'william.davis@example.com', '5556667777', 'Miami'),
('Olivia Wilson', 'olivia.wilson@example.com', '6667778888', 'San Francisco'),
('Liam Martinez', 'liam.martinez@example.com', '7778889999', 'Dallas'),
('Sophia Garcia', 'sophia.garcia@example.com', '8889990000', 'Seattle'),
('Mason Taylor', 'mason.taylor@example.com', '9990001111', 'Boston'),
('Isabella Clark', 'isabella.clark@example.com', '0001112222', 'San Diego'),
('James White', 'james.white@example.com', '1113335555', 'New York'),
('Charlotte Adams', 'charlotte.adams@example.com', '2224446666', 'Los Angeles'),
('Benjamin Harris', 'benjamin.harris@example.com', '3335557777', 'Chicago'),
('Ava Thomas', 'ava.thomas@example.com', '4446668888', 'Houston'),
('Lucas Lewis', 'lucas.lewis@example.com', '5557779999', 'Miami'),
('Mia Walker', 'mia.walker@example.com', '6668880000', 'San Francisco'),
('Henry King', 'henry.king@example.com', '7779991111', 'Dallas'),
('Ella Wright', 'ella.wright@example.com', '8880002222', 'Seattle'),
('Alexander Scott', 'alexander.scott@example.com', '9991113333', 'Boston'),
('Amelia Baker', 'amelia.baker@example.com', '0002224444', 'San Diego');

INSERT INTO products (name, category, price, stock, supplier_id) VALUES 
('Laptop', 'Electronics', 999.99, 50, 1),
('Smartphone', 'Electronics', 699.99, 100, 1),
('Headphones', 'Electronics', 49.99, 200, 1),
('Sofa Set', 'Furniture', 899.99, 20, 6),
('Dining Table', 'Furniture', 499.99, 15, 6),
('Running Shoes', 'Sports', 79.99, 120, 4),
('Treadmill', 'Sports', 599.99, 10, 4),
('Winter Jacket', 'Clothing', 129.99, 60, 3),
('Sunglasses', 'Accessories', 79.99, 90, 3),
('Coffee Maker', 'Kitchen', 99.99, 50, 2),
('Microwave Oven', 'Kitchen', 199.99, 30, 2),
('Yoga Mat', 'Fitness', 29.99, 80, 4),
('Tennis Racket', 'Sports', 149.99, 40, 4),
('Dog Food', 'Pet Supplies', 29.99, 150, 9),
('Cat Litter', 'Pet Supplies', 19.99, 200, 9),
('Car Tires', 'Automotive', 149.99, 75, 10);

select * from products

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 5
-- ------------------------------------------------------------------------------------------------------------------

to-do csv ellame create panum so hold on and go to TASK 6

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 6
-- ------------------------------------------------------------------------------------------------------------------
WITH RECURSIVE employee_hierarchy AS (
    -- Base Case: Select the CEO (or top-level managers)
    SELECT 
        employee_id, 
        name, 
        role, 
        manager_id,
        1 AS level  -- Level 1: Top-most manager (CEO)
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive Case: Select employees reporting to the previous level
    SELECT 
        e.employee_id, 
        e.name, 
        e.role, 
        e.manager_id,
        eh.level + 1 AS level
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT * FROM employee_hierarchy ORDER BY level, manager_id;

INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
(1, 2, 5, 500),
(2, 3, 2, 150),
(3, 4, 8, 200);

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 8 PIVOT TABLE
-- ------------------------------------------------------------------------------------------------------------------
INSERT INTO orders (customer_id, store_id, order_date, total_amount) VALUES
(1, 1, '2024-01-05', 500.00),
(2, 1, '2024-02-10', 700.00),
(3, 2, '2024-02-15', 450.00),
(4, 2, '2024-03-12', 300.00),
(5, 3, '2024-01-08', 600.00),
(6, 3, '2024-04-20', 1200.00),
(7, 4, '2024-05-22', 350.00),
(8, 4, '2024-06-18', 275.00),
(9, 5, '2024-07-05', 800.00),
(10, 5, '2024-08-30', 950.00),
(11, 1, '2024-09-12', 650.00),
(12, 2, '2024-10-15', 720.00),
(13, 3, '2024-11-18', 540.00),
(14, 4, '2024-12-22', 430.00),
(15, 5, '2024-01-10', 680.00),
(16, 1, '2024-02-25', 490.00),
(17, 2, '2024-03-08', 310.00),
(18, 3, '2024-04-18', 120.00),
(19, 4, '2024-05-28', 750.00),
(20, 5, '2024-06-05', 880.00);

INSERT INTO orders (customer_id, store_id, order_date, total_amount) VALUES
(1, 1, '2024-01-05', 500.00),
(2, 2, '2024-01-10', 720.00),
(3, 3, '2024-01-15', 430.00),
(4, 4, '2024-01-20', 380.00),
(5, 5, '2024-01-25', 650.00),
(6, 1, '2024-02-05', 710.00),
(7, 2, '2024-02-10', 300.00),
(8, 3, '2024-02-15', 520.00),
(9, 4, '2024-02-20', 600.00),
(10, 5, '2024-02-25', 900.00),
(11, 1, '2024-03-01', 850.00),
(12, 2, '2024-03-05', 370.00),
(13, 3, '2024-03-10', 490.00),
(14, 4, '2024-03-15', 780.00),
(15, 5, '2024-03-20', 890.00),
(16, 1, '2024-04-01', 430.00),
(17, 2, '2024-04-05', 610.00),
(18, 3, '2024-04-10', 720.00),
(19, 4, '2024-04-15', 520.00),
(20, 5, '2024-04-20', 480.00),
(1, 1, '2024-05-05', 750.00),
(2, 2, '2024-05-10', 320.00),
(3, 3, '2024-05-15', 530.00),
(4, 4, '2024-05-20', 640.00),
(5, 5, '2024-05-25', 580.00),
(6, 1, '2024-06-01', 910.00),
(7, 2, '2024-06-05', 750.00),
(8, 3, '2024-06-10', 680.00),
(9, 4, '2024-06-15', 570.00),
(10, 5, '2024-06-20', 870.00),
(11, 1, '2024-07-01', 620.00),
(12, 2, '2024-07-05', 450.00),
(13, 3, '2024-07-10', 740.00),
(14, 4, '2024-07-15', 350.00),
(15, 5, '2024-07-20', 960.00),
(16, 1, '2024-08-01', 510.00),
(17, 2, '2024-08-05', 820.00),
(18, 3, '2024-08-10', 390.00),
(19, 4, '2024-08-15', 430.00),
(20, 5, '2024-08-20', 770.00),
(1, 1, '2024-09-01', 590.00),
(2, 2, '2024-09-05', 450.00),
(3, 3, '2024-09-10', 600.00),
(4, 4, '2024-09-15', 540.00),
(5, 5, '2024-09-20', 820.00),
(6, 1, '2024-10-01', 390.00),
(7, 2, '2024-10-05', 920.00),
(8, 3, '2024-10-10', 740.00),
(9, 4, '2024-10-15', 510.00),
(10, 5, '2024-10-20', 630.00),
(11, 1, '2024-11-01', 840.00),
(12, 2, '2024-11-05', 410.00),
(13, 3, '2024-11-10', 530.00),
(14, 4, '2024-11-15', 480.00),
(15, 5, '2024-11-20', 720.00),
(16, 1, '2024-12-01', 490.00),
(17, 2, '2024-12-05', 860.00),
(18, 3, '2024-12-10', 400.00),
(19, 4, '2024-12-15', 690.00),
(20, 5, '2024-12-20', 940.00),
(1, 1, '2025-01-01', 510.00),
(2, 2, '2025-01-05', 780.00),
(3, 3, '2025-01-10', 560.00),
(4, 4, '2025-01-15', 430.00),
(5, 5, '2025-01-20', 880.00),
(6, 1, '2025-02-01', 600.00),
(7, 2, '2025-02-05', 470.00),
(8, 3, '2025-02-10', 730.00),
(9, 4, '2025-02-15', 530.00),
(10, 5, '2025-02-20', 910.00),
(11, 1, '2025-03-01', 490.00),
(12, 2, '2025-03-05', 870.00),
(13, 3, '2025-03-10', 420.00),
(14, 4, '2025-03-15', 650.00),
(15, 5, '2025-03-20', 890.00),
(16, 1, '2025-04-01', 480.00),
(17, 2, '2025-04-05', 560.00),
(18, 3, '2025-04-10', 400.00),
(19, 4, '2025-04-15', 720.00),
(20, 5, '2025-04-20', 910.00);

-- MANUAL PIVOT 
SELECT store_id,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 1 THEN total_amount ELSE 0 END) AS "Jan",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 2 THEN total_amount ELSE 0 END) AS "Feb",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 3 THEN total_amount ELSE 0 END) AS "Mar",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 4 THEN total_amount ELSE 0 END) AS "Apr",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 5 THEN total_amount ELSE 0 END) AS "May",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 6 THEN total_amount ELSE 0 END) AS "Jun",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 7 THEN total_amount ELSE 0 END) AS "Jul",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 8 THEN total_amount ELSE 0 END) AS "Aug",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 9 THEN total_amount ELSE 0 END) AS "Sep",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 10 THEN total_amount ELSE 0 END) AS "Oct",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 11 THEN total_amount ELSE 0 END) AS "Nov",
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 12 THEN total_amount ELSE 0 END) AS "Dec"
FROM orders
GROUP BY store_id
ORDER BY store_id;

-- -- PIVOT EXPLICTLY FOR POSTGRESQL 
-- -- crosstab()

-- CREATE EXTENSION IF NOT EXISTS tablefunc; -- its not enabled by default
-- -- select * from orders

select * from crosstab(
	'select store_id, TRIM(TO_CHAR(order_date, ''Month'')) AS month ,sum(total_amount) from orders 
 	 GROUP BY store_id, month
 	 ORDER BY store_id, month',
	 'VALUES (''January''), (''February''), (''March''), (''April'')'	
) as pivot_table (store_id int, january NUMERIC,febrauary NUMERIC, march NUMERIC,april NUMERIC);

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 9
-- ------------------------------------------------------------------------------------------------------------------
SELECT c.customer_id, c.name, o.order_id, o.order_date, o.total_amount
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
ORDER BY o.order_date DESC;

SELECT c.customer_id, c.name, o.order_id, o.order_date, o.total_amount
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
ORDER BY c.customer_id;

SELECT c.customer_id, c.name, o.order_id, o.order_date, o.total_amount
FROM customers c
RIGHT JOIN orders o ON c.customer_id = o.customer_id
ORDER BY o.order_date DESC;

SELECT c.customer_id, c.name, o.order_id, o.order_date, o.total_amount
FROM customers c
FULL JOIN orders o ON c.customer_id = o.customer_id
ORDER BY c.customer_id NULLS LAST;

SELECT e1.employee_id AS emp_id, e1.name AS employee_name, 
       e2.employee_id AS manager_id, e2.name AS manager_name
FROM employees e1
LEFT JOIN employees e2 ON e1.manager_id = e2.employee_id
ORDER BY e2.employee_id;

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 10
-- ------------------------------------------------------------------------------------------------------------------

SELECT customer_id AS id, name, email, 'Customer' AS type
FROM customers

UNION

SELECT employee_id AS id, name, NULL AS email, 'Employee' AS type
FROM employees

ORDER BY type, name;

SELECT order_id, customer_id, store_id, order_date, total_amount, 'Active' AS status
FROM orders
WHERE order_date >= NOW() - INTERVAL '6 months'

UNION ALL

SELECT order_id, customer_id, store_id, order_date, total_amount, 'Inactive' AS status
FROM orders
WHERE order_date < NOW() - INTERVAL '6 months'

ORDER BY order_date DESC;

TASK 11

UPDATE products
SET price = price * 1.10
WHERE category = 'Electronics';

ALTER TABLE employees ADD COLUMN hire_date DATE DEFAULT NOW() - INTERVAL '6 years';

UPDATE employees
SET salary = salary * 1.15
WHERE hire_date <= NOW() - INTERVAL '5 years';

CREATE TEMP TABLE temp_shipments (
    product_id INT,
    new_stock INT
);

to do cause csv is involved

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 12
-- ------------------------------------------------------------------------------------------------------------------

DELETE FROM customers
WHERE customer_id IN (
    SELECT c.customer_id
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date IS NULL OR o.order_date < NOW() - INTERVAL '2 years'
);

SELECT * FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date IS NULL OR o.order_date < NOW() - INTERVAL '2 years';

ALTER TABLE order_items DROP CONSTRAINT IF EXISTS order_items_order_id_fkey;
ALTER TABLE order_items ADD CONSTRAINT order_items_order_id_fkey
FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

-- ------------------------------------------------------------------------------------------------------------------
-- TASK 13
-- ------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION sp_AddCustomer(
    _name VARCHAR,
    _email VARCHAR,
    _phone VARCHAR,
    _city VARCHAR
) RETURNS VOID AS $$
BEGIN
    INSERT INTO customers (name, email, phone, city)
    VALUES (_name, _email, _phone, _city);
END;
$$ LANGUAGE plpgsql;
-- SELECT sp_AddCustomer('John Doe', 'john@example.com', '1234567890', 'New York');


CREATE OR REPLACE FUNCTION sp_UpdateCustomer(
    _customer_id INT,
    _name VARCHAR,
    _email VARCHAR,
    _phone VARCHAR,
    _city VARCHAR
) RETURNS VOID AS $$
BEGIN
    UPDATE customers
    SET name = _name, email = _email, phone = _phone, city = _city
    WHERE customer_id = _customer_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sp_DeleteCustomer(_customer_id INT) RETURNS VOID AS $$
BEGIN
    -- Check if customer has orders
    IF EXISTS (SELECT 1 FROM orders WHERE customer_id = _customer_id) THEN
        RAISE EXCEPTION 'Cannot delete customer: Active orders exist!';
    ELSE
        DELETE FROM customers WHERE customer_id = _customer_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sp_GetCustomerOrders(_customer_id INT)
RETURNS TABLE(order_id INT, store_id INT, order_date TIMESTAMP, total_amount DECIMAL) AS $$
BEGIN
    RETURN QUERY
    SELECT order_id, store_id, order_date, total_amount
    FROM orders WHERE customer_id = _customer_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sp_AddProductStock(_product_id INT, _quantity INT) RETURNS VOID AS $$
BEGIN
    UPDATE products SET stock = stock + _quantity WHERE product_id = _product_id;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sp_GenerateSalesReport(_month INT, _year INT)
RETURNS TABLE(store_id INT, total_sales DECIMAL) AS $$
BEGIN
    RETURN QUERY
    SELECT store_id, SUM(total_amount) AS total_sales
    FROM orders	
    WHERE EXTRACT(MONTH FROM order_date) = _month AND EXTRACT(YEAR FROM order_date) = _year
    GROUP BY store_id;
END;
$$ LANGUAGE plpgsql;

-- implemeted in cmd line
\copy (
    SELECT c.customer_id, c.name, SUM(o.total_amount) AS total_spending
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.name
    ORDER BY total_spending DESC
) TO 'D:/Certainti.Ai/customer_total_spending.csv' WITH CSV HEADER;

\copy (
	select * from employees
) to 'D:/Certainti.Ai/customer_total_spending.csv' with csv header;

-- done in cmd line
-- -- o/p successfully stored in the csv format in local path('D:/Certainti.Ai/customer_total_spending.csv')
