import psycopg2
import pandas as pd

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="shopease",
        user="postgres",
        password="sreena13",
        port="5432"
    )

def execute_query(query, params=None):
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                cur.execute(query, params or ())
                con.commit()
                print("Query executed successfully.")
    except Exception as e:
        print(f"Error: {e}")

def fetch_query(query, params=None):
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                cur.execute(query, params or ())
                return cur.fetchall()
    except Exception as e:
        print(f"Error: {e}")
        return []

def create_tables():
    queries = [
        """
        CREATE TABLE IF NOT EXISTS stores (
            store_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            location VARCHAR(255) NOT NULL,
            manager_id INT NULL
        )""",
        """
        CREATE TABLE IF NOT EXISTS employees (
            employee_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            role VARCHAR(100) NOT NULL,
            store_id INT REFERENCES stores(store_id) ON DELETE CASCADE,
            salary DECIMAL(10,2) CHECK (salary > 0),
            manager_id INT NULL,
            FOREIGN KEY (manager_id) REFERENCES employees(employee_id) ON DELETE SET NULL
        )""",
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL CHECK (email LIKE '%@%.%'),
            phone VARCHAR(15) UNIQUE NOT NULL,
            city VARCHAR(100) NOT NULL
        )""",
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id SERIAL PRIMARY KEY,
            customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
            store_id INT REFERENCES stores(store_id) ON DELETE CASCADE,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_amount DECIMAL(10,2) CHECK (total_amount >= 0)
        )"""
    ]
    for query in queries:
        execute_query(query)

def create_indexes():
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_product_name ON products(name);",
        "CREATE INDEX IF NOT EXISTS idx_customer_order ON orders(customer_id, order_date);"
    ]
    for query in indexes:
        execute_query(query)

def create_views():
    views = [
        """
        CREATE OR REPLACE VIEW top_selling_products AS 
        SELECT oi.product_id, p.name AS product_name, SUM(oi.quantity) AS total_sold
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY oi.product_id, p.name
        ORDER BY total_sold DESC
        """,
        """
        CREATE OR REPLACE VIEW store_revenue AS 
        SELECT o.store_id, s.name AS store_name, SUM(o.total_amount) AS total_revenue
        FROM orders o
        JOIN stores s ON o.store_id = s.store_id
        GROUP BY o.store_id, s.name
        ORDER BY total_revenue DESC
        """
    ]
    for query in views:
        execute_query(query)

def create_triggers_procedures():
    queries = [
        """
        CREATE OR REPLACE FUNCTION prevent_out_of_stock() RETURNS TRIGGER AS $$
        BEGIN
            IF (SELECT stock FROM products WHERE product_id = NEW.product_id) < NEW.quantity THEN
                RAISE EXCEPTION 'Cannot place order: Product is out of stock!';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
        """
        CREATE OR REPLACE TRIGGER trg_prevent_out_of_stock
        BEFORE INSERT ON order_items
        FOR EACH ROW
        EXECUTE FUNCTION prevent_out_of_stock();
        """,
        """
        CREATE OR REPLACE FUNCTION sp_AddCustomer(_name VARCHAR, _email VARCHAR, _phone VARCHAR, _city VARCHAR) RETURNS VOID AS $$
        BEGIN
            INSERT INTO customers (name, email, phone, city) VALUES (_name, _email, _phone, _city);
        END;
        $$ LANGUAGE plpgsql;
        """
    ]
    for query in queries:
        execute_query(query)

def export_to_file(query, filename):
    try:
        with get_connection() as con:
            df = pd.read_sql(query, con)
            csv_filename = filename + ".csv"
            df.to_csv(csv_filename, index=False)
            xlsx_filename = filename + ".xlsx"
            df.to_excel(xlsx_filename, index=False, engine="openpyxl")
    except Exception as e:
        print(f"Error exporting data: {e}")

def export_monthly_revenue():
    query = """
        SELECT store_id, 
               TO_CHAR(order_date, 'YYYY-MM') AS month, 
               SUM(total_amount) AS total_revenue
        FROM orders
        GROUP BY store_id, month
        ORDER BY store_id, month;
    """
    export_to_file(query, "monthly_revenue_per_store")

def export_customer_spending():
    query = """
        SELECT c.customer_id, c.name, SUM(o.total_amount) AS total_spending
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.name
        ORDER BY total_spending DESC;
    """
    export_to_file(query, "customer_total_spending")

def initialize_database():
    create_tables()
    create_indexes()
    create_views()
    create_triggers_procedures()
    print("Database initialized successfully.")

if __name__ == "__main__":
    initialize_database()
    export_monthly_revenue()
    export_customer_spending()