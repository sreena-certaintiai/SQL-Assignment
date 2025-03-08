import psycopg2
import pandas as pd

DB_CONFIG = {
    "host": "localhost",
    "dbname": "shopease",
    "user": "postgres",
    "password": "sreena13",
    "port": "5432"
}

def execute_query(query, fetch=False):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        if fetch:
            return cursor.fetchall()
    except Exception as e:
        print(f"Connection Error: {e}")
    finally:
        cursor.close()
        conn.close()

def create_tables():
    query = """
    CREATE TABLE IF NOT EXISTS stores (
        store_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        location VARCHAR(255) NOT NULL,
        manager_id INT NULL
    );

    CREATE TABLE IF NOT EXISTS employees (
        employee_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        role VARCHAR(100) NOT NULL,
        store_id INT REFERENCES stores(store_id) ON DELETE CASCADE,
        salary DECIMAL(10,2) CHECK (salary > 0),
        manager_id INT NULL,
        FOREIGN KEY (manager_id) REFERENCES employees(employee_id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL CHECK (email LIKE '%@%.%'),
        phone VARCHAR(15) UNIQUE NOT NULL,
        city VARCHAR(100) NOT NULL
    );
    """
    execute_query(query)
    print("Tables created successfully!")

def create_indexes():
    query = """
    CREATE INDEX IF NOT EXISTS idx_product_name ON products(name);
    CREATE INDEX IF NOT EXISTS idx_customer_order ON orders(customer_id, order_date);
    """
    execute_query(query)
    print("Indexes created!")

def create_views():
    query = """
    CREATE OR REPLACE VIEW top_selling_products AS 
    SELECT oi.product_id, p.name AS product_name, SUM(oi.quantity) AS total_sold
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY oi.product_id, p.name
    ORDER BY total_sold DESC;

    CREATE OR REPLACE VIEW store_revenue AS 
    SELECT o.store_id, s.name AS store_name, SUM(o.total_amount) AS total_revenue
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    GROUP BY o.store_id, s.name
    ORDER BY total_revenue DESC;
    """
    execute_query(query)
    print("Views created!")

def create_triggers():
    query = """
    CREATE OR REPLACE FUNCTION prevent_out_of_stock() RETURNS TRIGGER AS $$
    BEGIN
        IF (SELECT stock FROM products WHERE product_id = NEW.product_id) < NEW.quantity THEN
            RAISE EXCEPTION 'Cannot place order: Product is out of stock!';
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER trg_prevent_out_of_stock
    BEFORE INSERT ON order_items
    FOR EACH ROW
    EXECUTE FUNCTION prevent_out_of_stock();
    """
    execute_query(query)
    print("Triggers created!")

def insert_sample_data():
    query = """
    INSERT INTO stores (name, location, manager_id) VALUES 
    ('ShopEase - New York', 'New York, USA', 1),
    ('ShopEase - Los Angeles', 'Los Angeles, USA', 2)
    ON CONFLICT (name) DO NOTHING;

    INSERT INTO employees (name, role, store_id, salary, manager_id) VALUES 
    ('Alice Johnson', 'Manager', 1, 70000, NULL),
    ('Bob Smith', 'Cashier', 1, 35000, 1)
    ON CONFLICT (name) DO NOTHING;
    """
    execute_query(query)
    print("Sample data inserted!")

def recursive_query():
    query = """
    WITH RECURSIVE employee_hierarchy AS (
        SELECT employee_id, name, role, manager_id, 1 AS level
        FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.employee_id, e.name, e.role, e.manager_id, eh.level + 1 AS level
        FROM employees e
        INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
    )
    SELECT * FROM employee_hierarchy ORDER BY level, manager_id;
    """
    results = execute_query(query, fetch=True)
    print("Employee Hierarchy:")
    for row in results:
        print(row)

def import_csv(file_path, table_name):
    df = pd.read_csv(file_path)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        query = f"INSERT INTO {table_name} VALUES {tuple(row)} ON CONFLICT DO NOTHING;"
        cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data imported into {table_name}!")

def export_to_csv(query, output_file):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn)
    df.to_csv(output_file, index=False)
    conn.close()
    print(f"Data exported to {output_file}")

if __name__ == "__main__":
    while True:
        print("\nChoose an option:")
        print("1. Create Tables")
        print("2. Create Indexes")
        print("3. Create Views")
        print("4. Create Triggers")
        print("5. Insert Sample Data")
        print("6. Run Recursive Query")
        print("7. Import CSV Data")
        print("8. Export Data to CSV")
        print("9. Exit")

        choice = input("\nEnter your choice (1-9): ").strip()

        if choice == "1":
            create_tables()
        elif choice == "2":
            create_indexes()
        elif choice == "3":
            create_views()
        elif choice == "4":
            create_triggers()
        elif choice == "5":
            insert_sample_data()
        elif choice == "6":
            recursive_query()
        elif choice == "7":
            file_path = input("Enter CSV file path: ")
            table_name = input("Enter table name: ")
            import_csv(file_path, table_name)
        elif choice == "8":
            query = input("Enter SQL SELECT query: ")
            output_file = input("Enter output CSV file name: ")
            export_to_csv(query, output_file)
        elif choice == "9":
            print("\nExiting...")
            break
        else:
            print("\nInvalid choice! Try again.")
