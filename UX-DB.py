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
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    
    if fetch:
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results

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
    print("Tables created!")

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

def insert_data():
    table_name = input("Enter the table name to insert into: ").strip()
    
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
    columns = [col[0] for col in execute_query(query, fetch=True)]
    
    if not columns:
        print("Table does not exist!")
        return

    print(f"Columns in {table_name}: {', '.join(columns)}")
    values = [input(f"Enter value for {col}: ").strip() for col in columns]
    
    values_str = ', '.join(f"'{v}'" if v.lower() != "null" else "NULL" for v in values)
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_str});"
    
    execute_query(query)
    print(f"Data inserted into {table_name}!")

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
    for row in results:
        print(row)

def export_to_csv():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    table_name = input("Enter table name: ").strip()

    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
    columns = [col[0] for col in execute_query(query, fetch=True)]

    print(f"Columns in {table_name}: {', '.join(columns)}")
    selected_columns = input("Enter columns to export (comma-separated or * for all): ").strip()

    query = f"SELECT {selected_columns} FROM {table_name};"

    output_file = input("Enter output CSV file name: ").strip()
    df = pd.read_sql(query, conn)
    df.to_csv(output_file, index=False)

    conn.close()
    print(f"Data exported to {output_file}!")

if __name__ == "__main__":
    while True:
        print("\n1. Create Tables\n2. Create Indexes\n3. Create Views\n4. Create Triggers\n5. Insert Data\n6. Recursive Query\n7. Export Data to CSV\n8. Exit")
        choice = input("Enter your choice: ").strip()

        if choice == "1": create_tables()
        elif choice == "2": create_indexes()
        elif choice == "3": create_views()
        elif choice == "4": create_triggers()
        elif choice == "5": insert_data()
        elif choice == "6": recursive_query()
        elif choice == "7": export_to_csv()
        elif choice == "8": break
