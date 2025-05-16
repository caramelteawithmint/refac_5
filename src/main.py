import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('example.db')
cursor = conn.cursor()

# Создание таблиц
def create_tables():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            email TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product TEXT,
            quantity INTEGER,
            price REAL,
            order_date TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            price REAL
        )
    ''')
    conn.commit()


def insert_users():
    cursor.execute("INSERT INTO users (name, age, email) VALUES ('Alice', 30, 'alice@example.com')")
    cursor.execute("INSERT INTO users (name, age, email) VALUES ('Bob', 25, 'bob@example.com')")
    cursor.execute("INSERT INTO users (name, age, email) VALUES ('Charlie', 35, 'charlie@example.com')")
    cursor.execute("INSERT INTO users (name, age, email) VALUES ('David', 40, 'david@example.com')")
    conn.commit()

def insert_orders():
    cursor.execute("INSERT INTO orders (user_id, product, quantity, price, order_date) VALUES (1, 'Laptop', 1, 1000.0, '2023-01-10')")
    cursor.execute("INSERT INTO orders (user_id, product, quantity, price, order_date) VALUES (2, 'Phone', 2, 500.0, '2023-02-15')")
    cursor.execute("INSERT INTO orders (user_id, product, quantity, price, order_date) VALUES (3, 'Tablet', 1, 300.0, '2023-03-20')")
    cursor.execute("INSERT INTO orders (user_id, product, quantity, price, order_date) VALUES (4, 'Monitor', 2, 200.0, '2023-04-25')")
    conn.commit()

def insert_products():
    cursor.execute("INSERT INTO products (name, category, price) VALUES ('Laptop', 'Electronics', 1000.0)")
    cursor.execute("INSERT INTO products (name, category, price) VALUES ('Phone', 'Electronics', 500.0)")
    cursor.execute("INSERT INTO products (name, category, price) VALUES ('Tablet', 'Electronics', 300.0)")
    cursor.execute("INSERT INTO products (name, category, price) VALUES ('Monitor', 'Electronics', 200.0)")
    conn.commit()


def get_all_users():
    
    query = "SELECT * FROM users"
    cursor.execute(query)
    return cursor.fetchall()

def get_user_by_id(user_id):
    
    query = "SELECT * FROM users WHERE id = {}".format(user_id)
    cursor.execute(query)
    return cursor.fetchone()

def get_orders_for_user(user_id):
    
    query = "SELECT * FROM orders WHERE user_id = {}".format(user_id)
    cursor.execute(query)
    return cursor.fetchall()

def get_products_by_category(category):
    
    query = "SELECT * FROM products WHERE category = '{}'".format(category)
    cursor.execute(query)
    return cursor.fetchall()


def get_total_order_value_for_user(user_id):
    
    orders = get_orders_for_user(user_id)
    total = 0
    for order in orders:
        total += order[4] * order[3]  # цена * количество
        
        print(f"Order ID: {order[0]}, Product: {order[2]}, Total: {order[4] * order[3]}")
    
    return total

def get_users_with_orders():
    
    users = get_all_users()
    result = []
    
    for user in users:
        user_orders = get_orders_for_user(user[0])
        result.append({
            'user': user,
            'orders': user_orders
        })
    
    return result


def close_connection():
    try:
        conn.close()
        print("Connection closed")
    except Exception as e:
        print(f"Error closing connection: {e}")


def main():
    
    create_tables()
    
    insert_users()
    
    insert_products()
    
    
    insert_orders()
    insert_orders()
    insert_orders()
     
     
    users = get_all_users()
    for user in users:
         print(f"User: {user}")
         orders = get_orders_for_user(user[0])
         for order in orders:
             print(f"Order: {order}")
         
         total_value = get_total_order_value_for_user(user[0])
         print(f"Total order value for user {user[1]}: {total_value}")
         
    
    electronics_products = get_products_by_category('Electronics')
    print(f"Electronics products: {electronics_products}")
     
    
    users_with_orders = get_users_with_orders()
    for item in users_with_orders:
         print(f"User: {item['user'][1]}")
         for order in item['orders']:
             print(f"\tOrder ID: {order[0]}, Product: {order[2]}")
             
    close_connection()

if __name__ == "__main__":
   main()