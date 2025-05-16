import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('example.db')
cursor = conn.cursor()

# Создание таблиц с индексами для ускорения поиска
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
        CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product TEXT,
            quantity INTEGER,
            price REAL,
            order_date TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id)
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            price REAL
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_products_category ON products (category)
    ''')
    
    conn.commit()

# Функция для выполнения EXPLAIN QUERY PLAN и вывода плана
def explain_query(query, params=()):
    print(f"\nЭкспликация плана выполнения для запроса: {query}")
    plan = cursor.execute(f"EXPLAIN QUERY PLAN {query}", params).fetchall()
    for row in plan:
        print(row)

# Вставка данных с использованием параметризированных запросов для безопасности и эффективности
def insert_users():
    users = [
        ('Alice', 30, 'alice@example.com'),
        ('Bob', 25, 'bob@example.com'),
        ('Charlie', 35, 'charlie@example.com'),
        ('David', 40, 'david@example.com')
    ]
    cursor.executemany("INSERT INTO users (name, age, email) VALUES (?, ?, ?)", users)
    conn.commit()

def insert_products():
    products = [
        ('Laptop', 'Electronics', 1000.0),
        ('Phone', 'Electronics', 500.0),
        ('Tablet', 'Electronics', 300.0),
        ('Monitor', 'Electronics', 200.0)
    ]
    cursor.executemany("INSERT INTO products (name, category, price) VALUES (?, ?, ?)", products)
    conn.commit()

def insert_orders():
    orders = [
        (1, 'Laptop', 1, 1000.0, '2023-01-10'),
        (2, 'Phone', 2, 500.0, '2023-02-15'),
        (3, 'Tablet', 1, 300.0, '2023-03-20'),
        (4, 'Monitor', 2, 200.0, '2023-04-25')
    ]
    cursor.executemany("INSERT INTO orders (user_id, product, quantity, price, order_date) VALUES (?, ?, ?, ?, ?)", orders)
    conn.commit()

# Оптимизированные функции с параметризацией и использованием JOIN

def get_all_users():
    query = "SELECT * FROM users"
    explain_query(query)
    return cursor.execute(query).fetchall()

def get_user_by_id(user_id):
    query = "SELECT * FROM users WHERE id = ?"
    explain_query(query, (user_id,))
    return cursor.execute(query, (user_id,)).fetchone()

def get_orders_for_user(user_id):
    query = "SELECT * FROM orders WHERE user_id = ?"
    explain_query(query, (user_id,))
    return cursor.execute(query, (user_id,)).fetchall()

def get_products_by_category(category):
    query = "SELECT * FROM products WHERE category = ?"
    explain_query(query, (category,))
    return cursor.execute(query, (category,)).fetchall()

# Использование агрегатных функций для подсчета общей стоимости заказов
def get_total_order_value_for_user(user_id):
    query = "SELECT SUM(quantity * price) FROM orders WHERE user_id = ?"
    explain_query(query, (user_id,))
    total = cursor.execute(query, (user_id,)).fetchone()[0]
    
    # Обработка None в случае отсутствия заказов
    return total if total is not None else 0

# Получение пользователей с их заказами через JOIN — более эффективно
def get_users_with_orders():
    query = '''
        SELECT u.id, u.name, u.age, u.email,
               o.id as order_id, o.product, o.quantity, o.price, o.order_date
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        ORDER BY u.id
    '''
    
    explain_query(query)
    
    results = cursor.execute(query).fetchall()
    
    # Формируем структуру данных по пользователю с его заказами
    users_dict = {}
    
    for row in results:
        user_id = row[0]
        if user_id not in users_dict:
            users_dict[user_id] = {
                'user': row[:4],
                'orders': []
            }
        
        if row[4] is not None:  # Есть заказы
            order_info = {
                'order_id': row[4],
                'product': row[5],
                'quantity': row[6],
                'price': row[7],
                'order_date': row[8]
            }
            users_dict[user_id]['orders'].append(order_info)
    
    return list(users_dict.values())

# Закрытие соединения — рекомендуется делать в блоке finally или через контекстный менеджер
def close_connection():
     try:
         conn.close()
         print("Connection closed")
     except Exception as e:
         print(f"Error closing connection: {e}")

# Основная логика программы с минимизацией повторений и улучшенной структурой
def main():
    
     create_tables()
    
     insert_users()
    
     insert_products()
    
     # Вставка заказов один раз — избегайте дублирования вызовов без необходимости
     insert_orders()
     
     # Получение и вывод данных с использованием оптимизированных функций и анализа планов выполнения
     users = get_all_users()
     for user in users:
         print(f"User: {user}")
         
         total_value = get_total_order_value_for_user(user[0])
         print(f"Total order value for {user[1]}: {total_value}")
         
         # Получение заказов пользователя через JOIN — более эффективно
         user_orders = get_orders_for_user(user[0])
         for order in user_orders:
             print(f"\tOrder ID: {order[0]}, Product: {order[2]}, Quantity: {order[3]}, Price: {order[4]}")
         
     # Получение продуктов по категории — безопасно благодаря параметризации
     electronics_products = get_products_by_category('Electronics')
     print(f"Electronics products: {electronics_products}")
     
     # Получение пользователей с их заказами через JOIN — более эффективно и читаемо
     users_with_orders = get_users_with_orders()
     for item in users_with_orders:
         user_info = item['user']
         print(f"User: {user_info[1]} ({user_info[2]} years old)")
         for order in item['orders']:
             print(f"\tOrder ID: {order['order_id']}, Product: {order['product']}")
             
     close_connection()

if __name__ == "__main__":
   main() 