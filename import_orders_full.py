import pandas as pd
import sqlite3
import os

# === Пути файлов ===
XLSX_FILE = "sells.xlsx"  # Файл должен лежать в папке normalized
CSV_FILE = "orders_normalized.csv"
DB_FILE = "orders_normalized.db"

# 1. Чтение Excel и переименование столбцов
rename_dict = {
    "Номер заказа": "order_id",
    "Дата заказа": "order_date",
    "ФИО Клиента": "client_name",
    "Email клиента": "client_email",
    "Телефон клиента": "client_phone",
    "Название модели": "model_name",
    "Категория обуви": "category",
    "Производитель": "brand",
    "Размер обуви": "size",
    "Цвет": "color",
    "Цена за пару": "price",
    "Кол-во пар": "quantity",
    "ФИО продавца": "seller_name",
    "Должность продавца": "seller_position",
    "Склад отгрузки": "warehouse_name",
    "Адрес склада": "warehouse_address",
    "Вместимость склада": "warehouse_capacity",
    "Количество полок": "warehouse_shelves"
}
df = pd.read_excel(XLSX_FILE)
df = df.rename(columns=rename_dict)
df = df.drop_duplicates(subset=["order_id"])

# Нормализация строк: убрать пробелы, привести к нижнему регистру, заменить кавычки
def normalize_str(val):
    if pd.isnull(val):
        return ''
    s = str(val).strip().lower()
    s = s.replace('“', '"').replace('”', '"').replace("'", '"')
    s = s.replace('«', '"').replace('»', '"')
    s = s.replace('`', '"')
    s = ' '.join(s.split())  # убрать лишние пробелы внутри
    return s

for col in ["model_name", "category", "brand", "size", "color"]:
    if col in df.columns:
        df[col] = df[col].apply(normalize_str)
        
if "order_date" in df.columns:
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")
if "order_date" in df.columns:
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")

# 2. Сохраняем в CSV
os.makedirs(os.path.dirname(CSV_FILE) or ".", exist_ok=True)
df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")

# 3. Удаляем старую БД, если есть
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

# 4. Создаём новую SQLite-базу и таблицы в 3НФ
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute("""
CREATE TABLE clients (
    client_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_name TEXT,
    client_email TEXT,
    client_phone TEXT,
    UNIQUE(client_name, client_email, client_phone)
)
""")
cur.execute("""
CREATE TABLE sellers (
    seller_id INTEGER PRIMARY KEY AUTOINCREMENT,
    seller_name TEXT,
    seller_position TEXT,
    UNIQUE(seller_name, seller_position)
)
""")
cur.execute("""
CREATE TABLE warehouses (
    warehouse_id INTEGER PRIMARY KEY AUTOINCREMENT,
    warehouse_name TEXT,
    warehouse_address TEXT,
    warehouse_capacity INTEGER,
    warehouse_shelves INTEGER,
    UNIQUE(warehouse_name, warehouse_address)
)
""")
cur.execute("""
CREATE TABLE models (
    model_id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name TEXT,
    category TEXT,
    brand TEXT,
    size TEXT,
    color TEXT,
    UNIQUE(model_name, category, brand, size, color)
)
""")
cur.execute("""
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    order_date TEXT,
    client_id INTEGER,
    model_id INTEGER,
    price REAL,
    quantity INTEGER,
    seller_id INTEGER,
    warehouse_id INTEGER,
    FOREIGN KEY(client_id) REFERENCES clients(client_id),
    FOREIGN KEY(model_id) REFERENCES models(model_id),
    FOREIGN KEY(seller_id) REFERENCES sellers(seller_id),
    FOREIGN KEY(warehouse_id) REFERENCES warehouses(warehouse_id)
)
""")

# 5. Импорт данных по нормализованным таблицам
clients = df[["client_name", "client_email", "client_phone"]].drop_duplicates().reset_index(drop=True)
for _, row in clients.iterrows():
    cur.execute("INSERT OR IGNORE INTO clients (client_name, client_email, client_phone) VALUES (?, ?, ?)",
                (row.client_name, row.client_email, row.client_phone))
sellers = df[["seller_name", "seller_position"]].drop_duplicates().reset_index(drop=True)
for _, row in sellers.iterrows():
    cur.execute("INSERT OR IGNORE INTO sellers (seller_name, seller_position) VALUES (?, ?)",
                (row.seller_name, row.seller_position))
warehouses = df[["warehouse_name", "warehouse_address", "warehouse_capacity", "warehouse_shelves"]].drop_duplicates().reset_index(drop=True)
for _, row in warehouses.iterrows():
    cur.execute("INSERT OR IGNORE INTO warehouses (warehouse_name, warehouse_address, warehouse_capacity, warehouse_shelves) VALUES (?, ?, ?, ?)",
                (row.warehouse_name, row.warehouse_address, row.warehouse_capacity, row.warehouse_shelves))

models = df[["model_name", "category", "brand", "size", "color"]].drop_duplicates().reset_index(drop=True)
print("\n=== MODELS FROM DATAFRAME (до вставки в БД) ===")

for _, row in df.iterrows():
    print(f"\n[DEBUG] row при вставке заказа: {row.to_dict()}")
    model_name = normalize_str(row['model_name'])
    category = normalize_str(row['category'])
    brand = normalize_str(row['brand'])
    size_val = normalize_str(row['size'])
    color = normalize_str(row['color'])
    cur.execute(
        "SELECT model_id FROM models WHERE model_name=? AND category=? AND brand=? AND size=? AND color=?",
        (model_name, category, brand, size_val, color)
    )
    model = cur.fetchone()
    if not model:
        print(f"[INFO] Model not found, создаю новую: ({model_name!r}, {category!r}, {brand!r}, {size_val!r}, {color!r})")
        cur.execute(
            "INSERT INTO models (model_name, category, brand, size, color) VALUES (?, ?, ?, ?, ?)",
            (model_name, category, brand, size_val, color)
        )
        model_id = cur.lastrowid
    else:
        model_id = model[0]

    cur.execute("SELECT client_id FROM clients WHERE client_name=? AND client_email=? AND client_phone=?", (row['client_name'], row['client_email'], row['client_phone']))
    client = cur.fetchone()
    if not client:
        print(f"[ERROR] Client not found for row: {row.to_dict()}")
        continue
    client_id = client[0]

    cur.execute("SELECT seller_id FROM sellers WHERE seller_name=? AND seller_position=?", (row['seller_name'], row['seller_position']))
    seller = cur.fetchone()
    if not seller:
        print(f"[ERROR] Seller not found for row: {row.to_dict()}")
        continue
    seller_id = seller[0]

    cur.execute("SELECT warehouse_id FROM warehouses WHERE warehouse_name=? AND warehouse_address=?", (row['warehouse_name'], row['warehouse_address']))
    warehouse = cur.fetchone()
    if not warehouse:
        print(f"[ERROR] Warehouse not found for row: {row.to_dict()}")
        continue
    warehouse_id = warehouse[0]

    cur.execute("INSERT OR IGNORE INTO orders (order_id, order_date, client_id, model_id, price, quantity, seller_id, warehouse_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (row['order_id'], row['order_date'], client_id, model_id, row['price'], row['quantity'], seller_id, warehouse_id))

conn.commit()
conn.close()

print(f"✅ Готово! Создана нормализованная база {DB_FILE}, экспортирован CSV и импортированы все данные.")
