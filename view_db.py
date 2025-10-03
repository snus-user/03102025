import sqlite3

DB_FILE = "orders_normalized.db"

tables = [
    ("clients", "Клиенты"),
    ("models", "Модели обуви"),
    ("sellers", "Продавцы"),
    ("warehouses", "Склады"),
    ("orders", "Заказы")
]

def print_table(table_name):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cur.fetchall()]
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()
    print(f"\nТаблица: {table_name}")
    print(" | ".join(columns))
    print("-" * 80)
    for row in rows:
        print(" | ".join(str(x) for x in row))
    print()
    conn.close()

if __name__ == "__main__":
    print("Выберите таблицу для просмотра:")
    for i, (_, rus) in enumerate(tables, 1):
        print(f"{i}. {rus}")
    choice = input("Введите номер таблицы: ")
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(tables):
            print_table(tables[idx][0])
        else:
            print("Некорректный номер!")
    except ValueError:
        print("Введите число!")
