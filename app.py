import sqlite3
import os

# Указываем путь к папке, которую мы создали в Шаге 1
DB_PATH = '/data/bot_database.db'

def get_db_connection():
    # Проверяем, существует ли папка (на всякий случай для локальных тестов)
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir) and db_dir != '':
        os.makedirs(db_dir)
        
    conn = sqlite3.connect(DB_PATH)
    # Позволяет обращаться к полям по именам, как в словаре
    conn.row_factory = sqlite3.Row 
    return conn

