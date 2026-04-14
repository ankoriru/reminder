def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Таблица Дней Рождения
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS birthdays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT,
            position TEXT,
            department TEXT,
            birth_date TEXT
        )
    ''')
    
    # 2. Таблица Значимых Событий
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT,
            reminder_text TEXT,
            event_datetime TEXT
        )
    ''')

    # 3. Таблица CUSTOM (из твоего пункта 3)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS custom_reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT,
            run_datetime TEXT,       -- Дата и время запуска
            periodicity TEXT,        -- 'once', 'daily', 'weekly', 'monthly', 'yearly'
            last_run TEXT            -- Чтобы не отправлять дважды в одну минуту
        )
    ''')
    
    conn.commit()
    conn.close()

init_db()
