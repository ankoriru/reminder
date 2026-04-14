import os
import sqlite3
import pandas as pd
import io
import asyncio
import threading
from datetime import datetime, timedelta
import pytz

from flask import Flask, render_template, request, redirect, session, flash, url_for, send_file
from aiogram import Bot
from apscheduler.schedulers.background import BackgroundScheduler

# --- КОНФИГУРАЦИЯ ---
# Пароль прописан в коде (как требовалось)
ADMIN_PASSWORD = "admin123"  # Измените на свой пароль

# Токен и Chat ID можно задать здесь или через переменные окружения
TOKEN = os.getenv('BOT_TOKEN', '')  # Замените '' на ваш токен, например: '123456:ABC-DEF...'
CHAT_ID = os.getenv('CHAT_ID', '')  # Замените '' на ID чата, например: '-1001234567890'

DB_PATH = '/data/bot_database.db'
MSK = pytz.timezone('Europe/Moscow')

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Инициализация бота (если токен не задан - бот не инициализируется)
bot = None
if TOKEN:
    bot = Bot(token=TOKEN)

# --- РЕШЕНИЕ ОШИБКИ EVENT LOOP CLOSED ---
bot_loop = asyncio.new_event_loop()

def start_bot_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_bot_loop, args=(bot_loop,), daemon=True).start()

def send_msg_threadsafe(text):
    """Безопасная отправка сообщения из любого потока в цикл бота"""
    if bot and CHAT_ID:
        try:
            asyncio.run_coroutine_threadsafe(bot.send_message(CHAT_ID, text), bot_loop)
        except Exception as e:
            print(f"Ошибка отправки сообщения: {e}")
    else:
        print(f"[BOT NOT CONFIGURED] Message: {text}")

# --- БАЗА ДАННЫХ И МИГРАЦИИ ---
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
        # Таблица дней рождения
        conn.execute('''CREATE TABLE IF NOT EXISTS birthdays 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, full_name TEXT, pos TEXT, dep TEXT, bday TEXT)''')
        
        # Таблица значимых событий
        conn.execute('''CREATE TABLE IF NOT EXISTS events 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, event_name TEXT, reminder_text TEXT, dt TEXT, is_sent INTEGER DEFAULT 0)''')
        
        # Таблица кастомных задач
        conn.execute('''CREATE TABLE IF NOT EXISTS custom_tasks 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, dt TEXT, period TEXT, weekdays TEXT, last_sent TEXT)''')
        
        # Миграции (проверка структуры)
        cursor = conn.execute("PRAGMA table_info(events)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'is_sent' not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN is_sent INTEGER DEFAULT 0")
        
        cursor_c = conn.execute("PRAGMA table_info(custom_tasks)")
        cols_c = [row[1] for row in cursor_c.fetchall()]
        if 'weekdays' not in cols_c:
            conn.execute("ALTER TABLE custom_tasks ADD COLUMN weekdays TEXT")
        if 'last_sent' not in cols_c:
            conn.execute("ALTER TABLE custom_tasks ADD COLUMN last_sent TEXT")
        
        conn.commit()

# --- ЛОГИКА ПЛАНИРОВЩИКА ---
def check_and_send():
    """Проверка и отправка уведомлений"""
    now = datetime.now(MSK)
    now_dm = now.strftime("%d.%m")  # Текущий день и месяц для ДР
    current_weekday = now.weekday()  # 0=Пн, 6=Вс
    
    conn = get_db_connection()
    try:
        # 1. ДНИ РОЖДЕНИЯ (09:00:00 - 09:00:20)
        if now.hour == 9 and now.minute == 0 and 0 <= now.second <= 20:
            celebrants = conn.execute("SELECT * FROM birthdays").fetchall()
            birthday_people = []
            
            for person in celebrants:
                bday_str = str(person['bday']).strip()
                # Проверяем совпадение ДД.ММ
                if bday_str.startswith(now_dm):
                    birthday_people.append(person)
            
            if birthday_people:
                # Формируем сообщение согласно ТЗ
                msg_lines = ["🎉🫶🏼 Сегодня день рождения наших коллег:"]
                for person in birthday_people:
                    msg_lines.append(f"• {person['full_name']}, {person['pos']}, {person['dep']}")
                msg_lines.append("Поздравляем 😊🎊")
                msg = "\n".join(msg_lines)
                send_msg_threadsafe(msg)
        
        # 2. ЗНАЧИМЫЕ СОБЫТИЯ (ЗС) - Точное время
        events = conn.execute("SELECT * FROM events WHERE is_sent = 0").fetchall()
        for event in events:
            try:
                event_dt = datetime.strptime(event['dt'], "%d.%m.%Y %H:%M:%S").replace(tzinfo=MSK)
                if event_dt <= now:
                    msg = f"💡 {event['reminder_text']}"
                    send_msg_threadsafe(msg)
                    conn.execute("UPDATE events SET is_sent = 1 WHERE id = ?", (event['id'],))
                    conn.commit()
            except Exception as ex:
                print(f"Ошибка обработки события {event['id']}: {ex}")
        
        # 3. CUSTOM ЗАДАЧИ
        now_custom_dt = now.strftime("d.%m.%Y %H:%M")  # Для разовых задач
        now_time_hm = now.strftime("%H:%M")  # Текущее время ЧЧ:ММ
        now_date = now.strftime("%d.%m.%Y")  # Текущая дата
        
        custom_tasks = conn.execute("SELECT * FROM custom_tasks").fetchall()
        for task in custom_tasks:
            try:
                task_dt_str = str(task['dt']).strip()
                task_time = task_dt_str.split(' ')[1] if ' ' in task_dt_str else ""
                period = task['period']
                weekdays_str = task['weekdays'] or ""
                last_sent = task['last_sent']
                
                should_send = False
                
                # Проверяем, не отправляли ли уже в эту минуту
                current_minute = now.strftime("%d.%m.%Y %H:%M")
                if last_sent == current_minute:
                    continue
                
                if period == 'once':
                    # Разовая задача - проверяем точное совпадение даты и времени
                    if task_dt_str == now_custom_dt:
                        should_send = True
                
                elif period == 'daily':
                    # Каждый день в указанное время
                    if task_time == now_time_hm:
                        should_send = True
                
                elif period == 'workdays':
                    # Рабочие дни (Пн-Пт) в указанное время
                    if current_weekday < 5 and task_time == now_time_hm:  # 0-4 = Пн-Пт
                        should_send = True
                
                elif period == 'weekdays':
                    # Выбранные дни недели
                    selected_days = weekdays_str.split(',') if weekdays_str else []
                    if str(current_weekday) in selected_days and task_time == now_time_hm:
                        should_send = True
                
                elif period == 'weekly':
                    # Каждую неделю - проверяем день недели и время
                    # День недели берём из начальной даты задачи
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    if task_start.weekday() == current_weekday and task_time == now_time_hm:
                        should_send = True
                
                elif period == 'monthly':
                    # Каждый месяц - проверяем день месяца и время
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    if task_start.day == now.day and task_time == now_time_hm:
                        should_send = True
                
                elif period == 'yearly':
                    # Каждый год - проверяем день и месяц
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    task_dm = task_start.strftime("%d.%m")
                    now_dm_check = now.strftime("%d.%m")
                    if task_dm == now_dm_check and task_time == now_time_hm:
                        should_send = True
                
                if should_send:
                    send_msg_threadsafe(task['text'])
                    conn.execute("UPDATE custom_tasks SET last_sent = ? WHERE id = ?", (current_minute, task['id']))
                    conn.commit()
                    
                    # Для разовых задач удаляем после отправки
                    if period == 'once':
                        conn.execute("DELETE FROM custom_tasks WHERE id = ?", (task['id'],))
                        conn.commit()
            
            except Exception as ex:
                print(f"Ошибка обработки custom задачи {task['id']}: {ex}")
    
    finally:
        conn.close()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def normalize_bday_date(val):
    """Нормализация даты дня рождения к формату ДД.ММ"""
    if pd.isna(val):
        return ""
    try:
        val_str = str(val).strip()
        
        # Если уже в формате ДД.ММ
        if len(val_str) == 5 and val_str[2] == '.':
            return val_str
        
        # Пробуем разные форматы
        formats = ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m"]
        
        for fmt in formats:
            try:
                dt_obj = datetime.strptime(val_str, fmt)
                return dt_obj.strftime("%d.%m")
            except:
                continue
        
        # Если ничего не подошло, возвращаем как есть
        return val_str
    except:
        return str(val).strip()

def normalize_event_datetime(val):
    """Нормализация даты и времени события к формату ДД.ММ.ГГ ЧЧ:ММ:СС"""
    if pd.isna(val):
        return ""
    try:
        if isinstance(val, datetime):
            return val.strftime("%d.%m.%Y %H:%M:%S")
        
        val_str = str(val).strip()
        
        # Пробуем разные форматы
        formats = [
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d.%m.%y %H:%M",
            "%d.%m.%y %H:%M:%S"
        ]
        
        for fmt in formats:
            try:
                dt_obj = datetime.strptime(val_str, fmt)
                return dt_obj.strftime("%d.%m.%Y %H:%M:%S")
            except:
                continue
        
        return val_str
    except:
        return str(val).strip()

def read_data_file(file):
    """Чтение данных из XLSX или CSV файла"""
    filename = file.filename.lower()
    
    try:
        if filename.endswith('.csv'):
            # Пробуем разные кодировки
            for encoding in ['utf-8', 'cp1251', 'latin1']:
                try:
                    file.seek(0)
                    df = pd.read_csv(file, encoding=encoding)
                    break
                except:
                    continue
            else:
                raise ValueError("Не удалось прочитать CSV файл")
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, engine='openpyxl')
        else:
            raise ValueError("Неподдерживаемый формат файла. Используйте XLSX или CSV")
        
        # Удаляем пустые строки
        df = df.dropna(how='all')
        return df
    except Exception as e:
        raise e

def get_period_display(period, weekdays=None):
    """Получить отображаемое название периода"""
    period_names = {
        'once': 'Один раз',
        'daily': 'Каждый день',
        'workdays': 'Рабочие дни (Пн-Пт)',
        'weekdays': 'Выбранные дни',
        'weekly': 'Каждую неделю',
        'monthly': 'Каждый месяц',
        'yearly': 'Каждый год'
    }
    return period_names.get(period, period)

# --- WEB ROUTES ---
@app.route('/test_send/<type>')
def test_send(type):
    """Тестовая отправка сообщения"""
    test_msg = f"🛠 Тест связи ({type}): Бот работает стабильно!"
    send_msg_threadsafe(test_msg)
    flash(f"Тестовое сообщение ({type}) отправлено в Telegram!")
    return redirect(url_for('index'))

@app.route('/')
def index():
    """Главная страница"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    conn = get_db_connection()
    try:
        bdays = conn.execute("SELECT * FROM birthdays ORDER BY full_name").fetchall()
        events = conn.execute("SELECT * FROM events ORDER BY is_sent ASC, dt ASC").fetchall()
        customs = conn.execute("SELECT * FROM custom_tasks ORDER BY dt ASC").fetchall()
    finally:
        conn.close()
    
    return render_template('index.html', bdays=bdays, evs=events, customs=customs, get_period_display=get_period_display)

@app.route('/upload_dr', methods=['POST'])
def upload_dr():
    """Загрузка списка дней рождения"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    file = request.files.get('file')
    if not file:
        flash("Файл не выбран!")
        return redirect(url_for('index'))
    
    try:
        df = read_data_file(file)
        
        if len(df.columns) < 4:
            flash("Ошибка: файл должен содержать минимум 4 столбца (ФИО, Должность, Подразделение, Дата)")
            return redirect(url_for('index'))
        
        conn = get_db_connection()
        try:
            # Очищаем таблицу
            conn.execute("DELETE FROM birthdays")
            
            # Загружаем новые данные
            for _, row in df.iterrows():
                full_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                pos = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
                dep = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ""
                bday = normalize_bday_date(row.iloc[3])
                
                if full_name:  # Записываем только если есть ФИО
                    conn.execute(
                        "INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)",
                        (full_name, pos, dep, bday)
                    )
            
            conn.commit()
            flash(f"✅ Список дней рождения обновлен! Загружено записей: {len(df)}")
        finally:
            conn.close()
    
    except Exception as e:
        flash(f"❌ Ошибка загрузки файла: {str(e)}")
    
    return redirect(url_for('index'))

@app.route('/upload_zs', methods=['POST'])
def upload_zs():
    """Загрузка списка значимых событий"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    file = request.files.get('file')
    if not file:
        flash("Файл не выбран!")
        return redirect(url_for('index'))
    
    try:
        df = read_data_file(file)
        
        if len(df.columns) < 3:
            flash("Ошибка: файл должен содержать минимум 3 столбца (Событие, Напоминание, Дата и время)")
            return redirect(url_for('index'))
        
        conn = get_db_connection()
        try:
            # Очищаем таблицу
            conn.execute("DELETE FROM events")
            
            # Загружаем новые данные
            for _, row in df.iterrows():
                event_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                reminder_text = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
                dt = normalize_event_datetime(row.iloc[2])
                
                if event_name and dt:  # Записываем только если есть название и дата
                    conn.execute(
                        "INSERT INTO events (event_name, reminder_text, dt, is_sent) VALUES (?,?,?,0)",
                        (event_name, reminder_text, dt)
                    )
            
            conn.commit()
            flash(f"✅ Список значимых событий обновлен! Загружено записей: {len(df)}")
        finally:
            conn.close()
    
    except Exception as e:
        flash(f"❌ Ошибка загрузки файла: {str(e)}")
    
    return redirect(url_for('index'))

@app.route('/add_custom', methods=['POST'])
def add_custom():
    """Добавление кастомной задачи"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    try:
        text = request.form.get('text', '').strip()
        dt_raw = request.form.get('dt', '')
        period = request.form.get('period', 'once')
        days = request.form.getlist('days')
        
        if not text:
            flash("Текст сообщения не может быть пустым!")
            return redirect(url_for('index'))
        
        if not dt_raw:
            flash("Дата и время не указаны!")
            return redirect(url_for('index'))
        
        # Конвертируем дату из формата HTML datetime-local
        dt_final = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M').strftime('%d.%m.%Y %H:%M')
        
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO custom_tasks (text, dt, period, weekdays, last_sent) VALUES (?,?,?,?,?)",
                (text, dt_final, period, ",".join(days) if days else "", None)
            )
            conn.commit()
            flash("✅ Задача успешно добавлена!")
        finally:
            conn.close()
    
    except Exception as e:
        flash(f"❌ Ошибка добавления задачи: {str(e)}")
    
    return redirect(url_for('index'))

@app.route('/delete_custom/<int:id>')
def delete_custom(id):
    """Удаление кастомной задачи"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM custom_tasks WHERE id = ?", (id,))
        conn.commit()
        flash("Задача удалена!")
    finally:
        conn.close()
    
    return redirect(url_for('index'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Страница входа"""
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
            return '''<html><body style="text-align:center;padding-top:100px;">
                <h2>Вход</h2>
                <p style="color:red;">Неверный пароль!</p>
                <form method="post"><input type="password" name="password"><button>Вход</button></form>
            </body></html>'''
    
    return '''<html><body style="text-align:center;padding-top:100px;">
        <h2>🔐 Вход в панель управления</h2>
        <form method="post">
            <input type="password" name="password" placeholder="Введите пароль" style="padding:10px;"><br><br>
            <button style="padding:10px 20px;">Вход</button>
        </form>
    </body></html>'''

@app.route('/logout')
def logout():
    """Выход из системы"""
    session.pop('logged_in', None)
    return redirect(url_for('login'))

@app.route('/download_template/<t_type>')
def download_template(t_type):
    """Скачивание шаблона файла"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    output = io.BytesIO()
    
    if t_type == 'dr':
        # Шаблон для дней рождения
        df = pd.DataFrame(columns=['Фамилия Имя', 'Должность', 'Подразделение', 'День Месяц Рождения'])
        # Добавляем пример
        example = pd.DataFrame([['Иванов Иван', 'Менеджер', 'Отдел продаж', '15.03']], 
                               columns=['Фамилия Имя', 'Должность', 'Подразделение', 'День Месяц Рождения'])
        df = pd.concat([df, example], ignore_index=True)
    else:
        # Шаблон для значимых событий
        df = pd.DataFrame(columns=['Событие', 'Напоминание', 'Дата и время'])
        # Добавляем пример
        example = pd.DataFrame([['Встреча с клиентом', 'Совещание в переговорной', '25.12.2024 14:30']], 
                               columns=['Событие', 'Напоминание', 'Дата и время'])
        df = pd.concat([df, example], ignore_index=True)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Лист1')
    
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"{t_type}_template.xlsx")

# --- ИНИЦИАЛИЗАЦИЯ ---
init_db()

# Запуск планировщика
scheduler = BackgroundScheduler(timezone=MSK)
scheduler.add_job(check_and_send, 'interval', seconds=30, max_instances=1)
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=False)
