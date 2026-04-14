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
# Все переменные из окружения (без fallback значений)
TOKEN = os.getenv('TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')

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
            print(f"[SENT] {text[:50]}...")
        except Exception as e:
            print(f"[ERROR] Ошибка отправки сообщения: {e}")
    else:
        print(f"[NOT CONFIGURED] TOKEN={TOKEN is not None}, CHAT_ID={CHAT_ID is not None}")
        print(f"[MESSAGE] {text[:50]}...")

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
    now_dm = now.strftime("%d.%m")  # Текущий день и месяц для ДР (ДД.ММ)
    current_weekday = now.weekday()  # 0=Пн, 6=Вс
    now_time_hm = now.strftime("%H:%M")  # Текущее время ЧЧ:ММ
    now_str = now.strftime("%d.%m.%Y %H:%M:%S")
    
    print(f"\n[{'='*50}]")
    print(f"[CHECK] {now_str} MSK")
    print(f"[INFO] now_dm={now_dm}, weekday={current_weekday}, time={now_time_hm}")
    
    conn = get_db_connection()
    try:
        # 1. ДНИ РОЖДЕНИЯ (09:00 МСК)
        print(f"\n[BDAY CHECK] hour={now.hour}, minute={now.minute}")
        if now.hour == 20 and now.minute == 30:
            celebrants = conn.execute("SELECT * FROM birthdays").fetchall()
            print(f"[BDAY] Found {len(celebrants)} total birthdays in DB")
            birthday_people = []
            
            for person in celebrants:
                bday_str = str(person['bday']).strip() if person['bday'] else ""
                print(f"[BDAY] Checking: {person['full_name']}, bday='{bday_str}', now_dm='{now_dm}'")
                # Проверяем совпадение ДД.ММ
                if bday_str and bday_str.startswith(now_dm):
                    birthday_people.append(person)
                    print(f"[BDAY] MATCH! {person['full_name']}")
            
            if birthday_people:
                # Формируем сообщение согласно ТЗ
                msg_lines = ["🎉🫶🏼 Сегодня день рождения наших коллег:"]
                for person in birthday_people:
                    msg_lines.append(f"• {person['full_name']}, {person['pos']}, {person['dep']}")
                msg_lines.append("Поздравляем 😊🎊")
                msg = "\n".join(msg_lines)
                send_msg_threadsafe(msg)
                print(f"[BDAY SENT] Message sent for {len(birthday_people)} people")
            else:
                print(f"[BDAY] No birthdays today")
        else:
            print(f"[BDAY SKIP] Not 09:00 (current: {now.hour}:{now.minute:02d})")
        
        # 2. ЗНАЧИМЫЕ СОБЫТИЯ (ЗС) - Точное время
        events = conn.execute("SELECT * FROM events WHERE is_sent = 0").fetchall()
        print(f"\n[EVENTS] Found {len(events)} unsent events")
        
        for event in events:
            try:
                event_dt_str = event['dt']
                print(f"[EVENT] id={event['id']}, dt='{event_dt_str}', name='{event['event_name']}'")
                
                if not event_dt_str:
                    print(f"[EVENT SKIP] Empty dt")
                    continue
                
                # Парсим дату события
                try:
                    event_dt = datetime.strptime(event_dt_str, "%d.%m.%Y %H:%M:%S").replace(tzinfo=MSK)
                except ValueError as e:
                    print(f"[EVENT ERROR] Cannot parse date '{event_dt_str}': {e}")
                    continue
                
                print(f"[EVENT] Parsed: {event_dt.strftime('%d.%m.%Y %H:%M:%S')}, Now: {now_str}")
                print(f"[EVENT] event_dt <= now: {event_dt <= now}")
                
                # Сравниваем с текущим временем
                if event_dt <= now:
                    msg = f"💡 {event['reminder_text']}"
                    send_msg_threadsafe(msg)
                    conn.execute("UPDATE events SET is_sent = 1 WHERE id = ?", (event['id'],))
                    conn.commit()
                    print(f"[EVENT SENT] id={event['id']}: {event['event_name']}")
                else:
                    print(f"[EVENT WAIT] Event time not reached yet")
                    
            except Exception as ex:
                print(f"[EVENT ERROR] id={event['id']}: {ex}")
                import traceback
                traceback.print_exc()
        
        # 3. CUSTOM ЗАДАЧИ
        custom_tasks = conn.execute("SELECT * FROM custom_tasks").fetchall()
        print(f"\n[CUSTOM] Found {len(custom_tasks)} custom tasks")
        
        for task in custom_tasks:
            try:
                task_dt_str = str(task['dt']).strip() if task['dt'] else ""
                if not task_dt_str:
                    print(f"[CUSTOM SKIP] id={task['id']}: Empty dt")
                    continue
                
                period = task['period']
                weekdays_str = task['weekdays'] or ""
                last_sent = task['last_sent']
                
                # Парсим время из dt (формат: ДД.ММ.ГГГГ ЧЧ:ММ)
                task_time = task_dt_str.split(' ')[1] if ' ' in task_dt_str else ""
                
                # Проверяем, не отправляли ли уже в эту минуту
                current_minute = now.strftime("%d.%m.%Y %H:%M")
                if last_sent == current_minute:
                    print(f"[CUSTOM SKIP] id={task['id']}: Already sent this minute")
                    continue
                
                print(f"[CUSTOM] id={task['id']}, period={period}, dt='{task_dt_str}', time='{task_time}'")
                
                should_send = False
                reason = ""
                
                if period == 'once':
                    # Разовая задача - проверяем точное совпадение даты и времени
                    should_send = task_dt_str == current_minute
                    reason = f"once: {task_dt_str} == {current_minute}"
                
                elif period == 'daily':
                    # Каждый день в указанное время
                    should_send = task_time == now_time_hm
                    reason = f"daily: {task_time} == {now_time_hm}"
                
                elif period == 'workdays':
                    # Рабочие дни (Пн-Пт) в указанное время
                    should_send = current_weekday < 5 and task_time == now_time_hm
                    reason = f"workdays: weekday={current_weekday}<5, {task_time}=={now_time_hm}"
                
                elif period == 'weekdays':
                    # Выбранные дни недели
                    selected_days = weekdays_str.split(',') if weekdays_str else []
                    should_send = str(current_weekday) in selected_days and task_time == now_time_hm
                    reason = f"weekdays: {current_weekday} in {selected_days}, {task_time}=={now_time_hm}"
                
                elif period == 'weekly':
                    # Каждую неделю - проверяем день недели и время
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    should_send = task_start.weekday() == current_weekday and task_time == now_time_hm
                    reason = f"weekly: task_weekday={task_start.weekday()}=={current_weekday}, {task_time}=={now_time_hm}"
                
                elif period == 'monthly':
                    # Каждый месяц - проверяем день месяца и время
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    should_send = task_start.day == now.day and task_time == now_time_hm
                    reason = f"monthly: task_day={task_start.day}=={now.day}, {task_time}=={now_time_hm}"
                
                elif period == 'yearly':
                    # Каждый год - проверяем день и месяц
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    task_dm = task_start.strftime("%d.%m")
                    now_dm_check = now.strftime("%d.%m")
                    should_send = task_dm == now_dm_check and task_time == now_time_hm
                    reason = f"yearly: {task_dm}=={now_dm_check}, {task_time}=={now_time_hm}"
                
                print(f"[CUSTOM] id={task['id']}: should_send={should_send}, reason={reason}")
                
                if should_send:
                    send_msg_threadsafe(task['text'])
                    conn.execute("UPDATE custom_tasks SET last_sent = ? WHERE id = ?", (current_minute, task['id']))
                    conn.commit()
                    print(f"[CUSTOM SENT] id={task['id']}: {task['text'][:30]}...")
                    
                    # Для разовых задач удаляем после отправки
                    if period == 'once':
                        conn.execute("DELETE FROM custom_tasks WHERE id = ?", (task['id'],))
                        conn.commit()
                        print(f"[CUSTOM DELETED] one-time task id={task['id']}")
            
            except Exception as ex:
                print(f"[CUSTOM ERROR] id={task['id']}: {ex}")
                import traceback
                traceback.print_exc()
    
    finally:
        conn.close()
        print(f"[{'='*50}]\n")

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
    """Нормализация даты и времени события к формату ДД.ММ.ГГГГ ЧЧ:ММ:СС"""
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
            count = 0
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
                    count += 1
            
            conn.commit()
            flash(f"✅ Список дней рождения обновлен! Загружено записей: {count}")
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
            count = 0
            for _, row in df.iterrows():
                event_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                reminder_text = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
                dt = normalize_event_datetime(row.iloc[2])
                
                if event_name and dt:  # Записываем только если есть название и дата
                    conn.execute(
                        "INSERT INTO events (event_name, reminder_text, dt, is_sent) VALUES (?,?,?,0)",
                        (event_name, reminder_text, dt)
                    )
                    count += 1
            
            conn.commit()
            flash(f"✅ Список значимых событий обновлен! Загружено записей: {count}")
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
print(f"[STARTED] Scheduler started. Port: 80, Timezone: {MSK}")
print(f"[CONFIG] TOKEN set: {TOKEN is not None}, CHAT_ID set: {CHAT_ID is not None}, ADMIN_PASSWORD set: {ADMIN_PASSWORD is not None}")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=False)
