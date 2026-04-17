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

# --- ИИ ИНТЕГРАЦИЯ ---
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("[WARNING] OpenAI not installed. AI features disabled.")


# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv('TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')

# ИИ конфигурация (VseGPT - доступен из РФ)
VSEGPT_API_KEY = os.getenv('VSEGPT_API_KEY')
VSEGPT_MODEL = os.getenv('VSEGPT_MODEL', 'gpt-4o-mini')  # gpt-4o-mini, gpt-4o, claude-3-5-sonnet и др.
VSEGPT_BASE_URL = "https://api.vsegpt.ru/v1"

DB_PATH = '/data/bot_database.db'
MSK = pytz.timezone('Europe/Moscow')

# Инициализация VseGPT клиента (OpenAI-совместимый API)
ai_client = None
if OPENAI_AVAILABLE and VSEGPT_API_KEY:
    ai_client = OpenAI(
        api_key=VSEGPT_API_KEY,
        base_url=VSEGPT_BASE_URL
    )
    print(f"[INIT] VseGPT initialized with model: {VSEGPT_MODEL}")
elif not VSEGPT_API_KEY:
    print("[INIT] VSEGPT_API_KEY not set. AI features disabled.")

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Инициализация бота
bot = None
if TOKEN:
    bot = Bot(token=TOKEN)

# --- EVENT LOOP ---
bot_loop = asyncio.new_event_loop()

def start_bot_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_bot_loop, args=(bot_loop,), daemon=True).start()

def send_msg_threadsafe(text):
    """Безопасная отправка сообщения"""
    if bot and CHAT_ID:
        try:
            asyncio.run_coroutine_threadsafe(bot.send_message(CHAT_ID, text, parse_mode='HTML'), bot_loop)
        except Exception as e:
            print(f"[ERROR] Send failed: {e}")

# --- ИИ ФУНКЦИИ ---
def generate_ai_message(prompt_template, context=None):
    """Генерация сообщения через VseGPT (доступен из РФ)"""
    if not ai_client:
        print("[AI ERROR] VseGPT not configured. Set VSEGPT_API_KEY.")
        return None

    try:
        system_prompt = """Ты - корпоративный ассистент для рабочего чата Telegram. 
Твоя задача - создавать краткие, дружелюбные и информативные сообщения для коллег.
Используй эмодзи уместно. Сообщение должно быть на русском языке.
Будь профессиональным, но тёплым тоном."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt_template}
        ]

        if context:
            messages.insert(1, {"role": "user", "content": f"Контекст: {context}"})

        response = ai_client.chat.completions.create(
            model=VSEGPT_MODEL,
            messages=messages,
            max_tokens=500,
            temperature=0.7
        )

        message = response.choices[0].message.content.strip()
        print(f"[AI] Generated message: {message[:100]}...")
        return message

    except Exception as e:
        print(f"[AI ERROR] {e}")
        return None

def generate_birthday_message(name, position, department):
    """Генерация поздравления с днём рождения"""
    prompt = f"""Напиши поздравление с днём рождения для коллеги:
Имя: {name}
Должность: {position}
Отдел: {department}

Требования:
- Начни с эмодзи 🎉
- Поздравь тепло и по-дружески
- Упомяни должность и отдел
- Пожелай успехов в работе и личной жизни
- Максимум 4-5 предложений"""

    return generate_ai_message(prompt)

def generate_reminder_message(task_description, urgency="normal"):
    """Генерация напоминания о задаче"""
    urgency_text = {
        "high": "СРОЧНО!",
        "normal": "Напоминание:",
        "low": "Не забудьте:"
    }.get(urgency, "Напоминание:")

    prompt = f"""{urgency_text} {task_description}

Напиши краткое напоминание для рабочего чата.
- Используй эмодзи 💡 или ⚠️
- Будь лаконичным (2-3 предложения)
- Укажи важность задачи"""

    return generate_ai_message(prompt)

def generate_daily_summary(tasks, events, birthdays):
    """Генерация ежедневной сводки"""
    context = f"""Задачи на сегодня: {tasks}
События: {events}
Дни рождения: {birthdays}"""

    prompt = """Напиши утреннюю сводку для рабочего чата.
Структура:
📅 Доброе утро! Сегодня {дата}

🎯 Задачи:
[список]

📢 События:
[список]

🎉 Дни рождения:
[список или "Сегодня никто не празднует"]

Хорошего дня! 💪"""

    return generate_ai_message(prompt, context)

# --- DATABASE ---
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS birthdays 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, full_name TEXT, pos TEXT, dep TEXT, bday TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS events 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, event_name TEXT, reminder_text TEXT, dt TEXT, is_sent INTEGER DEFAULT 0)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS custom_tasks 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, dt TEXT, period TEXT, weekdays TEXT, last_sent TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS sent_log 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, ref_id INTEGER, sent_date TEXT, UNIQUE(type, ref_id, sent_date))''')

        # Таблица для ИИ-задач
        conn.execute('''CREATE TABLE IF NOT EXISTS ai_tasks 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, 
             name TEXT, 
             prompt_template TEXT, 
             context TEXT, 
             schedule_time TEXT, 
             period TEXT, 
             is_active INTEGER DEFAULT 1,
             last_sent TEXT)''')
        
        # Migrations
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

def is_already_sent_today(conn, notif_type, ref_id, today_str):
    """Проверка, было ли уже отправлено уведомление сегодня"""
    cursor = conn.execute(
        "SELECT 1 FROM sent_log WHERE type = ? AND ref_id = ? AND sent_date = ?",
        (notif_type, ref_id, today_str)
    )
    return cursor.fetchone() is not None

def mark_as_sent(conn, notif_type, ref_id, today_str):
    """Отметить уведомление как отправленное"""
    try:
        conn.execute(
            "INSERT OR IGNORE INTO sent_log (type, ref_id, sent_date) VALUES (?, ?, ?)",
            (notif_type, ref_id, today_str)
        )
        conn.commit()
    except:
        pass

# --- SCHEDULER ---
def check_and_send():
    """Проверка и отправка уведомлений"""
    now = datetime.now(MSK)
    now_dm = now.strftime("%d.%m")
    today_str = now.strftime("%Y-%m-%d")
    current_weekday = now.weekday()
    now_time_hm = now.strftime("%H:%M")
    
    conn = get_db_connection()
    try:
        # 1. BIRTHDAYS (09:00 MSK)
        if now.hour == 9 and now.minute <= 1:
            celebrants = conn.execute("SELECT * FROM birthdays").fetchall()
            birthday_people = []
            
            for person in celebrants:
                bday_str = str(person['bday']).strip() if person['bday'] else ""
                if bday_str and bday_str.startswith(now_dm):
                    if not is_already_sent_today(conn, 'birthday', person['id'], today_str):
                        birthday_people.append(person)
            
            if birthday_people:
                msg_lines = ["🎉🫶🏼 Сегодня день рождения наших коллег:"]
                for person in birthday_people:
                    msg_lines.append(f"• <b>{person['full_name']}</b>, {person['pos']}, {person['dep']}")
                    mark_as_sent(conn, 'birthday', person['id'], today_str)
                msg_lines.append("Поздравляем 😊🎊")
                send_msg_threadsafe("\n".join(msg_lines))
        
        # 2. EVENTS
        events = conn.execute("SELECT * FROM events WHERE is_sent = 0").fetchall()
        for event in events:
            try:
                event_dt_str = event['dt']
                if not event_dt_str:
                    continue
                
                event_dt = datetime.strptime(event_dt_str, "%d.%m.%Y %H:%M:%S")
                now_naive = now.replace(tzinfo=None)
                
                if event_dt <= now_naive:
                    send_msg_threadsafe(event['reminder_text'])
                    conn.execute("UPDATE events SET is_sent = 1 WHERE id = ?", (event['id'],))
                    conn.commit()
            except:
                pass
        
        # 3. CUSTOM TASKS
        custom_tasks = conn.execute("SELECT * FROM custom_tasks").fetchall()
        for task in custom_tasks:
            try:
                task_dt_str = str(task['dt']).strip() if task['dt'] else ""
                if not task_dt_str:
                    continue
                
                period = task['period']
                weekdays_str = task['weekdays'] or ""
                last_sent = task['last_sent']
                task_time = task_dt_str.split(' ')[1] if ' ' in task_dt_str else ""
                current_minute = now.strftime("%d.%m.%Y %H:%M")
                
                if last_sent == current_minute:
                    continue
                
                should_send = False
                
                if period == 'once':
                    should_send = task_dt_str == current_minute
                elif period == 'daily':
                    should_send = task_time == now_time_hm
                elif period == 'workdays':
                    should_send = current_weekday < 5 and task_time == now_time_hm
                elif period == 'weekdays':
                    selected_days = weekdays_str.split(',') if weekdays_str else []
                    should_send = str(current_weekday) in selected_days and task_time == now_time_hm
                elif period == 'weekly':
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    should_send = task_start.weekday() == current_weekday and task_time == now_time_hm
                elif period == 'monthly':
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    should_send = task_start.day == now.day and task_time == now_time_hm
                elif period == 'yearly':
                    task_start = datetime.strptime(task_dt_str, "%d.%m.%Y %H:%M")
                    task_dm = task_start.strftime("%d.%m")
                    now_dm_check = now.strftime("%d.%m")
                    should_send = task_dm == now_dm_check and task_time == now_time_hm
                
                if should_send:
                    send_msg_threadsafe(task['text'])
                    conn.execute("UPDATE custom_tasks SET last_sent = ? WHERE id = ?", (current_minute, task['id']))
                    conn.commit()
                    
                    if period == 'once':
                        conn.execute("DELETE FROM custom_tasks WHERE id = ?", (task['id'],))
                        conn.commit()
            except:
                pass
    finally:
        conn.close()

# --- HELPERS ---
def normalize_bday_date(val):
    if pd.isna(val):
        return ""
    try:
        val_str = str(val).strip()
        if len(val_str) == 5 and val_str[2] == '.':
            return val_str
        formats = ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m"]
        for fmt in formats:
            try:
                dt_obj = datetime.strptime(val_str, fmt)
                return dt_obj.strftime("%d.%m")
            except:
                continue
        return val_str
    except:
        return str(val).strip()

def normalize_event_datetime(val):
    if pd.isna(val):
        return ""
    try:
        if isinstance(val, datetime):
            return val.strftime("%d.%m.%Y %H:%M:%S")
        val_str = str(val).strip()
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
    filename = file.filename.lower()
    try:
        if filename.endswith('.csv'):
            for encoding in ['utf-8', 'cp1251', 'latin1']:
                try:
                    file.seek(0)
                    df = pd.read_csv(file, encoding=encoding)
                    break
                except:
                    continue
            else:
                raise ValueError("Cannot read CSV")
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, engine='openpyxl')
        else:
            raise ValueError("Unsupported format")
        df = df.dropna(how='all')
        return df
    except Exception as e:
        raise e

def get_period_display(period, weekdays=None):
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

# --- ROUTES ---
@app.route('/test_send/<type>')
def test_send(type):
    test_msg = f"🛠 Тест связи ({type}): Бот работает стабильно!"
    send_msg_threadsafe(test_msg)
    flash(f"Тестовое сообщение ({type}) отправлено в Telegram!")
    return redirect(url_for('index'))

@app.route('/')
def index():
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
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    file = request.files.get('file')
    if not file:
        flash("Файл не выбран!")
        return redirect(url_for('index'))
    
    try:
        df = read_data_file(file)
        if len(df.columns) < 4:
            flash("Ошибка: файл должен содержать минимум 4 столбца")
            return redirect(url_for('index'))
        
        conn = get_db_connection()
        try:
            conn.execute("DELETE FROM birthdays")
            count = 0
            for _, row in df.iterrows():
                full_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                pos = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
                dep = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ""
                bday = normalize_bday_date(row.iloc[3])
                if full_name:
                    conn.execute(
                        "INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)",
                        (full_name, pos, dep, bday)
                    )
                    count += 1
            conn.commit()
            flash(f"✅ Список дней рождения обновлен! Загружено: {count}")
        finally:
            conn.close()
    except Exception as e:
        flash(f"❌ Ошибка: {str(e)}")
    
    return redirect(url_for('index'))

@app.route('/upload_zs', methods=['POST'])
def upload_zs():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    file = request.files.get('file')
    if not file:
        flash("Файл не выбран!")
        return redirect(url_for('index'))
    
    try:
        df = read_data_file(file)
        if len(df.columns) < 3:
            flash("Ошибка: файл должен содержать минимум 3 столбца")
            return redirect(url_for('index'))
        
        conn = get_db_connection()
        try:
            conn.execute("DELETE FROM events")
            count = 0
            for _, row in df.iterrows():
                event_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                reminder_text = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
                dt = normalize_event_datetime(row.iloc[2])
                if event_name and dt:
                    conn.execute(
                        "INSERT INTO events (event_name, reminder_text, dt, is_sent) VALUES (?,?,?,0)",
                        (event_name, reminder_text, dt)
                    )
                    count += 1
            conn.commit()
            flash(f"✅ Список событий обновлен! Загружено: {count}")
        finally:
            conn.close()
    except Exception as e:
        flash(f"❌ Ошибка: {str(e)}")
    
    return redirect(url_for('index'))

@app.route('/add_custom', methods=['POST'])
def add_custom():
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
        
        dt_final = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M').strftime('%d.%m.%Y %H:%M')
        
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO custom_tasks (text, dt, period, weekdays, last_sent) VALUES (?,?,?,?,?)",
                (text, dt_final, period, ",".join(days) if days else "", None)
            )
            conn.commit()
            flash("✅ Задача добавлена!")
        finally:
            conn.close()
    except Exception as e:
        flash(f"❌ Ошибка: {str(e)}")
    
    return redirect(url_for('index'))

@app.route('/delete_custom/<int:id>')
def delete_custom(id):
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
        <h2>🔐 Вход</h2>
        <form method="post">
            <input type="password" name="password" placeholder="Введите пароль" style="padding:10px;"><br><br>
            <button style="padding:10px 20px;">Вход</button>
        </form>
    </body></html>'''

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

@app.route('/download_template/<t_type>')
def download_template(t_type):
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    output = io.BytesIO()
    
    if t_type == 'dr':
        df = pd.DataFrame(columns=['Фамилия Имя', 'Должность', 'Подразделение', 'День Месяц Рождения'])
        example = pd.DataFrame([['Иванов Иван', 'Менеджер', 'Отдел продаж', '15.03']], 
                               columns=['Фамилия Имя', 'Должность', 'Подразделение', 'День Месяц Рождения'])
        df = pd.concat([df, example], ignore_index=True)
    else:
        df = pd.DataFrame(columns=['Событие', 'Напоминание', 'Дата и время'])
        example = pd.DataFrame([['Встреча с клиентом', 'Совещание в переговорной', '25.12.2024 14:30']], 
                               columns=['Событие', 'Напоминание', 'Дата и время'])
        df = pd.concat([df, example], ignore_index=True)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Лист1')
    
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"{t_type}_template.xlsx")


# --- AI TASKS ROUTES ---
@app.route('/ai_tasks')
def ai_tasks():
    """Страница управления ИИ-задачами"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))

    conn = get_db_connection()
    try:
        tasks = conn.execute("SELECT * FROM ai_tasks ORDER BY id DESC").fetchall()
    finally:
        conn.close()

    ai_enabled = ai_client is not None
    return render_template('ai_tasks.html', tasks=tasks, ai_enabled=ai_enabled)

@app.route('/add_ai_task', methods=['POST'])
def add_ai_task():
    """Добавление ИИ-задачи"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))

    if not ai_client:
        flash("ИИ не настроен! Добавьте VSEGPT_API_KEY")
        return redirect(url_for('ai_tasks'))

    try:
        name = request.form.get('name', '').strip()
        prompt_template = request.form.get('prompt_template', '').strip()
        context = request.form.get('context', '').strip()
        schedule_time = request.form.get('schedule_time', '')  # HH:MM
        period = request.form.get('period', 'daily')

        if not name or not prompt_template or not schedule_time:
            flash("Заполните все обязательные поля!")
            return redirect(url_for('ai_tasks'))

        conn = get_db_connection()
        try:
            conn.execute(
                """INSERT INTO ai_tasks (name, prompt_template, context, schedule_time, period, is_active) 
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (name, prompt_template, context, schedule_time, period)
            )
            conn.commit()
            flash(f"✅ ИИ-задача '{name}' добавлена!")
        finally:
            conn.close()
    except Exception as e:
        flash(f"❌ Ошибка: {str(e)}")

    return redirect(url_for('ai_tasks'))

@app.route('/toggle_ai_task/<int:id>')
def toggle_ai_task(id):
    """Включение/выключение ИИ-задачи"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))

    conn = get_db_connection()
    try:
        task = conn.execute("SELECT * FROM ai_tasks WHERE id = ?", (id,)).fetchone()
        if task:
            new_status = 0 if task['is_active'] else 1
            conn.execute("UPDATE ai_tasks SET is_active = ? WHERE id = ?", (new_status, id))
            conn.commit()
            status_text = "включена" if new_status else "выключена"
            flash(f"Задача {status_text}")
    finally:
        conn.close()

    return redirect(url_for('ai_tasks'))

@app.route('/delete_ai_task/<int:id>')
def delete_ai_task(id):
    """Удаление ИИ-задачи"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))

    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM ai_tasks WHERE id = ?", (id,))
        conn.commit()
        flash("Задача удалена")
    finally:
        conn.close()

    return redirect(url_for('ai_tasks'))

@app.route('/test_ai_task/<int:id>')
def test_ai_task(id):
    """Тестовая генерация сообщения для ИИ-задачи"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))

    if not ai_client:
        flash("ИИ не настроен! Добавьте VSEGPT_API_KEY")
        return redirect(url_for('ai_tasks'))

    conn = get_db_connection()
    try:
        task = conn.execute("SELECT * FROM ai_tasks WHERE id = ?", (id,)).fetchone()
        if task:
            message = generate_ai_message(task['prompt_template'], task['context'])
            if message:
                test_msg = f"🧪 Тест ИИ-задачи '{task['name']}':\n\n{message}"
                send_msg_threadsafe(test_msg)
                flash("Тестовое сообщение отправлено!")
            else:
                flash("Ошибка генерации сообщения")
    finally:
        conn.close()

    return redirect(url_for('ai_tasks'))

# --- INIT ---
init_db()
scheduler = BackgroundScheduler(timezone=MSK)
scheduler.add_job(check_and_send, 'interval', seconds=30, max_instances=1)
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=False)
