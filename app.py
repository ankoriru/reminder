import os
import sqlite3
import pandas as pd
import io
import asyncio
import threading
from datetime import datetime
import pytz

from flask import Flask, render_template, request, redirect, session, flash, url_for, send_file
from aiogram import Bot
from apscheduler.schedulers.background import BackgroundScheduler

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')
DB_PATH = '/data/bot_database.db'
MSK = pytz.timezone('Europe/Moscow')

app = Flask(__name__)
app.secret_key = os.urandom(24)
bot = Bot(token=TOKEN)

# Глобальная переменная для цикла событий бота
bot_loop = asyncio.new_event_loop()

def start_bot_loop(loop):
    """Запуск бесконечного цикла событий в отдельном потоке"""
    asyncio.set_event_loop(loop)
    loop.run_forever()

# Запускаем поток при старте приложения
threading.Thread(target=start_bot_loop, args=(bot_loop,), daemon=True).start()

# --- БАЗА ДАННЫХ ---
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
            (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, dt TEXT, period TEXT, weekdays TEXT)''')
        
        # Миграции
        cursor = conn.execute("PRAGMA table_info(events)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'is_sent' not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN is_sent INTEGER DEFAULT 0")
        
        cursor_c = conn.execute("PRAGMA table_info(custom_tasks)")
        cols_c = [row[1] for row in cursor_c.fetchall()]
        if 'weekdays' not in cols_c:
            conn.execute("ALTER TABLE custom_tasks ADD COLUMN weekdays TEXT")
        conn.commit()

# --- ЛОГИКА ОПОВЕЩЕНИЙ ---
def send_msg_threadsafe(text):
    """Безопасная отправка сообщения через выделенный цикл событий"""
    asyncio.run_coroutine_threadsafe(bot.send_message(CHAT_ID, text), bot_loop)

def check_and_send():
    """Синхронная проверка в планировщике"""
    now = datetime.now(MSK)
    now_dm = now.strftime("%d.%m")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    try:
        # 1. Дни Рождения (09:00 МСК)
        if now.hour == 9 and now.minute == 0 and 0 <= now.second <= 15:
            celebrants = [u for u in conn.execute("SELECT * FROM birthdays").fetchall() 
                          if str(u['bday']).strip().startswith(now_dm)]
            if celebrants:
                msg = "🎉🫶🏼 Сегодня день рождения коллег:\n" + \
                      "\n".join([f"• {u['full_name']}, {u['pos']}" for u in celebrants]) + "\n\n🎊"
                send_msg_threadsafe(msg)

        # 2. Значимые события (ЗС)
        events = conn.execute("SELECT * FROM events WHERE is_sent = 0").fetchall()
        for e in events:
            try:
                event_dt = datetime.strptime(e['dt'], "%d.%m.%Y %H:%M:%S").replace(tzinfo=MSK)
                if event_dt <= now:
                    send_msg_threadsafe(f"💡 {e['reminder_text']}")
                    conn.execute("UPDATE events SET is_sent = 1 WHERE id = ?", (e['id'],))
                    conn.commit()
            except Exception as ex:
                print(f"Ошибка даты ЗС: {ex}")

        # 3. Custom (Раз в минуту)
        if now.second < 15:
            now_custom_dt = now.strftime("%d.%m.%Y %H:%M")
            now_time_hm = now.strftime("%H:%M")
            current_weekday = str(now.weekday())
            for t in conn.execute("SELECT * FROM custom_tasks").fetchall():
                t_dt_str = str(t['dt']).strip()
                t_time = t_dt_str.split(' ')[1] if ' ' in t_dt_str else ""
                if (t['period'] == 'once' and t_dt_str == now_custom_dt) or \
                   (t['period'] == 'weekdays' and t_time == now_time_hm and current_weekday in (t['weekdays'] or "").split(',')):
                    send_msg_threadsafe(t['text'])
    finally:
        conn.close()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def normalize_date(val, include_seconds=True):
    if pd.isna(val): return ""
    try:
        if isinstance(val, datetime):
            dt_obj = val
        else:
            val_str = str(val).strip()
            for fmt in ["%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%d.%m.%Y", "%Y-%m-%d"]:
                try:
                    dt_obj = datetime.strptime(val_str, fmt)
                    break
                except: dt_obj = None
        if not dt_obj: return str(val).strip()
        return dt_obj.strftime("%d.%m.%Y %H:%M:%S" if include_seconds else "%d.%m.%Y")
    except: return str(val).strip()

# --- WEB ROUTES ---
@app.before_request
def auth_middleware():
    if request.endpoint not in ['login', 'static'] and not session.get('logged_in'):
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST' and request.form.get('password') == ADMIN_PASSWORD:
        session['logged_in'] = True
        return redirect(url_for('index'))
    return '<html><body style="text-align:center;padding-top:100px;"><h2>Вход</h2><form method="post"><input type="password" name="password"><button>Вход</button></form></body></html>'

@app.route('/')
def index():
    with get_db_connection() as conn:
        b = conn.execute("SELECT * FROM birthdays").fetchall()
        e = conn.execute("SELECT * FROM events ORDER BY is_sent ASC, dt DESC").fetchall()
        c = conn.execute("SELECT * FROM custom_tasks").fetchall()
    return render_template('index.html', bdays=b, evs=e, customs=c)

@app.route('/upload_dr', methods=['POST'])
def upload_dr():
    file = request.files.get('file')
    if file:
        df = pd.read_excel(file, engine='openpyxl').dropna(how='all')
        with get_db_connection() as conn:
            conn.execute("DELETE FROM birthdays")
            for _, r in df.iterrows():
                conn.execute("INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)", 
                             (str(r.iloc[0]).strip(), str(r.iloc[1]).strip(), str(r.iloc[2]).strip(), normalize_date(r.iloc[3], False)))
            conn.commit()
    return redirect(url_for('index'))

@app.route('/upload_zs', methods=['POST'])
def upload_zs():
    file = request.files.get('file')
    if file:
        df = pd.read_excel(file, engine='openpyxl').dropna(how='all')
        with get_db_connection() as conn:
            conn.execute("DELETE FROM events")
            for _, r in df.iterrows():
                conn.execute("INSERT INTO events (event_name, reminder_text, dt, is_sent) VALUES (?,?,?,0)", 
                             (str(r.iloc[0]).strip(), str(r.iloc[1]).strip(), normalize_date(r.iloc[2], True)))
            conn.commit()
    return redirect(url_for('index'))

@app.route('/add_custom', methods=['POST'])
def add_custom():
    dt_raw = request.form.get('dt')
    dt_final = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M').strftime('%d.%m.%Y %H:%M')
    with get_db_connection() as conn:
        conn.execute("INSERT INTO custom_tasks (text, dt, period, weekdays) VALUES (?,?,?,?)", 
                     (request.form.get('text'), dt_final, request.form.get('period'), ",".join(request.form.getlist('days'))))
        conn.commit()
    return redirect(url_for('index'))

@app.route('/delete_custom/<int:id>')
def delete_custom(id):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM custom_tasks WHERE id = ?", (id,))
        conn.commit()
    return redirect(url_for('index'))

@app.route('/download_template/<t_type>')
def download_template(t_type):
    output = io.BytesIO()
    cols = ['ФИО', 'Должн', 'Отдел', 'Дата'] if t_type=='dr' else ['Название', 'Текст', 'Дата']
    pd.DataFrame(columns=cols).to_excel(output, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"{t_type}.xlsx")

# --- СТАРТ ---
init_db()
scheduler = BackgroundScheduler(timezone=MSK)
scheduler.add_job(check_and_send, 'interval', seconds=10, max_instances=1)
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
