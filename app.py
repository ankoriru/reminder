import os
import sqlite3
import pandas as pd
import io
import asyncio
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

# --- БАЗА ДАННЫХ И МИГРАЦИИ ---
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS birthdays 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, full_name TEXT, pos TEXT, dep TEXT, bday TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS events 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, event_name TEXT, reminder_text TEXT, dt TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS custom_tasks 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, dt TEXT, period TEXT, weekdays TEXT)''')
        
        # Миграция: Проверка и добавление колонок для ЗС
        cursor = conn.execute("PRAGMA table_info(events)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'event_name' not in cols: conn.execute("ALTER TABLE events ADD COLUMN event_name TEXT")
        if 'reminder_text' not in cols: conn.execute("ALTER TABLE events ADD COLUMN reminder_text TEXT")
        
        # Миграция: Проверка и добавление колонок для CUSTOM
        cursor_c = conn.execute("PRAGMA table_info(custom_tasks)")
        cols_c = [row[1] for row in cursor_c.fetchall()]
        if 'weekdays' not in cols_c: conn.execute("ALTER TABLE custom_tasks ADD COLUMN weekdays TEXT")
        conn.commit()

# --- ЛОГИКА ОТПРАВКИ ---
async def send_to_tg(text):
    try:
        await bot.send_message(CHAT_ID, text)
    except Exception as e:
        print(f"Ошибка отправки в TG: {e}")

def check_and_send():
    now = datetime.now(MSK)
    now_dm = now.strftime("%d.%m")
    now_full_sec = now.strftime("%d.%m.%Y %H:%M:%S") # Формат для ЗС
    now_full_min = now.strftime("%d.%m.%y %H:%M")   # Формат для Custom (разово)
    now_time = now.strftime("%H:%M")
    current_weekday = str(now.weekday())

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    with get_db_connection() as conn:
        # 1. Дни Рождения (09:00:00 МСК)
        if now.hour == 9 and now.minute == 0 and now.second == 0:
            users = conn.execute("SELECT * FROM birthdays WHERE bday = ?", (now_dm,)).fetchall()
            if users:
                msg = "🎉🫶🏼Сегодня день рождения наших коллег:\n"
                for u in users:
                    msg += f"• {u['full_name']}, {u['pos']}, {u['dep']}\n"
                msg += "Поздравляем 😊🎊"
                loop.run_until_complete(send_to_tg(msg))

        # 2. Значимые события (По секундному совпадению)
        evs = conn.execute("SELECT * FROM events WHERE dt = ?", (now_full_sec,)).fetchall()
        for e in evs:
            loop.run_until_complete(send_to_tg(f"💡 {e['reminder_text']}"))

        # 3. CUSTOM уведомления (Проверка раз в минуту на 00 секунде)
        if now.second == 0:
            tasks = conn.execute("SELECT * FROM custom_tasks").fetchall()
            for t in tasks:
                t_time = t['dt'].split(' ')[1] if ' ' in t['dt'] else ""
                if t['period'] == 'once' and t['dt'] == now_full_min:
                    loop.run_until_complete(send_to_tg(t['text']))
                elif t['period'] == 'weekdays' and t_time == now_time:
                    allowed = t['weekdays'].split(',') if t['weekdays'] else []
                    if current_weekday in allowed:
                        loop.run_until_complete(send_to_tg(t['text']))
    loop.close()

# --- ROUTES ---
@app.before_request
def auth_middleware():
    if request.endpoint not in ['login', 'static'] and not session.get('logged_in'):
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        flash("Неверный пароль")
    return '<html><body style="text-align:center;padding-top:100px;font-family:sans-serif;"><h2>Вход</h2><form method="post"><input type="password" name="password" style="padding:10px;"><button style="padding:10px 20px;">Войти</button></form></body></html>'

@app.route('/')
def index():
    with get_db_connection() as conn:
        bdays = conn.execute("SELECT * FROM birthdays").fetchall()
        evs = conn.execute("SELECT * FROM events").fetchall()
        customs = conn.execute("SELECT * FROM custom_tasks").fetchall()
    return render_template('index.html', bdays=bdays, evs=evs, customs=customs)

@app.route('/download_template/<t_type>')
def download_template(t_type):
    output = io.BytesIO()
    if t_type == 'dr':
        df = pd.DataFrame(columns=['ФИО', 'Должность', 'Отдел', 'Дата (ДД.ММ)'])
        df.loc[0] = ['Иванов Иван', 'Менеджер', 'ИТ', '14.04']
    else:
        df = pd.DataFrame(columns=['Событие', 'Текст напоминания', 'Дата (ДД.ММ.ГГГГ ЧЧ:ММ:СС)'])
        df.loc[0] = ['Событие 1', '💡 Текст сообщения', '14.04.2026 12:00:00']
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"template_{t_type}.xlsx")

@app.route('/upload_dr', methods=['POST'])
def upload_dr():
    file = request.files.get('file')
    if file:
        try:
            df = pd.read_excel(file, engine='openpyxl').dropna(how='all')
            with get_db_connection() as conn:
                conn.execute("DELETE FROM birthdays")
                for _, r in df.iterrows():
                    conn.execute("INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)", 
                                 (str(r.iloc[0]), str(r.iloc[1]), str(r.iloc[2]), str(r.iloc[3])))
                conn.commit()
            flash("База ДР обновлена")
        except Exception as e: flash(f"Ошибка ДР: {e}")
    return redirect(url_for('index'))

@app.route('/upload_zs', methods=['POST'])
def upload_zs():
    file = request.files.get('file')
    if file:
        try:
            df = pd.read_excel(file, engine='openpyxl').dropna(how='all')
            with get_db_connection() as conn:
                conn.execute("DELETE FROM events")
                for _, r in df.iterrows():
                    conn.execute("INSERT INTO events (event_name, reminder_text, dt) VALUES (?,?,?)", 
                                 (str(r.iloc[0]), str(r.iloc[1]), str(r.iloc[2])))
                conn.commit()
            flash("База ЗС обновлена")
        except Exception as e: flash(f"Ошибка ЗС: {e}")
    return redirect(url_for('index'))

@app.route('/add_custom', methods=['POST'])
def add_custom():
    try:
        text, dt_raw, period = request.form.get('text'), request.form.get('dt'), request.form.get('period')
        days = request.form.getlist('days')
        if period == 'workdays': days = ['0','1','2','3','4']
        dt_obj = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M')
        dt_final = dt_obj.strftime('%d.%m.%y %H:%M')
        with get_db_connection() as conn:
            conn.execute("INSERT INTO custom_tasks (text, dt, period, weekdays) VALUES (?,?,?,?)", 
                         (text, dt_final, 'weekdays' if period != 'once' else 'once', ",".join(days)))
            conn.commit()
        flash("Custom добавлено")
    except Exception as e: flash(f"Ошибка: {e}")
    return redirect(url_for('index'))

@app.route('/delete_custom/<int:id>')
def delete_custom(id):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM custom_tasks WHERE id = ?", (id,))
        conn.commit()
    flash("Удалено")
    return redirect(url_for('index'))

# --- ИНИЦИАЛИЗАЦИЯ ---
init_db()
scheduler = BackgroundScheduler(timezone=MSK)
scheduler.add_job(check_and_send, 'interval', seconds=1) # Проверка каждую секунду
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
