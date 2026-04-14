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
            (id INTEGER PRIMARY KEY AUTOINCREMENT, event_name TEXT, reminder_text TEXT, dt TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS custom_tasks 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, dt TEXT, period TEXT, weekdays TEXT)''')
        
        # Миграция колонок для таблицы events (ЗС)
        existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()]
        if 'event_name' not in existing_cols:
            conn.execute("ALTER TABLE events ADD COLUMN event_name TEXT")
        if 'reminder_text' not in existing_cols:
            conn.execute("ALTER TABLE events ADD COLUMN reminder_text TEXT")
            
        # Миграция колонок для custom_tasks (Дни недели)
        custom_cols = [row[1] for row in conn.execute("PRAGMA table_info(custom_tasks)").fetchall()]
        if 'weekdays' not in custom_cols:
            conn.execute("ALTER TABLE custom_tasks ADD COLUMN weekdays TEXT")
        conn.commit()

# --- ПЛАНИРОВЩИК ---
async def send_to_tg(text):
    try:
        await bot.send_message(CHAT_ID, text)
    except Exception as e:
        print(f"Ошибка TG: {e}")

def check_and_send():
    now = datetime.now(MSK)
    now_dm = now.strftime("%d.%m")
    now_full = now.strftime("%d.%m.%y %H:%M")
    now_time = now.strftime("%H:%M")
    current_weekday = str(now.weekday()) # 0=Пн, 6=Вс

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with get_db_connection() as conn:
        # 1. ДР
        if now.hour == 9 and now.minute == 0:
            users = conn.execute("SELECT * FROM birthdays WHERE bday = ?", (now_dm,)).fetchall()
            if users:
                msg = f"🎉🫶🏼Сегодня день рождения наших коллег:\n" + "\n".join([f"• {u['full_name']}, {u['pos']}, {u['dep']}" for u in users]) + "\nПоздравляем 😊🎊"
                loop.run_until_complete(send_to_tg(msg))
        
        # 2. ЗС
        evs = conn.execute("SELECT * FROM events WHERE dt = ?", (now_full,)).fetchall()
        for e in evs:
            loop.run_until_complete(send_to_tg(f"💡 {e['reminder_text']}"))
            
        # 3. CUSTOM
        tasks = conn.execute("SELECT * FROM custom_tasks").fetchall()
        for t in tasks:
            task_time = t['dt'].split(' ')[1] if ' ' in t['dt'] else ""
            
            # Единоразово
            if t['period'] == 'once' and t['dt'] == now_full:
                loop.run_until_complete(send_to_tg(t['text']))
            
            # По дням недели
            elif t['period'] == 'weekdays' and task_time == now_time:
                selected_days = t['weekdays'].split(',') if t['weekdays'] else []
                if current_weekday in selected_days:
                    loop.run_until_complete(send_to_tg(t['text']))
                    
    loop.close()

# --- ROUTES ---
@app.route('/')
def index():
    with get_db_connection() as conn:
        bdays = conn.execute("SELECT * FROM birthdays").fetchall()
        evs = conn.execute("SELECT * FROM events").fetchall()
        customs = conn.execute("SELECT * FROM custom_tasks").fetchall()
    return render_template('index.html', bdays=bdays, evs=evs, customs=customs)

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
        except Exception as e: flash(f"Ошибка загрузки ЗС: {e}")
    return redirect(url_for('index'))

@app.route('/add_custom', methods=['POST'])
def add_custom():
    try:
        text = request.form.get('text')
        dt_raw = request.form.get('dt')
        period = request.form.get('period')
        
        # Получаем выбранные дни недели
        days = request.form.getlist('days')
        if period == 'workdays':
            days = ['0','1','2','3','4']
            period = 'weekdays'
        
        days_str = ",".join(days)
        dt_obj = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M')
        dt_final = dt_obj.strftime('%d.%m.%y %H:%M')
        
        with get_db_connection() as conn:
            conn.execute("INSERT INTO custom_tasks (text, dt, period, weekdays) VALUES (?,?,?,?)", 
                         (text, dt_final, period, days_str))
            conn.commit()
        flash("Добавлено")
    except Exception as e: flash(f"Ошибка: {e}")
    return redirect(url_for('index'))

@app.route('/delete_custom/<int:id>')
def delete_custom(id):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM custom_tasks WHERE id = ?", (id,))
        conn.commit()
    return redirect(url_for('index'))

# Остальные роуты (login, upload_dr, download_template) остаются без изменений как в прошлом коде

init_db()
scheduler = BackgroundScheduler(timezone=MSK)
scheduler.add_job(check_and_send, 'interval', minutes=1)
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
