import os
import sqlite3
import pandas as pd
from datetime import datetime
import pytz
import threading
import asyncio

from flask import Flask, render_template, request, redirect, session, flash
from aiogram import Bot
from apscheduler.schedulers.background import BackgroundScheduler

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')
DB_PATH = '/data/bot_database.db'
MSK = pytz.timezone('Europe/Moscow')

app = Flask(__name__)
app.secret_key = "super_secret_key"
bot = Bot(token=TOKEN)

# --- БАЗА ДАННЫХ ---
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS birthdays 
            (id INTEGER PRIMARY KEY, full_name TEXT, pos TEXT, dep TEXT, bday TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS events 
            (id INTEGER PRIMARY KEY, name TEXT, rem TEXT, dt TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS custom 
            (id INTEGER PRIMARY KEY, text TEXT, dt TEXT, period TEXT)''')

# --- ЛОГИКА БОТА И ПЛАНИРОВЩИКА ---
async def send_msg(text):
    try:
        await bot.send_message(CHAT_ID, text)
    except Exception as e:
        print(f"Error sending: {e}")

def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro)

def scheduler_job():
    now = datetime.now(MSK)
    now_str_short = now.strftime("%d.%m")
    now_str_full = now.strftime("%d.%m.%Y %H:%M")

    with get_db() as conn:
        # 1. Проверка Дней Рождения (в 09:00)
        if now.hour == 9 and now.minute == 0:
            users = conn.execute("SELECT * FROM birthdays WHERE bday = ?", (now_str_short,)).fetchall()
            if users:
                msg = "🎉🫶🏼Сегодня день рождения наших коллег:\n"
                for u in users:
                    msg += f"• {u['full_name']}, {u['pos']}, {u['dep']}\n"
                msg += "Поздравляем 😊🎊"
                run_async(send_msg(msg))

        # 2. Значимые события
        evs = conn.execute("SELECT * FROM events WHERE dt = ?", (now_str_full,)).fetchall()
        for e in evs:
            run_async(send_msg(f"💡 {e['rem']}"))

        # 3. Custom уведомления
        custs = conn.execute("SELECT * FROM custom WHERE dt = ?", (now_str_full,)).fetchall()
        for c in custs:
            run_async(send_msg(c['text']))
            # Тут можно добавить логику переноса даты для регулярных (period)

# --- WEB ИНТЕРФЕЙС ---
@app.before_request
def check_auth():
    if request.endpoint not in ['login', 'static'] and not session.get('auth'):
        return redirect('/login')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['auth'] = True
            return redirect('/')
    return '🔎 <form method="post">Пароль: <input name="password" type="password"><button>Вход</button></form>'

@app.route('/')
def index():
    with get_db() as conn:
        bdays = conn.execute("SELECT * FROM birthdays").fetchall()
        evs = conn.execute("SELECT * FROM events").fetchall()
        customs = conn.execute("SELECT * FROM custom").fetchall()
    return render_template('index.html', bdays=bdays, evs=evs, customs=customs)

@app.route('/upload_dr', methods=['POST'])
def upload_dr():
    file = request.files['file']
    if file:
        df = pd.read_excel(file) if file.filename.endswith('.xlsx') else pd.read_csv(file)
        with get_db() as conn:
            conn.execute("DELETE FROM birthdays")
            for _, row in df.iterrows():
                conn.execute("INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)", 
                             (row[0], row[1], row[2], str(row[3])))
        flash("Список ДР обновлен")
    return redirect('/')

# (Добавьте аналогичные роуты для upload_zs и custom)

if __name__ == "__main__":
    init_db()
    scheduler = BackgroundScheduler(timezone=MSK)
    scheduler.add_job(scheduler_job, 'interval', minutes=1)
    scheduler.start()
    app.run(host='0.0.0.0', port=80)
