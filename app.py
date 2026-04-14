import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import pytz
import asyncio
import threading

from flask import Flask, render_template, request, redirect, session, flash, url_for
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
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        # Таблица ДР
        conn.execute('''CREATE TABLE IF NOT EXISTS birthdays 
            (id INTEGER PRIMARY KEY, full_name TEXT, pos TEXT, dep TEXT, bday TEXT)''')
        # Таблица Значимых событий
        conn.execute('''CREATE TABLE IF NOT EXISTS events 
            (id INTEGER PRIMARY KEY, event_name TEXT, reminder_text TEXT, dt TEXT)''')
        # Таблица Custom
        conn.execute('''CREATE TABLE IF NOT EXISTS custom_tasks 
            (id INTEGER PRIMARY KEY, text TEXT, dt TEXT, period TEXT, last_sent TEXT)''')
        conn.commit()

# --- ЛОГИКА БОТА ---
async def send_telegram_msg(text):
    try:
        await bot.send_message(CHAT_ID, text)
    except Exception as e:
        print(f"Ошибка отправки в TG: {e}")

def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro)

def scheduler_job():
    now = datetime.now(MSK)
    now_day_month = now.strftime("%d.%m")
    now_full = now.strftime("%d.%m.%y %H:%M") # ДД.ММ.ГГ ЧЧ:ММ

    with get_db() as conn:
        # 1. Рассылка ДР (в 09:00 МСК)
        if now.hour == 9 and now.minute == 0:
            users = conn.execute("SELECT * FROM birthdays WHERE bday = ?", (now_day_month,)).fetchall()
            if users:
                msg = "🎉🫶🏼Сегодня день рождения наших коллег:\n"
                for u in users:
                    msg += f"{u['full_name']}, {u['pos']}, {u['dep']}\n"
                msg += "Поздравляем 😊🎊"
                run_async(send_telegram_msg(msg))

        # 2. Значимые события
        evs = conn.execute("SELECT * FROM events WHERE dt = ?", (now_full,)).fetchall()
        for e in evs:
            run_async(send_telegram_msg(f"💡 {e['reminder_text']}"))

        # 3. Custom уведомления
        customs = conn.execute("SELECT * FROM custom_tasks WHERE dt = ?", (now_full,)).fetchall()
        for c in customs:
            run_async(send_telegram_msg(c['text']))
            # Если периодично - высчитываем следующую дату (упрощенно)
            if c['period'] != 'Единоразово':
                # Здесь можно добавить логику обновления dt в БД на +1 день/неделю/и т.д.
                pass

# --- WEB-ИНТЕРФЕЙС ---
@app.before_request
def require_login():
    if request.endpoint not in ['login', 'static'] and not session.get('logged_in'):
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        flash("Неверный пароль")
    return '''
        <form method="post" style="text-align:center;margin-top:100px;">
            <h2>Вход в панель управления</h2>
            <input type="password" name="password" placeholder="Пароль">
            <button type="submit">Войти</button>
        </form>
    '''

@app.route('/')
def index():
    with get_db() as conn:
        b = conn.execute("SELECT * FROM birthdays").fetchall()
        e = conn.execute("SELECT * FROM events").fetchall()
        c = conn.execute("SELECT * FROM custom_tasks").fetchall()
    return render_template('index.html', birthdays=b, events=e, customs=c)

@app.route('/upload_dr', methods=['POST'])
def upload_dr():
    file = request.files.get('file')
    if file:
        try:
            df = pd.read_excel(file) if file.filename.endswith('.xlsx') else pd.read_csv(file)
            with get_db() as conn:
                conn.execute("DELETE FROM birthdays")
                for _, row in df.iterrows():
                    # Ожидаем порядок: Фамилия Имя, Должность, Подразделение, ДД.ММ
                    conn.execute("INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)", 
                                 (str(row[0]), str(row[1]), str(row[2]), str(row[3])))
                conn.commit()
            flash("Список ДР успешно обновлен!")
        except Exception as e:
            flash(f"Ошибка: {e}")
    return redirect(url_for('index'))

@app.route('/upload_zs', methods=['POST'])
def upload_zs():
    file = request.files.get('file')
    if file:
        try:
            df = pd.read_excel(file) if file.filename.endswith('.xlsx') else pd.read_csv(file)
            with get_db() as conn:
                conn.execute("DELETE FROM events")
                for _, row in df.iterrows():
                    # Ожидаем: Событие, Напоминание, ДД.ММ.ГГ ЧЧ:ММ
                    conn.execute("INSERT INTO events (event_name, reminder_text, dt) VALUES (?,?,?)", 
                                 (str(row[0]), str(row[1]), str(row[2])))
                conn.commit()
            flash("Значимые события обновлены!")
        except Exception as e:
            flash(f"Ошибка: {e}")
    return redirect(url_for('index'))

@app.route('/add_custom', methods=['POST'])
def add_custom():
    text = request.form.get('text')
    dt = request.form.get('dt') # Ожидается формат из HTML: YYYY-MM-DDTHH:MM
    period = request.form.get('period')
    
    # Преобразуем формат даты для БД
    clean_dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M').strftime('%d.%m.%y %H:%M')
    
    with get_db() as conn:
        conn.execute("INSERT INTO custom_tasks (text, dt, period) VALUES (?,?,?)", (text, clean_dt, period))
        conn.commit()
    return redirect(url_for('index'))

if __name__ == "__main__":
    init_db()
    scheduler = BackgroundScheduler(timezone=MSK)
    scheduler.add_job(scheduler_job, 'interval', minutes=1)
    scheduler.start()
    app.run(host='0.0.0.0', port=80)
