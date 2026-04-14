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
    print("Инициализация базы данных...")
    with get_db_connection() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS birthdays 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, full_name TEXT, pos TEXT, dep TEXT, bday TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS events 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, event_name TEXT, reminder_text TEXT, dt TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS custom_tasks 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, dt TEXT, period TEXT)''')
        conn.commit()

# --- ПЛАНИРОВЩИК И БОТ ---
async def send_to_tg(text):
    try:
        await bot.send_message(CHAT_ID, text)
        print(f"Сообщение отправлено в TG: {text[:30]}...")
    except Exception as e:
        print(f"Ошибка отправки в TG: {e}")

def check_and_send():
    now = datetime.now(MSK)
    now_dm = now.strftime("%d.%m")
    now_full = now.strftime("%d.%m.%y %H:%M")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    with get_db_connection() as conn:
        # 1. Дни рождения (09:00 МСК)
        if now.hour == 9 and now.minute == 0:
            users = conn.execute("SELECT * FROM birthdays WHERE bday = ?", (now_dm,)).fetchall()
            if users:
                msg = "🎉🫶🏼Сегодня день рождения наших коллег:\n"
                for u in users:
                    msg += f"• {u['full_name']}, {u['pos']}, {u['dep']}\n"
                msg += "Поздравляем 😊🎊"
                loop.run_until_complete(send_to_tg(msg))

        # 2. Значимые события
        evs = conn.execute("SELECT * FROM events WHERE dt = ?", (now_full,)).fetchall()
        for e in evs:
            loop.run_until_complete(send_to_tg(f"💡 {e['reminder_text']}"))

        # 3. Custom напоминания
        custs = conn.execute("SELECT * FROM custom_tasks WHERE dt = ?", (now_full,)).fetchall()
        for c in custs:
            loop.run_until_complete(send_to_tg(c['text']))
    loop.close()

# --- FLASK ROUTES ---
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
        df = pd.DataFrame(columns=['ФИО', 'Должность', 'Подразделение', 'Дата (ДД.ММ)'])
        df.loc[0] = ['Иванов Иван', 'Менеджер', 'ИТ', '14.04']
        name = "template_DR.xlsx"
    else:
        df = pd.DataFrame(columns=['Событие', 'Текст напоминания', 'Дата (ДД.ММ.ГГ ЧЧ:ММ)'])
        df.loc[0] = ['Событие 1', '💡 Не забудьте отчет', '14.04.26 15:30']
        name = "template_ZS.xlsx"
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=name)

@app.route('/upload_dr', methods=['POST'])
def upload_dr():
    print("Запрос на загрузку ДР получен")
    file = request.files.get('file')
    if not file or file.filename == '':
        flash("Файл не выбран")
        return redirect(url_for('index'))
    try:
        df = pd.read_excel(file, engine='openpyxl').dropna(how='all')
        with get_db_connection() as conn:
            conn.execute("DELETE FROM birthdays")
            for _, r in df.iterrows():
                conn.execute("INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)", 
                             (str(r.iloc[0]), str(r.iloc[1]), str(r.iloc[2]), str(r.iloc[3])))
            conn.commit()
        flash("Список ДР успешно обновлен!")
    except Exception as e:
        flash(f"Ошибка загрузки: {str(e)}")
        print(f"Ошибка при импорте ДР: {e}")
    return redirect(url_for('index'))

@app.route('/upload_zs', methods=['POST'])
def upload_zs():
    file = request.files.get('file')
    if not file or file.filename == '':
        flash("Файл не выбран")
        return redirect(url_for('index'))
    try:
        df = pd.read_excel(file, engine='openpyxl').dropna(how='all')
        with get_db_connection() as conn:
            conn.execute("DELETE FROM events")
            for _, r in df.iterrows():
                conn.execute("INSERT INTO events (event_name, reminder_text, dt) VALUES (?,?,?)", 
                             (str(r.iloc[0]), str(r.iloc[1]), str(r.iloc[2])))
            conn.commit()
        flash("Значимые события обновлены!")
    except Exception as e:
        flash(f"Ошибка загрузки ЗС: {str(e)}")
    return redirect(url_for('index'))

@app.route('/add_custom', methods=['POST'])
def add_custom():
    try:
        text = request.form.get('text')
        dt_raw = request.form.get('dt')
        period = request.form.get('period')
        dt_obj = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M')
        dt_final = dt_obj.strftime('%d.%m.%y %H:%M')
        with get_db_connection() as conn:
            conn.execute("INSERT INTO custom_tasks (text, dt, period) VALUES (?,?,?)", (text, dt_final, period))
            conn.commit()
        flash("Уведомление добавлено")
    except Exception as e:
        flash(f"Ошибка: {str(e)}")
    return redirect(url_for('index'))

# --- ЗАПУСК ---
init_db()
scheduler = BackgroundScheduler(timezone=MSK)
scheduler.add_job(check_and_send, 'interval', minutes=1)
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
