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
        
        # Миграции (Золотой стандарт стабильности)
        cursor = conn.execute("PRAGMA table_info(events)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'event_name' not in cols: conn.execute("ALTER TABLE events ADD COLUMN event_name TEXT")
        if 'reminder_text' not in cols: conn.execute("ALTER TABLE events ADD COLUMN reminder_text TEXT")
        
        cursor_c = conn.execute("PRAGMA table_info(custom_tasks)")
        cols_c = [row[1] for row in cursor_c.fetchall()]
        if 'weekdays' not in cols_c: conn.execute("ALTER TABLE custom_tasks ADD COLUMN weekdays TEXT")
        conn.commit()

# --- ЛОГИКА ОПОВЕЩЕНИЙ ---
async def send_to_tg(text):
    try:
        await bot.send_message(CHAT_ID, text)
    except Exception as e:
        print(f"Ошибка TG: {e}")

def check_and_send():
    now = datetime.now(MSK)
    now_dm = now.strftime("%d.%m")              
    now_zs = now.strftime("%d.%m.%Y %H:%M:%S")   
    now_custom = now.strftime("%d.%m.%Y %H:%M")   
    now_time_hm = now.strftime("%H:%M")          
    current_weekday = str(now.weekday())

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    with get_db_connection() as conn:
        # 1. Дни Рождения - 09:00:00 (проверка ДД.ММ)
        if now.hour == 9 and now.minute == 0 and now.second == 0:
            users = conn.execute("SELECT * FROM birthdays").fetchall()
            celebrants = [u for u in users if str(u['bday']).strip().startswith(now_dm)]
            if celebrants:
                msg = "🎉🫶🏼Сегодня день рождения наших коллег:\n"
                for u in celebrants:
                    msg += f"• {u['full_name']}, {u['pos']}, {u['dep']}\n"
                msg += "Поздравляем 😊🎊"
                loop.run_until_complete(send_to_tg(msg))

        # 2. Значимые события (ЗС) - Посекундная проверка
        evs = conn.execute("SELECT * FROM events WHERE dt = ?", (now_zs,)).fetchall()
        for e in evs:
            loop.run_until_complete(send_to_tg(f"💡 {e['reminder_text']}"))

        # 3. Custom уведомления
        if now.second == 0:
            tasks = conn.execute("SELECT * FROM custom_tasks").fetchall()
            for t in tasks:
                t_dt_str = str(t['dt']).strip()
                t_time = t_dt_str.split(' ')[1] if ' ' in t_dt_str else ""
                if t['period'] == 'once' and t_dt_str == now_custom:
                    loop.run_until_complete(send_to_tg(t['text']))
                elif t['period'] == 'weekdays' and t_time == now_time_hm:
                    allowed = t['weekdays'].split(',') if t['weekdays'] else []
                    if current_weekday in allowed:
                        loop.run_until_complete(send_to_tg(t['text']))
    loop.close()

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: НОРМАЛИЗАЦИЯ ДАТ ---
def normalize_date(val, include_seconds=True):
    if pd.isna(val): return ""
    try:
        if isinstance(val, datetime):
            dt_obj = val
        else:
            val_str = str(val).strip()
            # Пробуем форматы: ГГГГ-ММ-ДД и ДД.ММ.ГГГГ
            for fmt in ["%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%d"]:
                try:
                    dt_obj = datetime.strptime(val_str, fmt)
                    break
                except: dt_obj = None
        
        if not dt_obj: return str(val).strip()
        if include_seconds: return dt_obj.strftime("%d.%m.%Y %H:%M:%S")
        return dt_obj.strftime("%d.%m.%Y")
    except: return str(val).strip()

# --- WEB ROUTES ---
@app.route('/')
def index():
    if not session.get('logged_in'): return redirect(url_for('login'))
    with get_db_connection() as conn:
        b = conn.execute("SELECT * FROM birthdays").fetchall()
        e = conn.execute("SELECT * FROM events").fetchall()
        c = conn.execute("SELECT * FROM custom_tasks").fetchall()
    return render_template('index.html', bdays=b, evs=e, customs=c)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
    return '<html><body style="text-align:center;padding-top:100px;font-family:sans-serif;"><h2>Вход</h2><form method="post"><input type="password" name="password" style="padding:8px;"><button style="padding:8px 15px;margin-left:5px;">Войти</button></form></body></html>'

@app.route('/upload_dr', methods=['POST'])
def upload_dr():
    file = request.files.get('file')
    if file:
        try:
            df = pd.read_excel(file, engine='openpyxl').dropna(how='all')
            with get_db_connection() as conn:
                conn.execute("DELETE FROM birthdays")
                for _, r in df.iterrows():
                    clean_bday = normalize_date(r.iloc[3], False)
                    conn.execute("INSERT INTO birthdays (full_name, pos, dep, bday) VALUES (?,?,?,?)", 
                                 (str(r.iloc[0]).strip(), str(r.iloc[1]).strip(), str(r.iloc[2]).strip(), clean_bday))
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
                    clean_dt = normalize_date(r.iloc[2], True)
                    conn.execute("INSERT INTO events (event_name, reminder_text, dt) VALUES (?,?,?)", 
                                 (str(r.iloc[0]).strip(), str(r.iloc[1]).strip(), clean_dt))
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
        dt_final = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M').strftime('%d.%m.%Y %H:%M')
        with get_db_connection() as conn:
            conn.execute("INSERT INTO custom_tasks (text, dt, period, weekdays) VALUES (?,?,?,?)", 
                         (text, dt_final, 'weekdays' if period != 'once' else 'once', ",".join(days)))
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

@app.route('/download_template/<t_type>')
def download_template(t_type):
    output = io.BytesIO()
    if t_type == 'dr':
        df = pd.DataFrame(columns=['ФИО', 'Должность', 'Отдел', 'Дата (ДД.ММ.ГГГГ)'])
        df.loc[0] = ['Иванов Иван', 'Менеджер', 'ИТ', '14.04.1990']
    else:
        df = pd.DataFrame(columns=['Событие', 'Текст напоминания', 'Дата (ДД.ММ.ГГГГ ЧЧ:ММ:СС)'])
        df.loc[0] = ['Событие 1', '💡 Пора на встречу', '14.04.2026 12:46:00']
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"template_{t_type}.xlsx")

init_db()
scheduler = BackgroundScheduler(timezone=MSK)
scheduler.add_job(check_and_send, 'interval', seconds=1)
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
