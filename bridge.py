"""Мост (модуль внутри remind): Telegram-группа -> групповой чат публичного eXpress через веб-версию corp.express.

Один процесс asyncio:
  * aiogram читает Telegram-группу (long polling, сообщения обрабатываются строго по очереди);
  * Playwright держит открытую веб-версию eXpress с постоянным профилем в /data;
  * aiohttp отдаёт защищённую паролем страницу статуса с живым скриншотом (для входа по QR).
"""
import asyncio, base64, hmac, logging, os, re, tempfile, time
from collections import deque
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.types import Message
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

VERSION = "2026.11.25-1"
JS_HEAP_MB = int(os.getenv("JS_HEAP_MB", "384"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bridge")
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

TG_TOKEN = os.environ.get("TG_BOT_TOKEN") or os.environ.get("TOKEN", "")   # по умолчанию бот remind
TG_CHAT_ID = int(os.environ.get("TG_CHAT_ID", "0") or 0)        # 0 = только логировать id чатов
TG_API_BASE = os.environ.get("TG_API_BASE", "").rstrip("/")      # необязательный прокси Bot API
EX_CHAT = os.environ.get("EXPRESS_CHAT_NAME", "").strip()        # пусто = мост выключен
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
DATA = Path(os.environ.get("DATA_DIR", "/data"))
MAX_FILE = int(os.environ.get("MAX_FILE_MB", "20")) * 1024 * 1024
WEB_URL = "https://corp.express/"

S_INPUT = ".message-input__message-field--main-chat .slate-message-input"
S_DOC = "input[type=file][id^=document-input]"
S_IMG = "input[type=file][id^=image-input]"
S_ENTRY = ".chat-list-entry"
S_ENTRY_NAME = ".chat-list-entry__name"

state = {"browser": "запуск", "logged_in": False, "sent": 0, "failed": 0,
         "last_ok": None, "last_error": None, "telegram": "запуск", "started": time.time()}
events: deque = deque(maxlen=50)


def ev(msg: str, level=logging.INFO):
    log.log(level, msg)
    events.appendleft(f"{time.strftime('%d.%m %H:%M:%S')}  {msg}")


class Express:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.page = None
        self.ctx = None
        self.pw = None
        self.proc = None
        self.browser = None

    async def start(self):
        """Chromium запускается отдельным процессом с низким приоритетом (nice 15) и
        урезанной памятью, чтобы не душить Flask и напоминания; Playwright цепляется по CDP."""
        import subprocess, socket
        self.pw = self.pw or await async_playwright().start()
        profile = DATA / "chrome-profile"
        profile.mkdir(parents=True, exist_ok=True)
        for f in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            (profile / f).unlink(missing_ok=True)     # остатки после перезапуска контейнера
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
        with socket.socket() as sk:
            sk.bind(("127.0.0.1", 0)); port = sk.getsockname()[1]
        cmd = ["nice", "-n", "15", self.pw.chromium.executable_path, "--headless=new",
               f"--remote-debugging-port={port}", f"--user-data-dir={profile}",
               "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
               "--disable-extensions", "--disable-background-networking", "--disable-sync",
               "--disable-component-update", "--disable-default-apps", "--no-first-run",
               "--mute-audio", "--renderer-process-limit=1", "--no-zygote",
               "--disable-features=site-per-process,Translate,MediaRouter,OptimizationHints",
               f"--js-flags=--max-old-space-size={JS_HEAP_MB}",
               "--window-size=1280,800", "--lang=ru-RU", "about:blank"]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:   # при нехватке памяти ядро должно убить Chromium, а не процесс с напоминаниями
            Path(f"/proc/{self.proc.pid}/oom_score_adj").write_text("1000")
        except OSError:
            pass
        for _ in range(60):
            try:
                self.browser = await self.pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
                break
            except Exception:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"Chromium завершился с кодом {self.proc.returncode}")
                await asyncio.sleep(1)
        else:
            raise RuntimeError("Chromium не ответил за 60 с")
        self.ctx = self.browser.contexts[0]
        self.page = self.ctx.pages[0] if self.ctx.pages else await self.ctx.new_page()
        state["browser"] = "работает"
        ev("браузер запущен")
        await self.ensure_page()

    async def ensure_page(self) -> bool:
        """Открывает corp.express, если вкладка ещё не там; ошибки сети не роняют мост."""
        if "corp.express" in (self.page.url or ""):
            return True
        t0 = time.time()
        try:
            await self.page.goto(WEB_URL, wait_until="commit", timeout=120000)
            await self.page.wait_for_load_state("domcontentloaded", timeout=120000)
            ev(f"corp.express открыт за {time.time() - t0:.1f} с")
            return True
        except Exception as e:
            ev(f"corp.express не открылся за {time.time() - t0:.0f} с: {str(e).splitlines()[0]}", logging.ERROR)
            state["last_error"] = f"{time.strftime('%H:%M:%S')} corp.express недоступен"
            return False

    async def check_login(self) -> bool:
        p = self.page
        if not await self.ensure_page():
            state["logged_in"] = False
            return False
        try:
            await p.locator(f"{S_ENTRY}, .slate-message-input, canvas, img[src*=qr]").or_(
                p.get_by_text(re.compile("QR"))).first.wait_for(timeout=20000)
        except PWTimeout:
            pass
        ok = await p.locator(S_ENTRY).count() > 0 or await p.locator(".slate-message-input").count() > 0
        if ok != state["logged_in"]:
            ev("вход в eXpress выполнен" if ok else "нужен вход в eXpress по QR-коду",
               logging.INFO if ok else logging.WARNING)
        state["logged_in"] = ok
        return ok

    async def open_chat(self):
        p = self.page
        active = p.locator(f"{S_ENTRY}.chat-list-entry--active {S_ENTRY_NAME}")
        if await active.count() and (await active.first.inner_text()).strip() == EX_CHAT:
            return
        entry = p.locator(S_ENTRY).filter(has=p.locator(S_ENTRY_NAME, has_text=re.compile(
            rf"^\s*{re.escape(EX_CHAT)}\s*$")))
        if not await entry.count():
            raise RuntimeError(f"чат «{EX_CHAT}» не найден в списке чатов")
        await entry.first.click()
        await p.wait_for_selector(S_INPUT, timeout=15000)

    async def _type(self, text: str):
        p = self.page
        box = p.locator(S_INPUT).first
        await box.click()
        lines = text.split("\n")
        for i, line in enumerate(lines):
            if line:
                await p.keyboard.insert_text(line)
            if i < len(lines) - 1:
                await p.keyboard.press("Shift+Enter")

    async def _composer_empty(self) -> bool:
        # innerText содержит подсказку «Введите сообщение», поэтому берём только строки Slate
        txt = await self.page.locator(S_INPUT).first.evaluate(
            "e => [...e.querySelectorAll('[data-slate-string]')].map(x => x.textContent).join('')")
        has_file = await self.page.locator(".input-attachment__file").count()
        return txt.replace("\ufeff", "").strip() == "" and not has_file

    async def _last_outgoing(self) -> str:
        return await self.page.evaluate(
            "() => { const m = [...document.querySelectorAll('.chat-message')]; "
            "return m.length ? m[m.length - 1].innerText : ''; }")

    async def send(self, text: str, file_path: str | None = None, is_photo=False):
        marker = next((l for l in (text or "").split("\n")[::-1] if l.strip()), "")[:60]
        async with self.lock:
            for attempt in range(1, 4):
                try:
                    while not await self.check_login():       # ждём, пока войдут по QR
                        await asyncio.sleep(30)
                    await self.open_chat()
                    p = self.page
                    if attempt > 1 and marker and marker in await self._last_outgoing():
                        ev("сообщение уже доставлено предыдущей попыткой — повтор пропущен")
                        state["sent"] += 1
                        state["last_ok"] = time.time()
                        return
                    if not await self._composer_empty():          # остатки прошлой попытки
                        for b in await p.locator(".input-attachment__file button").all():
                            await b.click()
                        await p.locator(S_INPUT).first.click()
                        await p.keyboard.press("Control+A")
                        await p.keyboard.press("Delete")
                    if file_path:
                        await p.locator(S_IMG if is_photo else S_DOC).first.set_input_files(file_path)
                        await p.wait_for_timeout(1500)
                    await self._type(text or " ")
                    await p.keyboard.press("Enter")
                    for _ in range(30 if file_path else 10):
                        await p.wait_for_timeout(500)
                        if await self._composer_empty():
                            break
                    else:
                        raise RuntimeError("сообщение осталось в поле ввода")
                    state["sent"] += 1
                    state["last_ok"] = time.time()
                    return
                except Exception as e:
                    state["last_error"] = f"{time.strftime('%H:%M:%S')} {e}"
                    ev(f"попытка {attempt}: ошибка отправки: {e}", logging.ERROR)
                    try:
                        await self.page.reload(wait_until="commit", timeout=120000)
                    except Exception:
                        pass
                    await asyncio.sleep(5 * attempt)
            state["failed"] += 1
            raise RuntimeError("не удалось отправить после 3 попыток")

    async def watchdog(self):
        while True:
            await asyncio.sleep(120)
            if self.lock.locked():
                continue
            async with self.lock:
                try:
                    if self.proc and self.proc.poll() is not None:
                        ev(f"Chromium завершился (код {self.proc.returncode}), перезапуск", logging.ERROR)
                        state["browser"] = "перезапуск"
                        await self.start()
                    await self.check_login()
                except Exception as e:
                    ev(f"watchdog: {e}", logging.ERROR)


ex = Express()
bot = None
dp = Dispatcher()
LOOP: asyncio.AbstractEventLoop | None = None


def author(m: Message) -> str:
    if m.sender_chat:
        return m.sender_chat.title or "Канал"
    u = m.from_user
    return " ".join(x for x in (u.first_name, u.last_name) if x) or (u.username or "Без имени")


def media(m: Message):
    if m.photo:
        return m.photo[-1], f"photo_{m.message_id}.jpg", True
    for kind, ext in (("document", ""), ("video", ".mp4"), ("audio", ".mp3"),
                      ("voice", ".ogg"), ("video_note", ".mp4"), ("animation", ".mp4")):
        obj = getattr(m, kind)
        if obj:
            return obj, getattr(obj, "file_name", None) or f"{kind}_{m.message_id}{ext}", False
    return None, None, False


@dp.message()
@dp.channel_post()
async def on_message(m: Message):
    if m.chat.id != TG_CHAT_ID:
        ev(f"сообщение из чата {m.chat.id} «{m.chat.title}» — не настроен для пересылки")
        return
    head = f"{author(m)}:"
    if m.forward_origin:
        head = f"{author(m)} (переслано):"
    if m.reply_to_message:
        q = (m.reply_to_message.text or m.reply_to_message.caption or "[вложение]").replace("\n", " ")
        head += f"\n» {q[:120]}"
    body = m.text or m.caption or ""
    obj, name, is_photo = media(m)
    path = None
    try:
        if obj:
            if (obj.file_size or 0) > MAX_FILE:
                body += f"\n[файл {name} больше {MAX_FILE // 1048576} МБ — не передан]"
            else:
                tmp = Path(tempfile.mkdtemp())
                path = str(tmp / re.sub(r"[^\w.\-() ]", "_", name))
                await bot.download(obj, destination=path)
        elif not body:
            body = "[стикер, опрос или другой тип сообщения — не поддерживается]"
        await ex.send(f"{head}\n{body}".strip(), path, is_photo)
        ev(f"переслано сообщение {m.message_id} от {author(m)}")
    except Exception as e:
        ev(f"сообщение {m.message_id} не переслано: {e}", logging.ERROR)
    finally:
        if path:
            Path(path).unlink(missing_ok=True)




# ---------- запуск в фоне и панель /bridge для Flask ----------
async def _main():
    global bot
    session = AiohttpSession(api=TelegramAPIServer.from_base(TG_API_BASE)) if TG_API_BASE else None
    bot = Bot(TG_TOKEN, session=session) if session else Bot(TG_TOKEN)
    ev(f"мост запущен, версия {VERSION}")
    while True:
        try:
            await ex.start()
            break
        except Exception as e:
            state["browser"] = f"ошибка запуска: {str(e)[:150]}"
            ev(f"браузер не запустился, повтор через 60 с: {e}", logging.ERROR)
            await asyncio.sleep(60)
    if not await ex.check_login():
        ev("нужен вход в eXpress по QR-коду", logging.WARNING)
    asyncio.create_task(ex.watchdog())
    # handle_as_tasks=False: строго по порядку; необработанные апдейты Telegram отдаст повторно
    while True:
        try:
            state["telegram"] = "подключён"
            await dp.start_polling(bot, handle_as_tasks=False, handle_signals=False,
                                   allowed_updates=["message", "channel_post"])
        except Exception as e:
            state["telegram"] = f"ошибка: {str(e)[:150]}"
            ev(f"Telegram недоступен, повтор через 30 с: {e}", logging.ERROR)
        await asyncio.sleep(30)


def _thread():
    global LOOP
    LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(LOOP)
    try:
        LOOP.run_until_complete(_main())
    except Exception as e:
        state["browser"] = f"остановлен: {e}"
        log.exception("мост остановлен")


def start_background() -> bool:
    if not EX_CHAT or not TG_TOKEN:
        log.info("мост выключен: не заданы EXPRESS_CHAT_NAME или токен бота")
        state["browser"] = "выключен (нет EXPRESS_CHAT_NAME)"
        return False
    import threading
    threading.Thread(target=_thread, name="tg2express", daemon=True).start()
    return True


def _run(coro, timeout=60):
    if not LOOP:
        raise RuntimeError("мост не запущен")
    return asyncio.run_coroutine_threadsafe(coro, LOOP).result(timeout)


def register(flask_app):
    from flask import Response, jsonify, redirect, request

    def need_auth():
        """Тот же вход, что у напоминаний (сессия Flask); Basic оставлен для внешних проверок."""
        from flask import session
        if session.get("logged_in"):
            return None
        a = request.authorization
        if ADMIN_PASSWORD and a and hmac.compare_digest(a.password or "", ADMIN_PASSWORD):
            return None
        return Response("Нужен вход", 401)

    @flask_app.get("/bridge/health")
    def bridge_health():
        return jsonify(status="ok", version=VERSION, logged_in=state["logged_in"],
                       browser=state["browser"], telegram=state["telegram"],
                       sent=state["sent"], failed=state["failed"])

    @flask_app.get("/bridge")
    @flask_app.get("/bridge/")
    def bridge_index():
        return redirect("/#bridge")

    @flask_app.get("/bridge/status")
    def bridge_status():
        if (r := need_auth()):
            return r
        fmt = lambda t: time.strftime("%d.%m %H:%M:%S", time.localtime(t)) if t else "—"
        return jsonify(version=VERSION, enabled=bool(EX_CHAT and TG_TOKEN), browser=state["browser"],
                       telegram=state["telegram"], logged_in=state["logged_in"],
                       express_chat=EX_CHAT, tg_chat_id=TG_CHAT_ID, sent=state["sent"],
                       failed=state["failed"], last_ok=fmt(state["last_ok"]),
                       last_error=state["last_error"], events=list(events))

    @flask_app.get("/bridge/screenshot.png")
    def bridge_shot():
        if (r := need_auth()):
            return r
        if not ex.page:
            return Response("браузер не готов", 503)
        png = _run(ex.page.screenshot(type="png"), 20)
        return Response(png, mimetype="image/png", headers={"Cache-Control": "no-store"})

    @flask_app.post("/bridge/reload")
    def bridge_reload():
        if (r := need_auth()):
            return r
        async def go():
            async with ex.lock:
                try:
                    await ex.page.goto(WEB_URL, wait_until="commit", timeout=120000)
                except Exception as e:
                    ev(f"перезагрузка: {e}", logging.ERROR)
                await ex.check_login()
        try:
            _run(go(), 280)
        except Exception as e:
            return jsonify(ok=False, error=str(e)), 500
        return jsonify(ok=True)
