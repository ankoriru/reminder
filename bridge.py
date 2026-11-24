"""Мост (модуль внутри remind): Telegram-группа -> групповой чат публичного eXpress через веб-версию corp.express.

Один процесс asyncio:
  * aiogram читает Telegram-группу (long polling, сообщения обрабатываются строго по очереди);
  * Playwright держит открытую веб-версию eXpress с постоянным профилем в /data;
  * aiohttp отдаёт защищённую паролем страницу статуса с живым скриншотом (для входа по QR).
"""
import asyncio, base64, hmac, json, logging, os, re, tempfile, time
from collections import OrderedDict, deque
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.types import Message
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

VERSION = "2026.11.25-8"
IDLE_SEC = int(os.getenv("BROWSER_IDLE_SEC", "120"))   # браузер закрывается после простоя
JS_HEAP_MB = int(os.getenv("JS_HEAP_MB", "256"))
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
DEL_CHECK_SEC = int(os.getenv("DELETE_CHECK_SEC", "300"))     # как часто проверять удаления в Telegram
DEL_WINDOW_H = int(os.getenv("DELETE_WINDOW_HOURS", "48"))    # за какой срок отслеживать удаления
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


def _browser_exe(pw) -> str:
    """chrome-headless-shell вдвое легче полного Chromium; если его нет в образе — обычный."""
    import glob
    roots = [os.getenv("PLAYWRIGHT_BROWSERS_PATH", ""), "/ms-playwright",
             str(Path.home() / ".cache/ms-playwright")]
    for r in roots:
        found = sorted(glob.glob(f"{r}/chromium_headless_shell-*/chrome-headless-shell-linux64/chrome-headless-shell")) if r else []
        if found:
            return found[-1]
    return pw.chromium.executable_path


class Express:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.page = None
        self.ctx = None
        self.pw = None
        self.proc = None
        self.browser = None
        self.last_used = 0.0

    async def start(self):
        """Chromium запускается отдельным процессом с низким приоритетом (nice 15) и
        урезанной памятью, чтобы не душить Flask и напоминания; Playwright цепляется по CDP."""
        import subprocess, socket
        if not self.pw:
            self.pw = await async_playwright().start()
            _renice_children()   # драйвер Playwright (node) тоже на низкий приоритет
        profile = DATA / "chrome-profile"
        profile.mkdir(parents=True, exist_ok=True)
        for f in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            (profile / f).unlink(missing_ok=True)     # остатки после перезапуска контейнера
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
        with socket.socket() as sk:
            sk.bind(("127.0.0.1", 0)); port = sk.getsockname()[1]
        cmd = ["nice", "-n", "15", _browser_exe(self.pw), "--headless",
               f"--remote-debugging-port={port}", f"--user-data-dir={profile}",
               "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
               "--disable-extensions", "--disable-background-networking", "--disable-sync",
               "--disable-component-update", "--disable-default-apps", "--no-first-run",
               "--mute-audio", "--single-process", "--enable-low-end-device-mode",
               "--disable-features=site-per-process,Translate,MediaRouter,OptimizationHints",
               f"--js-flags=--max-old-space-size={JS_HEAP_MB}",
               "--window-size=900,700", "--lang=ru-RU", "about:blank"]
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
        try:   # блокировка внутри Chromium, без пересылки каждого запроса в Python
            cdp = await self.ctx.new_cdp_session(self.page)
            await cdp.send("Network.enable")
            await cdp.send("Network.setBlockedURLs", {"urls": [
                "*.ttf*", "*.woff*", "*.otf*", "*sentry*", "*.mp4*", "*.mp3*", "*.webm*", "*.ogg*"]})
        except Exception as e:
            ev(f"не удалось включить блокировку ресурсов: {e}", logging.WARNING)
        state["browser"] = "работает"
        ev("браузер запущен")
        await self.ensure_page()

    def running(self) -> bool:
        return bool(self.proc and self.proc.poll() is None and self.page)

    async def awake(self):
        """Браузер поднимается только когда нужен: в простое его нет, памяти и CPU он не ест."""
        self.last_used = time.time()
        if not self.running():
            state["browser"] = "запуск"
            await self.start()
        self.last_used = time.time()

    async def sleep(self, reason=""):
        if not self.proc:
            return
        try:
            cdp = await self.browser.new_browser_cdp_session()
            await cdp.send("Browser.close")            # штатное закрытие, профиль сохраняется
        except Exception:
            pass
        for _ in range(30):
            if self.proc.poll() is not None:
                break
            await asyncio.sleep(0.5)
        else:
            self.proc.kill()
        self.proc = self.page = self.ctx = self.browser = None
        state["browser"] = "спит (запустится при новом сообщении)"
        ev(f"браузер закрыт{reason}")

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
        if ok:
            try:
                await self.dismiss_popups()
            except Exception as e:
                ev(f"не удалось закрыть окно eXpress: {e}", logging.WARNING)
        return ok

    async def dismiss_popups(self):
        """Закрывает рекламные и информационные окна eXpress («Ваши возможности в eXpress» и т. п.)."""
        p = self.page
        for _ in range(3):
            btn = p.get_by_text(re.compile(r"^\s*(Больше не показывать|Don't show again|Не сейчас|Понятно|Закрыть)\s*$"))
            if await btn.count() and await btn.first.is_visible():
                await btn.first.click(timeout=5000)
                ev("закрыто всплывающее окно eXpress")
                await p.wait_for_timeout(500)
                continue
            dlg = p.locator("[role=dialog], .modal, [class*=modal]").filter(has_text=re.compile("eXpress"))
            if await dlg.count() and await dlg.first.is_visible():
                await p.keyboard.press("Escape")
                await p.wait_for_timeout(500)
                if await dlg.count() and await dlg.first.is_visible():
                    close = dlg.first.locator("button, [class*=close]").filter(has_not_text=re.compile(r"\w"))
                    if await close.count():
                        await close.first.click(timeout=5000)
                        await p.wait_for_timeout(500)
                continue
            break

    async def open_chat(self):
        p = self.page
        await self.dismiss_popups()
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

    async def _last_my_id(self) -> str | None:
        return await self.page.evaluate(
            "() => { const m = [...document.querySelectorAll('.chat-message')]"
            ".filter(e => e.querySelector('.chat-message__bubble--my')); "
            "return m.length ? m[m.length - 1].id || null : null; }")

    async def edit(self, ex_id: str, text: str) -> bool:
        """Правит своё сообщение в eXpress по его id. False — сообщение не найдено или правка не удалась."""
        if not state["logged_in"]:
            return False
        async with self.lock:
            try:
                await self.awake()
                if not await self.check_login():
                    return False
                await self.open_chat()
                p = self.page
                msg = await self._find_my(ex_id)
                if msg is None:
                    ev(f"сообщение {ex_id} не найдено в чате eXpress", logging.WARNING)
                    return False
                await msg.locator(".chat-message__bubble").first.click(button="right")
                await p.wait_for_timeout(600)
                item = p.locator(".message-menu__root-item").filter(
                    has_text=re.compile(r"^\s*(Edit|Изменить|Редактировать)\s*$"))
                if not await item.count():
                    await p.keyboard.press("Escape")
                    ev("в меню сообщения нет пункта «Изменить»", logging.WARNING)
                    return False
                await item.first.evaluate("e => e.click()")   # меню бывает за краем окна
                await p.wait_for_timeout(800)
                box = p.locator(S_INPUT).first
                await box.click()
                await p.keyboard.press("Control+A")
                await p.keyboard.press("Delete")
                await self._type(text or " ")
                await p.keyboard.press("Enter")
                want = next((l for l in text.split("\n")[::-1] if l.strip()), "")[:60]
                for _ in range(16):
                    await p.wait_for_timeout(500)
                    if want in (await msg.inner_text()) and await self._composer_empty():
                        self.last_used = time.time()
                        return True
                raise RuntimeError("текст сообщения не обновился")
            except Exception as e:
                state["last_error"] = f"{time.strftime('%H:%M:%S')} правка: {e}"
                ev(f"ошибка правки {ex_id}: {e}", logging.ERROR)
                try:
                    await self.page.reload(wait_until="commit", timeout=120000)
                except Exception:
                    pass
                return False

    async def _find_my(self, ex_id: str):
        p = self.page
        msg = p.locator(f'.chat-message[id="{ex_id}"]')
        for _ in range(15):                 # старые сообщения подгружаются при прокрутке вверх
            if await msg.count():
                break
            await p.evaluate("() => { const c = document.querySelector('.chat-message')?.parentElement; "
                             "let e = c; while (e && e.scrollHeight <= e.clientHeight) e = e.parentElement; "
                             "if (e) e.scrollTop = 0; }")
            await p.wait_for_timeout(1200)
        if not await msg.count() or await msg.first.locator(".chat-message__bubble--my").count() == 0:
            return None
        await msg.first.scroll_into_view_if_needed()
        return msg.first

    async def delete(self, ex_id: str) -> bool | None:
        """Удаляет своё сообщение в eXpress у всех по его id. None — сообщение не найдено."""
        if not state["logged_in"]:
            return False
        async with self.lock:
            try:
                await self.awake()
                if not await self.check_login():
                    return False
                await self.open_chat()
                p = self.page
                msg = await self._find_my(ex_id)
                if msg is None:
                    ev(f"сообщение {ex_id} не найдено в чате eXpress (возможно, уже удалено)", logging.WARNING)
                    return None
                await msg.locator(".chat-message__bubble").first.click(button="right")
                await p.wait_for_timeout(600)
                item = p.locator(".message-menu__root-item").filter(
                    has_text=re.compile(r"^\s*(Delete|Удалить)\s*$"))
                if not await item.count():
                    await p.keyboard.press("Escape")
                    ev("в меню сообщения нет пункта «Удалить»", logging.WARNING)
                    return False
                await item.first.evaluate("e => e.click()")
                await p.wait_for_timeout(800)
                act = p.locator("span.message-input__operator--actions").filter(
                    has_text=re.compile(r"^\s*(Delete|Удалить)\s*$"))
                await act.first.click(timeout=10000)
                modal = p.locator(".ReactModal__Content")
                await modal.first.wait_for(timeout=10000)
                cb = modal.first.locator("input[type=checkbox]")
                if await cb.count() and not await cb.first.is_checked():
                    await cb.first.evaluate("e => e.click()")     # «Удалить у всех»
                await modal.first.locator(".button--contained--negative").first.click(timeout=10000)
                for _ in range(16):
                    await p.wait_for_timeout(500)
                    gone = await p.locator(f'.chat-message[id="{ex_id}"]').count() == 0
                    if gone or re.search(r"(deleted|удален)", await msg.inner_text(), re.I):
                        self.last_used = time.time()
                        return True
                raise RuntimeError("сообщение не удалилось")
            except Exception as e:
                state["last_error"] = f"{time.strftime('%H:%M:%S')} удаление: {e}"
                ev(f"ошибка удаления {ex_id}: {e}", logging.ERROR)
                try:
                    await self.page.reload(wait_until="commit", timeout=120000)
                except Exception:
                    pass
                return False

    async def send(self, text: str, file_path: str | None = None, is_photo=False) -> str | None:
        """Отправляет сообщение; возвращает id своего сообщения в eXpress (или None)."""
        marker = next((l for l in (text or "").split("\n")[::-1] if l.strip()), "")[:60]
        warned = False
        while True:                    # без входа браузер не держим: ждём, пока войдут через панель
            async with self.lock:
                await self.awake()
                if await self.check_login():
                    break
                self.last_used = 0     # пусть watchdog закроет браузер, если панель не открыта
            if not warned:
                ev("сообщение ждёт входа в eXpress: откройте вкладку моста и отсканируйте QR-код",
                   logging.WARNING)
                warned = True
            while not state["logged_in"]:
                await asyncio.sleep(30)
        async with self.lock:
            for attempt in range(1, 4):
                try:
                    await self.awake()
                    if not await self.check_login():
                        raise RuntimeError("вход в eXpress потерян")
                    await self.open_chat()
                    p = self.page
                    if attempt > 1 and marker and marker in await self._last_outgoing():
                        ev("сообщение уже доставлено предыдущей попыткой — повтор пропущен")
                        state["sent"] += 1
                        state["last_ok"] = time.time()
                        return await self._last_my_id()
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
                    state["last_ok"] = self.last_used = time.time()
                    try:
                        return await self._last_my_id()
                    except Exception:
                        return None
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
        """Раз в 30 с: закрыть браузер после простоя; пока открыта панель без входа — ловить вход по QR."""
        while True:
            await asyncio.sleep(30)
            if self.lock.locked():
                continue
            async with self.lock:
                try:
                    if self.proc and self.proc.poll() is not None:
                        ev(f"Chromium завершился (код {self.proc.returncode})", logging.ERROR)
                        self.proc = self.page = self.ctx = self.browser = None
                        state["browser"] = "спит (запустится при новом сообщении)"
                    if not self.running():
                        continue
                    if time.time() - self.last_used > IDLE_SEC:
                        await self.sleep(f" после {IDLE_SEC} с простоя")
                    elif not state["logged_in"]:
                        await self.check_login()
                except Exception as e:
                    ev(f"watchdog: {e}", logging.ERROR)


def _renice_children(nice=15):
    try:
        for d in Path("/proc").iterdir():
            if d.name.isdigit():
                try:
                    st = (d / "stat").read_text().split()
                    if int(st[3]) == os.getpid():
                        os.setpriority(os.PRIO_PROCESS, int(d.name), nice)
                except (OSError, ValueError, IndexError):
                    pass
    except OSError:
        pass


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


# соответствие сообщений Telegram -> eXpress: {tg_id: {"ex": id в eXpress, "head": заголовок, "text": текст}}
# хранится в /data, чтобы правки работали и после перезапуска
MAP_FILE = DATA / "bridge_map.json"
MAP_MAX = 1000


def _load_map() -> "OrderedDict[str, dict]":
    try:
        return OrderedDict(json.loads(MAP_FILE.read_text()))
    except Exception:
        return OrderedDict()


_map = _load_map()


def _remember(mid: int, **kw):
    k = str(mid)
    rec = _map.get(k, {})
    rec.update({a: b for a, b in kw.items() if b is not None})
    _map[k] = rec
    _map.move_to_end(k)
    while len(_map) > MAP_MAX:
        _map.popitem(last=False)
    try:
        tmp = MAP_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_map, ensure_ascii=False))
        tmp.replace(MAP_FILE)
    except Exception as e:
        log.warning(f"не удалось сохранить {MAP_FILE}: {e}")


# ---------- удаления: Bot API не сообщает боту об удалённых сообщениях ----------
# Поэтому мост раз в DEL_CHECK_SEC пробует переслать недавние сообщения в служебный чат:
# если Telegram отвечает «сообщение не найдено» — его удалили, и мост удаляет копию в eXpress.
# Пересланные копии в служебном чате сразу удаляются.
SERVICE_FILE = DATA / "bridge_service.json"


def _service_chat() -> int:
    env = int(os.getenv("TG_SERVICE_CHAT_ID", "0") or 0)
    if env:
        return env
    try:
        return int(json.loads(SERVICE_FILE.read_text())["chat_id"])
    except Exception:
        return 0


@dp.message(Command("bridge_service"))
@dp.channel_post(Command("bridge_service"))
async def on_service(m: Message):
    if m.chat.id == TG_CHAT_ID:
        await m.reply("Это основной чат. Отправьте команду в личном чате с ботом или в отдельном закрытом канале.")
        return
    SERVICE_FILE.write_text(json.dumps({"chat_id": m.chat.id}))
    ev(f"служебный чат для отслеживания удалений: {m.chat.id}")
    await m.answer("Готово: этот чат будет служебным для отслеживания удалений. "
                   "Здесь могут на секунду появляться и сразу исчезать копии сообщений.")


def _is_missing(e: Exception) -> bool:
    t = str(e).lower()
    return "not found" in t or "message_id_invalid" in t or "can't be forwarded" in t


async def _probe(service: int, ids: list[int]) -> list[int]:
    """Возвращает id сообщений, которых больше нет в Telegram."""
    gone, copies = [], []
    try:
        res = await bot.forward_messages(service, TG_CHAT_ID, ids, disable_notification=True)
        copies = [r.message_id for r in res]
        if len(copies) < len(ids):            # кого-то нет — проверяем по одному
            for i in ids:
                try:
                    r = await bot.forward_message(service, TG_CHAT_ID, i, disable_notification=True)
                    copies.append(r.message_id)
                except TelegramBadRequest as e:
                    if _is_missing(e):
                        gone.append(i)
                await asyncio.sleep(0.05)
    finally:
        for k in range(0, len(copies), 100):
            try:
                await bot.delete_messages(service, copies[k:k + 100])
            except Exception as e:
                log.warning(f"не удалось убрать копии из служебного чата: {e}")
    return gone


async def deletion_watch():
    warned = False
    while True:
        await asyncio.sleep(DEL_CHECK_SEC)
        service = _service_chat()
        if not service:
            if not warned:
                ev("удаления не отслеживаются: отправьте боту в личном чате команду /bridge_service",
                   logging.WARNING)
                warned = True
            continue
        if not state["logged_in"]:
            continue
        since = time.time() - DEL_WINDOW_H * 3600
        ids = sorted(int(k) for k, r in _map.items()
                     if r.get("ex") and not r.get("deleted") and r.get("ts", 0) >= since)
        for k in range(0, len(ids), 100):
            try:
                gone = await _probe(service, ids[k:k + 100])
            except Exception as e:
                ev(f"проверка удалений: {e}", logging.ERROR)
                break
            for i in gone:
                rec = _map[str(i)]
                ok = await ex.delete(rec["ex"])
                if ok or ok is None or rec.get("del_fail", 0) >= 2:
                    _remember(i, deleted=True)
                    ev(f"сообщение {i} удалено в Telegram — " + ("удалено и в eXpress" if ok else
                       "в eXpress не найдено" if ok is None else "удалить в eXpress не удалось"))
                else:
                    _remember(i, del_fail=rec.get("del_fail", 0) + 1)


@dp.edited_message()
@dp.edited_channel_post()
async def on_edit(m: Message):
    if m.chat.id != TG_CHAT_ID:
        return
    body = m.text or m.caption or ""
    rec = _map.get(str(m.message_id), {})
    old = rec.get("text")
    if old is not None and old == body:
        return                          # меняли не текст (например, реакции или превью ссылки)
    try:
        if rec.get("ex"):
            head = rec.get("head") or f"{author(m)}:"
            if await ex.edit(rec["ex"], f"{head}\n{body or '[текст удалён]'}".strip()):
                _remember(m.message_id, text=body)
                ev(f"сообщение {m.message_id} изменено в eXpress")
                return
        # исходное сообщение в eXpress не найдено — отправляем правку отдельным сообщением
        head = f"{author(m)} (изменено):"
        if old:
            head += f"\n» было: {old.replace(chr(10), ' ')[:120]}"
        ex_id = await ex.send(f"{head}\n{body or '[текст удалён]'}".strip())
        _remember(m.message_id, text=body, ex=ex_id, head=f"{author(m)} (изменено):",
                  ts=rec.get("ts") or int(time.time()))
        ev(f"правка сообщения {m.message_id} от {author(m)} отправлена новым сообщением")
    except Exception as e:
        ev(f"правка сообщения {m.message_id} не переслана: {e}", logging.ERROR)


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
    _remember(m.message_id, text=body, head=head, ts=int(time.time()))
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
        ex_id = await ex.send(f"{head}\n{body}".strip(), path, is_photo)
        if ex_id:
            _remember(m.message_id, ex=ex_id)
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
    # браузер при старте не запускаем: он нужен только для отправки и для входа по QR
    state["browser"] = "спит (запустится при новом сообщении или на вкладке моста)"
    asyncio.create_task(ex.watchdog())
    asyncio.create_task(deletion_watch())
    # handle_as_tasks=False: строго по порядку; необработанные апдейты Telegram отдаст повторно
    while True:
        try:
            state["telegram"] = "подключён"
            await dp.start_polling(bot, handle_as_tasks=False, handle_signals=False,
                                   allowed_updates=["message", "channel_post", "edited_message", "edited_channel_post"])
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
        async def shot():
            if not ex.running():
                async with ex.lock:
                    await ex.awake()
                    await ex.ensure_page()
            ex.last_used = time.time()          # панель открыта — браузер не усыпляем
            return await ex.page.screenshot(type="jpeg", quality=60)
        try:
            img = _run(shot(), 150)
        except Exception as e:
            return Response(f"браузер не готов: {e}", 503)
        return Response(img, mimetype="image/jpeg", headers={"Cache-Control": "no-store"})

    @flask_app.post("/bridge/reload")
    def bridge_reload():
        if (r := need_auth()):
            return r
        async def go():
            async with ex.lock:
                try:
                    await ex.awake()
                    await ex.page.goto(WEB_URL, wait_until="commit", timeout=120000)
                except Exception as e:
                    ev(f"перезагрузка: {e}", logging.ERROR)
                await ex.check_login()
        try:
            _run(go(), 280)
        except Exception as e:
            return jsonify(ok=False, error=str(e)), 500
        return jsonify(ok=True)
