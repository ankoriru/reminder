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

VERSION = "2026.11.25-9"
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
DELIVERY_TIMEOUT = int(os.getenv("DELIVERY_TIMEOUT_SEC", "900"))  # сколько ждать подтверждения доставки
MAX_TRIES = int(os.getenv("MAX_SEND_TRIES", "3"))                  # сколько раз можно отправить заново
MAX_FILE = int(os.environ.get("MAX_FILE_MB", "20")) * 1024 * 1024
WEB_URL = "https://corp.express/"

S_INPUT = ".message-input__message-field--main-chat .slate-message-input"
S_DOC = "input[type=file][id^=document-input]"
S_IMG = "input[type=file][id^=image-input]"
S_ENTRY = ".chat-list-entry"
S_ENTRY_NAME = ".chat-list-entry__name"

state = {"browser": "запуск", "logged_in": False, "sent": 0, "failed": 0, "delivered": 0,
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
        self.ws_last = 0.0        # последний кадр websocket eXpress: признак живой связи с сервером
        self.ws_since = 0.0

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
        self.watch_ws(self.page)
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
                cur = await msg.locator(".chat-message__text").first.inner_text() if await msg.locator(
                    ".chat-message__text").count() else ""
                if _norm(cur) == _norm(text):
                    return True                         # правка уже применена прошлой попыткой
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
                if msg is None or await self.status(ex_id) == "deleted":
                    ev(f"сообщение {ex_id} не найдено в чате eXpress или уже удалено", logging.WARNING)
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
                    if await self.status(ex_id) in ("deleted", "absent"):
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

    # ---------- отправка с подтверждением доставки ----------
    def watch_ws(self, page):
        def on_ws(ws):
            if "socket" not in ws.url:
                return
            def frame(_):
                if time.time() - self.ws_last > 60:
                    self.ws_since = time.time()
                self.ws_last = time.time()
            ws.on("framereceived", frame)
            ws.on("close", lambda _: setattr(self, "ws_last", 0.0))
        page.on("websocket", on_ws)

    def connected_for(self) -> float:
        """Сколько секунд подряд живо соединение с сервером eXpress (0 — нет связи)."""
        if time.time() - self.ws_last > 60:          # heartbeat Phoenix идёт каждые 30 с
            return 0.0
        return time.time() - self.ws_since

    async def my_ids(self) -> list[str]:
        return await self.page.evaluate(
            "() => [...document.querySelectorAll('.chat-message')]"
            ".filter(e => e.querySelector('.chat-message__bubble--my')).map(e => e.id)")

    async def status(self, ex_id: str) -> str:
        """sent/read/... — сервер принял; local — ещё не отправлено; deleted; absent — нет в чате."""
        return await self.page.evaluate("""(id) => {
            const x = document.getElementById(id);
            if (!x) return 'absent';
            if (x.querySelector('.chat-message__bubble--deleted')) return 'deleted';
            const s = x.querySelector('.chat-message__status');
            const m = s && s.className.match(/chat-message__status--([\\w-]+)/);
            return m ? m[1] : 'nostatus'; }""", ex_id)

    async def wait_status(self, ex_id: str, sec: float) -> str:
        st = "absent"
        for _ in range(int(sec * 2)):
            st = await self.status(ex_id)
            if confirmed(st):
                return st
            await self.page.wait_for_timeout(500)
        return st

    async def find_posted(self, item) -> str | None:
        """Ищет уже отправленную копию (после сбоя между Enter и записью id), чтобы не задублировать."""
        before = set(item.get("before") or [])
        head, tail = item["marker"]
        for i in reversed((await self.my_ids())[-15:]):
            if i in before:
                continue
            t = await self.page.evaluate("(id) => document.getElementById(id)?.innerText || ''", i)
            if head in t and tail in t:
                return i
        return None

    async def post(self, item):
        """Набирает и отправляет сообщение; запоминает id, присвоенный eXpress (ещё до ответа сервера)."""
        p = self.page
        if not await self._composer_empty():          # остатки прошлой попытки
            for b in await p.locator(".input-attachment__file button").all():
                await b.click()
            await p.locator(S_INPUT).first.click()
            await p.keyboard.press("Control+A")
            await p.keyboard.press("Delete")
        before = await self.my_ids()
        item["before"] = before[-30:]
        if item.get("file"):
            await p.locator(S_IMG if item.get("photo") else S_DOC).first.set_input_files(item["file"])
            await p.wait_for_timeout(1500)
        await self._type(item["text"] or " ")
        item["stage"] = "typed"
        save_outbox()
        await p.keyboard.press("Enter")
        for _ in range(60 if item.get("file") else 20):
            await p.wait_for_timeout(500)
            if await self._composer_empty():
                break
        else:
            raise RuntimeError("сообщение осталось в поле ввода")
        for _ in range(20):
            found = await self.find_posted(item)
            if found:
                item["ex"], item["stage"] = found, "local"
                save_outbox()
                return
            await p.wait_for_timeout(500)
        raise RuntimeError("отправленное сообщение не появилось в чате")

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
                    if time.time() - self.last_used > IDLE_SEC and not outbox:
                        await self.sleep(f" после {IDLE_SEC} с простоя")
                    elif not state["logged_in"]:
                        await self.check_login()
                except Exception as e:
                    ev(f"watchdog: {e}", logging.ERROR)


def confirmed(st: str) -> bool:
    return st not in ("local", "absent", "nostatus", "deleted", "error", "failed") and "err" not in st


def _norm(t: str) -> str:
    return re.sub(r"\s+", " ", t or "").strip()


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
                     if r.get("ts", 0) >= since and not r.get("deleted"))
        for k in range(0, len(ids), 100):
            try:
                gone = await _probe(service, ids[k:k + 100])
            except Exception as e:
                ev(f"проверка удалений: {e}", logging.ERROR)
                break
            for i in gone:
                _remember(i, deleted=True)
                enqueue({"kind": "delete", "tg": i})
                ev(f"сообщение {i} удалено в Telegram — ставлю удаление в eXpress в очередь")


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
    _remember(m.message_id, text=body)
    enqueue({"kind": "edit", "tg": m.message_id, "author": author(m), "body": body, "old": old})


@dp.message()
@dp.channel_post()
async def on_message(m: Message):
    if m.chat.id != TG_CHAT_ID:
        ev(f"сообщение из чата {m.chat.id} «{m.chat.title}» — не настроен для пересылки")
        return
    if str(m.message_id) in _map and _map[str(m.message_id)].get("queued"):
        return                          # Telegram прислал апдейт повторно — второй раз не ставим
    head = f"{author(m)}:"
    if m.forward_origin:
        head = f"{author(m)} (переслано):"
    if m.reply_to_message:
        q = (m.reply_to_message.text or m.reply_to_message.caption or "[вложение]").replace("\n", " ")
        head += f"\n» {q[:120]}"
    body = m.text or m.caption or ""
    obj, name, is_photo = media(m)
    path = None
    if obj:
        if (obj.file_size or 0) > MAX_FILE:
            body += f"\n[файл {name} больше {MAX_FILE // 1048576} МБ — не передан]"
        else:
            OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
            path = str(OUTBOX_DIR / f"{m.message_id}_{re.sub(r'[^\w.\-() ]', '_', name)}")
            try:
                await bot.download(obj, destination=path)
            except Exception as e:
                ev(f"файл сообщения {m.message_id} не скачан: {e}", logging.ERROR)
                body += f"\n[файл {name} не удалось получить из Telegram]"
                path = None
    elif not body:
        body = "[стикер, опрос или другой тип сообщения — не поддерживается]"
    _remember(m.message_id, text=m.text or m.caption or "", head=head, ts=int(time.time()), queued=True)
    enqueue({"kind": "send", "tg": m.message_id, "text": f"{head}\n{body}".strip(),
             "file": path, "photo": is_photo, "who": author(m)})


# ---------- очередь отправки с подтверждением доставки ----------
# Каждое действие (отправка, правка, удаление) лежит в /data/bridge_outbox.json, пока eXpress
# не подтвердит его. Порядок сохраняется, после перезапуска очередь продолжается с того же места.
# Дубли исключены: повторно отправляется только то, чего гарантированно нет в чате —
# по id сообщения eXpress (eXpress сам досылает неотправленное после восстановления связи).
OUTBOX_FILE = DATA / "bridge_outbox.json"
OUTBOX_DIR = DATA / "outbox"
outbox: list = []
undelivered: deque = deque(maxlen=20)
_wake = asyncio.Event()


def save_outbox():
    try:
        tmp = OUTBOX_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(outbox, ensure_ascii=False))
        tmp.replace(OUTBOX_FILE)
    except Exception as e:
        log.warning(f"не удалось сохранить очередь: {e}")


def load_outbox():
    global outbox
    try:
        outbox = json.loads(OUTBOX_FILE.read_text())
        if outbox:
            ev(f"в очереди после перезапуска: {len(outbox)} — продолжаю с проверкой доставки")
    except Exception:
        outbox = []


def _prep(item: dict):
    item.setdefault("t", time.time())
    item.setdefault("tries", 0)
    if item["kind"] == "send":
        lines = [l for l in item["text"].split("\n") if l.strip()]
        item["marker"] = [lines[0][:60], lines[-1][:60]] if lines else ["", ""]
        item["stage"] = "new"


def enqueue(item: dict):
    _prep(item)
    outbox.append(item)
    save_outbox()
    try:
        _wake.set()
    except RuntimeError:
        pass


def _preview(item) -> str:
    return _norm(item.get("text") or item.get("body") or "")[:80]


def _done(item, ok: bool, note: str):
    if ok:
        state["delivered"] += 1
        state["sent"] += 1
        state["last_ok"] = time.time()
        ev(f"✓ доставлено: {note}")
    else:
        state["failed"] += 1
        undelivered.appendleft({"time": time.strftime("%d.%m %H:%M:%S"), "tg": item.get("tg"),
                                "kind": item["kind"], "text": _preview(item), "reason": note})
        state["last_error"] = f"{time.strftime('%H:%M:%S')} не доставлено: {note}"
        ev(f"✗ НЕ доставлено: {note}", logging.ERROR)
    f = item.get("file")
    if f:
        Path(f).unlink(missing_ok=True)


async def _ready():
    """Браузер поднят, вход есть, нужный чат открыт. Без входа ждём (браузер не держим)."""
    warned = False
    while True:
        async with ex.lock:
            await ex.awake()
            if await ex.check_login():
                await ex.open_chat()
                return
            ex.last_used = 0
        if not warned:
            ev("очередь ждёт входа в eXpress: откройте вкладку моста и отсканируйте QR-код", logging.WARNING)
            warned = True
        while not state["logged_in"]:
            await asyncio.sleep(30)


async def _reload():
    async with ex.lock:
        try:
            await ex.page.reload(wait_until="commit", timeout=120000)
        except Exception as e:
            ev(f"перезагрузка страницы: {str(e).splitlines()[0]}", logging.WARNING)
    await _ready()


async def deliver_send(item):
    n = item["tg"]
    cycle = 0
    while True:
        await _ready()
        async with ex.lock:
            if not item.get("ex") and item.get("stage") == "typed":
                found = await ex.find_posted(item)       # Enter был нажат, но id не успели записать
                if found:
                    item["ex"], item["stage"] = found, "local"
                    ev(f"сообщение {n} уже было отправлено — повтор не нужен")
                else:
                    item["stage"] = "new"
                save_outbox()
            if not item.get("ex"):
                if item["tries"] >= MAX_TRIES:
                    return _done(item, False, f"сообщение {n}: {MAX_TRIES} попытки отправки без результата")
                item["tries"] += 1
                save_outbox()
                try:
                    await ex.post(item)
                except Exception as e:
                    ev(f"сообщение {n}, попытка {item['tries']}: {e}", logging.WARNING)
            if item.get("ex"):
                _remember(n, ex=item["ex"])
                st = await ex.wait_status(item["ex"], 20)
                if confirmed(st):
                    ex.last_used = time.time()
                    return _done(item, True, f"сообщение {n} от {item.get('who')} (id {item['ex'][:8]})")
            else:
                st = "не отправлено"
        if time.time() - item["t"] > DELIVERY_TIMEOUT:
            return _done(item, False, f"сообщение {n}: нет подтверждения сервера eXpress за "
                                      f"{DELIVERY_TIMEOUT // 60} мин (статус {st}); при восстановлении связи "
                                      f"eXpress может дослать его сам")
        cycle += 1
        ev(f"сообщение {n}: сервер eXpress ещё не подтвердил ({st}), проверка №{cycle}", logging.WARNING)
        await asyncio.sleep(min(60, 5 * cycle))
        await _reload()
        if not item.get("ex"):
            continue
        async with ex.lock:
            st = await ex.wait_status(item["ex"], 60)
            if confirmed(st):
                return _done(item, True, f"сообщение {n} от {item.get('who')} (после проверки №{cycle})")
            if st == "absent" and ex.connected_for() >= 30 and await ex.my_ids():
                # связь есть, чат загружен, а сообщения нет ни в чате, ни в очереди eXpress
                ev(f"сообщение {n} не найдено в eXpress при живой связи — отправлю заново", logging.WARNING)
                item.pop("ex", None)
                item["stage"] = "new"
                save_outbox()


async def deliver_edit(item):
    n = item["tg"]
    rec = _map.get(str(n), {})
    body = item.get("body") or "[текст удалён]"
    if not rec.get("ex"):
        # исходного сообщения в eXpress нет — правка уходит новым сообщением (с той же гарантией)
        head = f"{item['author']} (изменено):"
        if item.get("old"):
            head += f"\n» было: {item['old'].replace(chr(10), ' ')[:120]}"
        item.update(kind="send", text=f"{head}\n{body}", who=item["author"])
        _prep(item)
        save_outbox()
        await deliver_send(item)
        if item.get("ex"):
            _remember(n, head=f"{item['author']} (изменено):")
        return
    text = f"{rec.get('head') or item['author'] + ':'}\n{body}"
    for attempt in range(1, MAX_TRIES + 1):
        await _ready()
        if await ex.edit(rec["ex"], text):
            async with ex.lock:
                st = await ex.wait_status(rec["ex"], 20)
            if confirmed(st):
                return _done(item, True, f"правка сообщения {n}")
            ev(f"правка {n}: сервер ещё не подтвердил ({st})", logging.WARNING)
        if time.time() - item["t"] > DELIVERY_TIMEOUT:
            break
        await asyncio.sleep(5 * attempt)
        await _reload()
    _done(item, False, f"правка сообщения {n} не подтверждена")


async def deliver_delete(item):
    n = item["tg"]
    ex_id = _map.get(str(n), {}).get("ex")
    if not ex_id:
        return _done(item, True, f"удаление {n}: в eXpress сообщения не было")
    for attempt in range(1, MAX_TRIES + 1):
        await _ready()
        r = await ex.delete(ex_id)
        if r or r is None:
            return _done(item, True, f"удаление сообщения {n}" + ("" if r else " (уже удалено)"))
        await asyncio.sleep(5 * attempt)
        await _reload()
    _done(item, False, f"удаление сообщения {n} не выполнено")


async def outbox_worker():
    while True:
        if not outbox:
            _wake.clear()
            await _wake.wait()
            continue
        item = outbox[0]
        try:
            await {"send": deliver_send, "edit": deliver_edit, "delete": deliver_delete}[item["kind"]](item)
        except Exception as e:
            ev(f"очередь: {e}", logging.ERROR)
            await asyncio.sleep(10)
            continue                    # элемент остаётся в очереди
        if outbox and outbox[0] is item:
            outbox.pop(0)
        save_outbox()


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
    load_outbox()
    asyncio.create_task(outbox_worker())
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
                       failed=state["failed"], delivered=state["delivered"], pending=len(outbox),
                       undelivered=list(undelivered), last_ok=fmt(state["last_ok"]),
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
