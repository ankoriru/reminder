"""Точка входа контейнера remind: напоминания (app.py) + мост Telegram → eXpress (bridge.py)."""
import os
import app as remind         # инициализирует БД, бота напоминаний и планировщик, как раньше
from app import app
import bridge

bridge.register(app)
# сообщения, которые remind отправляет сам, Telegram боту не возвращает — передаём их в мост напрямую
remind.SENT_HOOKS.append(bridge.forward_own)
bridge.start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "80")), debug=False, threaded=True)
