"""Точка входа контейнера remind: напоминания (app.py) + мост Telegram → eXpress (bridge.py)."""
import os
from app import app          # инициализирует БД, бота напоминаний и планировщик, как раньше
import bridge

bridge.register(app)
bridge.start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "80")), debug=False, threaded=True)
