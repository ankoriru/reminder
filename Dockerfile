# Официальный образ Playwright: Ubuntu 24.04, Python 3.12, Chromium и системные зависимости уже внутри
FROM mcr.microsoft.com/playwright/python:v1.49.1-noble
WORKDIR /app
ENV PYTHONUNBUFFERED=1 TZ=Europe/Moscow PIP_BREAK_SYSTEM_PACKAGES=1 \
    PIP_DEFAULT_TIMEOUT=180 PIP_RETRIES=10 PIP_NO_CACHE_DIR=1
COPY requirements.txt .
# Сеть сборщика иногда обрывается, поэтому до 5 попыток установки.
RUN for i in 1 2 3 4 5; do \
      pip install --ignore-installed -r requirements.txt && break; \
      echo "pip: попытка $i не удалась, повтор через 15 с"; sleep 15; \
      [ $i -eq 5 ] && exit 1; \
    done && \
    python -c "import playwright, flask, aiogram, pandas; print('deps ok')"
# лёгкий chrome-headless-shell (если в базовом образе его нет — докачиваем; не критично)
RUN ls /ms-playwright; ls /ms-playwright | grep -q headless_shell || \
    (for i in 1 2 3; do playwright install chromium-headless-shell && break; sleep 15; done) || true
COPY . .
EXPOSE 80
CMD ["python", "run.py"]
