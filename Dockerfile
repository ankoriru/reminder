# Официальный образ Playwright: Ubuntu 24.04, Python 3.12, Chromium и системные зависимости уже внутри
FROM mcr.microsoft.com/playwright/python:v1.49.1-noble
WORKDIR /app
ENV PYTHONUNBUFFERED=1 TZ=Europe/Moscow PIP_BREAK_SYSTEM_PACKAGES=1
COPY requirements.txt .
RUN pip install --no-cache-dir --ignore-installed -r requirements.txt
COPY . .
EXPOSE 80
CMD ["python", "run.py"]
