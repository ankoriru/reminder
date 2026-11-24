FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PLAYWRIGHT_BROWSERS_PATH=/ms-playwright TZ=Europe/Moscow
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt \
 && playwright install --with-deps chromium && rm -rf /var/lib/apt/lists/*
COPY . .
EXPOSE 80
CMD ["python", "run.py"]
