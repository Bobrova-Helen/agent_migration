FROM python:3.11-slim-bookworm

# 🔹 Оптимизация Python для контейнеров
# PYTHONDONTWRITEBYTECODE=1  → не генерировать __pycache__
# PYTHONUNBUFFERED=1         → логи сразу в stdout (не буферизуются)
# PIP_NO_CACHE_DIR=1         → не сохранять кэш pip в образ
# PIP_DISABLE_PIP_VERSION_CHECK=1 → ускорить сборку
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Рабочая директория внутри контейнера
WORKDIR /app

# 1️⃣ Копируем только requirements.txt (Docker закэширует этот слой)
COPY requirements.txt .

# 2️⃣ Устанавливаем зависимости
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 3️⃣ Копируем исходный код
COPY . .

# 4️⃣ Запуск агента по умолчанию
CMD ["python", "agent.py"]