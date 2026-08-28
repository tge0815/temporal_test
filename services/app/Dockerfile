FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Der konkrete Startbefehl wird je Service in der docker-compose.yml gesetzt.
CMD ["python", "-c", "print('Bitte command in docker-compose.yml setzen')"]
