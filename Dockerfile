FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cria volume para o banco de dados persistir
VOLUME ["/app/data"]
ENV DB_PATH=/app/data/mercado.db

EXPOSE 5000

CMD ["python", "app.py"]
