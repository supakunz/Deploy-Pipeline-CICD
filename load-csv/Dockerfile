FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_csv.py .
COPY data/ ./data

CMD ["python", "load_csv.py"]
