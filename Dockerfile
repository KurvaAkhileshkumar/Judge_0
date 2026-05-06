FROM python:3.12-slim

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY . .
RUN pip install --no-cache-dir .

# Default: API server via gunicorn + gevent
# Override CMD in docker-compose for worker / reconciler
CMD ["gunicorn", "api:app", \
     "--worker-class", "gevent", \
     "--workers", "2", \
     "--worker-connections", "1000", \
     "--bind", "0.0.0.0:5000", \
     "--timeout", "1900"]
