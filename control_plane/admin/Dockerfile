FROM tiangolo/uwsgi-nginx:python3.9

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

COPY nginx.conf migrate.py consumer.py serve.py wsgi.py sampled_data.pkl /app
WORKDIR /app