FROM tiangolo/uwsgi-nginx:python3.9

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

COPY nginx.conf dao.py queueingservice.py serve.py wsgi.py /app
WORKDIR /app