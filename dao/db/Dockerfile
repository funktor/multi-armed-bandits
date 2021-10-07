FROM tiangolo/uwsgi-nginx:python3.9

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

COPY nginx.conf dbservice.py dao.py serve.py wsgi.py /app
WORKDIR /app