FROM tiangolo/uwsgi-nginx:python3.9

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

COPY matrixfactorization.py nginx.conf predictor.py serve.py wsgi.py facto.pkl /app
WORKDIR /app