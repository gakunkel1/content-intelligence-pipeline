FROM python:3.12-slim

RUN pip install --no-cache-dir streamlit psycopg2-binary pandas

WORKDIR /app

COPY app.py /app/app.py

ENTRYPOINT ["streamlit", "run", "app.py", "--server.address", "0.0.0.0"]