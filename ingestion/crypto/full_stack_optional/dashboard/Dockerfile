FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.39.0 \
    psycopg[binary]==3.2.3 \
    pandas==2.2.3 \
    plotly==5.24.1 \
    sqlalchemy==2.0.35

COPY dashboard /app/dashboard

EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.address=0.0.0.0", "--server.port=8501", \
     "--server.headless=true"]
