FROM python:3.9
WORKDIR /app
COPY . ./app
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["python", "load_data_to_postgres.py"]