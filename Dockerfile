FROM python:3.9
WORKDIR /ecstask
COPY . ./ecstask
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8000
ENTRYPOINT ["python", "dbscript-v3.py"]