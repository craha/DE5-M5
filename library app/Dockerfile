FROM python:3.12.3
WORKDIR /app
COPY . /app
RUN pip3 install -r requirements.txt
ENTRYPOINT ["python","datacleaner.py"]