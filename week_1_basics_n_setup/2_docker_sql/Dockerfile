FROM python:3.9

RUN apt-get install wget
RUN pip3 install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python3", "ingest_data.py"]