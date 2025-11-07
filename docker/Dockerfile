####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Dockerfile
####

FROM apache/airflow:3.1.2
COPY /docker/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt