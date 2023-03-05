FROM python:3.8-slim

ENV AIRFLOW_HOME=~/airflow
ENV AIRFLOW_VERSION=2.5.0
ENV PYTHON=3.8

ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON}.txt"

COPY requirements.txt /opt/fundamentus/requirements.txt

RUN python -m pip install --upgrade pip
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install -r /opt/fundamentus/requirements.txt

#COPY fundamentus_dag.py /root/airflow/dags/
COPY dag_generate_fundamentus.py /root/airflow/dags/

COPY fundamentus /root/airflow/dags/fundamentus
COPY ticker /root/airflow/dags/ticker
COPY resources /root/airflow/dags/resources

EXPOSE 8080

CMD ["airflow", "standalone"]

# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html