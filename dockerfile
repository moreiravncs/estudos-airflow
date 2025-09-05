FROM apache/airflow:3.0.0

# Copiando a pasta local com o provider para dentro do container do airflow.
COPY my-sdk /opt/airflow/my-sdk

# Instalando o provider.
RUN pip install -e /opt/airflow/my-sdk