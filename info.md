1. Instalar o venv para a versão do Python instalada:

`sudo apt install python3.10-venv`

2. Criar ambiente virtual:

`python3 -m venv venv`

3. Ativar ambiente virtual:

`source venv/bin/activate`

Para desativar: `deactivate`

4. Instalar airflow:

Essa instalação via pip é apenas para o VS Code entender a sintaxe do airflow.

`pip install apache-airflow==3.0.0`

5. Checar os containers já em execução, para evitar conflito com os serviços do airflow:

`docker ps`

Se houver algum que possa causar conflito, parar a execução do mesmo: `docker stop <container_id_ou_nome>`

6. Executar docker compose:

`docker compose up -d`

7. Acessar a GUI

`localhost:8080`


