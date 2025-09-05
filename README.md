## Passo a passo para configurar o ambiente local para estudo do Apache Airflow

1. Checar a versão do Python instalada

```bash
python3 --version
```

2. Instalar o venv para a versão correta do Python

```bash
sudo apt install python3.10-venv -y
```

3. Criar o ambiente virtual

```bash
python3 -m venv venv
```

3. Ativar o ambiente

```bash
source venv/bin/activate
```

Para desativar: 

```bash
deactivate
```

4. Instalar o airflow no venv

```bash
pip install apache-airflow
```

Essa instalação é apenas para o VS Code entender a sintaxe, o Airflow de fato está no docker-compose

5. Checar os containers em execução e parar os que possam causar conflito

```bash
docker ps
docker stop nome_container
```

4. Deploy dos serviços do Airflow

```bash
docker compose up
```

Para parar os serviços: 

```bash
docker compose down
```