## Passo a passo para configurar o ambiente local para estudo do Apache Airflow

#### 1. Checar a versão do Python instalada

```bash
python3 --version
```

#### 2. Instalar o venv para a versão correta do Python

'-y' é yes para todas as perguntas

```bash
sudo apt install python3.10-venv -y
```

#### 3. Criar o ambiente virtual

No Linux o Python é instalado em vários pacotes separados
'-m' é para usar o módulo padrão venv (se não fizer isso dá erro)
Na prática, a instalação anterior do venv instala arquivos extras no pacote pra ele funcionar de verdade

```bash
python3 -m venv venv
```

#### 4. Ativar o ambiente

```bash
source venv/bin/activate
```

Para desativar: 

```bash
deactivate
```

#### 5. Instalar o airflow no venv

```bash
pip install apache-airflow
```

Essa instalação é apenas para o VS Code entender a sintaxe, o Airflow de fato está no docker-compose

#### 6. Checar os containers em execução e parar os que possam causar conflito

```bash
docker ps
docker stop nome_container
```

#### 7. Deploy dos serviços do Airflow

'-d' aqui é para liberar o terminal

```bash
docker compose up -d
```

Para parar os serviços: 

```bash
docker compose down
```