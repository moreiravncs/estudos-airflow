import requests
import csv
from datetime import datetime

# importando o decorador que define dag/task
from airflow.sdk import dag, task

# importando os operadores
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.sdk.bases.sensor import PokeReturnValue

from airflow.providers.standard.operators.python import PythonOperator

# 'hook' é uma abstração para interagir com uma ferramenta ou serviço
# SEMPRE que eu uso uma ferramenta ou serviço através de um operador, esse operador depende de um hook
# usando hooks eu posso ter acesso a métodos que o operador não possui
# foi preciso instalar esse hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _extract_user(ti):
    '''
    recebe um objeto do tipo TaskInstance, que é uma instância de uma task
    
    esse objeto me permite acessar um método que busca os dados provenientes de uma task anterior
    
    'ti' é palavra reservada no airflow
    '''
    
    # acesando o valor do xcom para a task 'is_api_available'
    # fake_user = ti.xcom_pull(task_ids = "is_api_available")
    
    # apenas para testar a task via cli do scheduler, pois esse método de teste não pode acessar a tabela de xcom's do metastore
    #'''
    response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
    fake_user = response.json()
    #'''
    
    return {
        "id": fake_user["id"],
        "firstname": fake_user["personalInfo"]["firstName"],
        "lastname": fake_user["personalInfo"]["lastName"],
        "email": fake_user["personalInfo"]["email"]
    }
    

# definindo a dag
# 'user_processing' é o identificador único da dag
@dag
def user_processing():
    '''
    Na função da dag eu defino as tasks, que são objetos de vários tipos
    '''
    
    # para todo banco de dados, usar 'SQL Operators' (e não operadores específicos para cada um)
    create_table = SQLExecuteQueryOperator(
        # id único da task
        task_id = "create_table",
        
        # id único da conexão (é preciso criar a conexão na GUI)
        # se o provider que eu preciso acessar não aparecer lá no cadastro de nova conexão, será preciso baixar o pacote correspondente (via pip, só ver a documentação)
        conn_id = "postgres",
        
        sql = """
        CREATE TABLE IF NOT EXISTS users (
            id INT,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        )
        """
    )
    
    # endereço da api com o .json de teste
    # raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json
        
    # verificando se a API está funcionando
    # operador sensor: espera uma condição ser verdadeira para prosseguir
    # indicando que a função que será criada, além de uma task, é um sensor
    # 'poke_interval' é o intervalo dos testes da api
    # timeout é em segundos também
    # há alguns outros sensores disponíveis
    @task.sensor(poke_interval = 10, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
            
        print(response.status_code)
            
        if response.status_code == 200:
            condition = True
                
            # aproveitando para já passar os dados da API para o XCom, caso o retorno seja 200
            # lembrando que o XCom é o mecanismo do Airflow usado para troca de informações pequenas entre as tasks (pequenas pois são armazenadas no metastore)
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
            
        return PokeReturnValue(is_done = condition, xcom_value = fake_user)
    
    # essa é apenas uma forma de executar Python no airflow
    '''
    extract_user = PythonOperator(
        task_id = "extract_user",
        python_callable = _extract_user
    )
    '''
    
    # outra forma de executar Python (melhor!)
    # o decorador task é o PythonOperator "por baixo dos panos"
    # recebe o valor retornado pela task anterior
    @task
    def extract_user(fake_user): 
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"]
        }
    
    @task
    def process_user(user_info):
        # apenas para testar a task
        '''
        user_info = {
            "id": "123123",
            "firstname": "Zé",
            "lastname": "Maria",
            "email": "reiDelas@gmail.com"
        }
        
        # apenas para testar a task 'store_user'
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        '''
        
        # acaba que essa coluna precisa existir, senão dá erro no 'COPY'
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # essa pasta 'tmp' está no root do container do scheduler
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames = user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
        
    @task
    def store_user():
        # 'postgres' é o nome da conexão criada na GUI do Airflow
        hook = PostgresHook(postgres_conn_id = "postgres")
        hook.copy_expert(
            # 'users' é a tabela criada anteriormente no postgres
            
            # copiando o csv direto para a tabela
            sql = "COPY users FROM STDIN WITH CSV HEADER",
            filename = "/tmp/user_info.csv"
        )

    
    # sempre que defino uma task como função, devo chamá-la
    # a dependência entre as tasks aqui é implícita (devo tornar explícita)
    # 'create_table' e 'store_user' aqui estão "flutuando"
    '''
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    process_user(user_info)
    store_user()
    '''
    
    '''
    task_a >> task_b
    
    # 3 tasks em paralelo
    task_a >> [task_b, task_c, task_d] >> task_e
    
    task_a >> task_b
    task_b >> task_c
    ou
    task_a >> task_b >> task_c
    '''
    
    # criando as dependências explicítas
    # '>>' bitwise operator
    process_user(extract_user(create_table >> is_api_available())) >> store_user()

# chamando a dag (senão, não aparece na GUI)
user_processing()