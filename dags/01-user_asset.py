from airflow.sdk import asset, Asset, Context

@asset(
    # obrigatório
    schedule = "@daily",
    
    # boa prática
    # caminho da fonte dos dados
    uri = "https://randomuser.me/api/"
)
def user_asset(self) -> dict[str]:
    '''
        retorna um dicionário com strings como valores
    '''
    import requests
    
    # já que defini uri no decorador e já que isso agora é um objeto do tipo asset, eu posso referenciar o atributo com self
    r = requests.get(self.uri)
    
    # sempre que retorno um valor em uma task, ele se torna um XCom
    return r.json()

# não chamar o asset!
# user_asset()

# criando vários assets em um só
# apesar disso ser possível, é recomendável ter uma função para cada asset mesmo
@asset.multi(
    schedule = user_asset,
    outlets=[
        Asset(name="user_location_asset"),
        Asset(name="user_login_asset")
    ]
)
def user_info_asset(user_asset: Asset, context: Context) -> list[dict[str]]:
    user_data = context['ti'].xcom_pull(
        dag_id = user_asset.name,
        task_ids = user_asset.name,
        include_prior_dates = True
    )
    
    return [
        user_data['results'][0]['location'],
        user_data['results'][0]['login']
    ]

# sempre é preciso ativar o asset na UI

"""
@asset(
    # assim que o asset anterior for materializado, esse também será
    schedule = user_asset
)
def user_location_asset(user_asset: Asset, context: Context) -> dict[str]:
    '''
        - o parâmetro deve ter o mesmo nome do asset de origem
        - o objeto context é necessário para eu conseguir fazer pull no XCom
    '''
    
    # 'ti' = task instance
    # um asset, por baixo dos panos, é uma dag com uma task
    user_data = context['ti'].xcom_pull(
        dag_id = user_asset.name,
        task_ids = user_asset.name,
        
        # necessário para recuperar sempre o último XCom retornado pelo asset/dag user_asset
        include_prior_dates = True
    )
    
    # isso aqui também vai para a tabela XCom do metastore como um registro
    return user_data['results'][0]['location']

@asset(
    # assim que o asset anterior for materializado, esse também será
    schedule = user_asset
)
def user_login_asset(user_asset: Asset, context: Context) -> dict[str]:
    
    user_data = context['ti'].xcom_pull(
        dag_id = user_asset.name,
        task_ids = user_asset.name,
        include_prior_dates = True
    )
    
    return user_data['results'][0]['login']
"""