from typing import Any

# o hatch usa isso para preencher dinamicamente a versão do pacote
__version__ = "0.0.1"

# "interface pública" do pacote
# quando alguém importar o pacote com 'my_sdk import *', tudo dessa lista ficará disponível
# lembrar de POO, interface de uma classe é o que ela disponibiliza, respeitando seu encapsulamento
__all__ = ["sql"]

# essa função retorna metadados do pacote 
# é essencial para o Airflow conseguir carregar providers
# é o 'entrypoint' para o meu pacote, conforme definido no arquivo '.toml'
def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "my-sdk",
        "name": "My SDK",
        "description": "Teste de criação de pacote para o Airflow.",
        "versions": [__version__],
        
        # decoradores que quero disponibilizar através do provider
        "task-decorators": [
            {
                "name": "sql",
                
                # caminho da classe/função que define o decorador 'sql'
                "class-name": "my_sdk.decorators.sql.sql_task"
            }
        ]
    }