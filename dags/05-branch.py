from airflow.sdk import dag, task

@dag
def branch():
    
    @task
    def a() -> int:
        return 1
    
    # se a retornar 1, executar c
    # senão, executar d
    # adicionando '.branch' essa task já vira uma task só pra fazer a condição
    @task.branch
    def b(val: int) -> str:
        # o retorno é o id da task
        if val == 1:
            return ["is_1", "is_1_really"]
        else:
            return "isnt_1"
    
    @task
    def is_1(val: int):
        print(f"é {val}")
    
    @task
    def is_1_really(val: int):
        print(f"é {val}")
    
    @task
    def isnt_1(val: int):
        print(f"não é {val}")
    
    val = a()
    
    # ao chamar 'b' o airflow já infere que na lista estão as possibilidades de execução da condição de 'b'
    b(val) >> [is_1(val), isnt_1(val), is_1_really(val)]

branch()