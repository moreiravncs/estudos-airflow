from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():
    
    @task
    def t1() -> Dict[str, Any]:
        '''
        val = 42
        
        # para usar o xcom_push, eu preciso do "context" dessa task
        # 'ti' para Task Instance
        context['ti'].xcom_push(
            key = "t1_xcom", # identificador do xcom
            value = val
        )
        '''
        
        # também vai para o xcom
        # return 42
        
        # aqui são criados 2 xcom's, um para cada par chave-valor
        return {"val": 42, "sentence": "Hey you!"}
    
    '''
    @task
    def t2(context: Context):
        val = context['ti'].xcom_pull(
             task_ids = 't1',
             key = 't1_xcom'
        )
        
        print(val)
    
    @task
    def t2(val: int):
        print(val)
    '''
    
    @task
    def t2(data: Dict[str, Any]):
        print(data["val"])
        print(data["sentence"])
    
    dict_val = t1() 
    t2(dict_val)
    
xcom_dag()