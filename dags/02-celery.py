from airflow.sdk import dag, task
from time import sleep

@dag
def celery_dag():
    
    @task
    def a():
        sleep(5)
    
    @task
    def b():
        sleep(5)

    # essa fila foi criada na interface do flower, em um worker em específico    
    @task(
        queue = "high_cpu"
    )
    def c():
        sleep(5)
    
    @task(
        queue = "high_cpu"
    )
    def d():
        sleep(5)
    
    # tasks 'a' e 'b' serão executadas nos workers cuja fila é 'default'
    # tasks 'c' e 'd' serão executadas nos workers cuja fila é 'high_cpu'
    a() >> [b(), c()] >> d()
    
celery_dag()