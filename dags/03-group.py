from airflow.sdk import dag, task, task_group

@dag
def group():
    @task
    def a():
        #print("a")
        
        # valor automaticamente vai para o XCom
        return 42
    
    @task_group(
        
        # 2 tentativas de executar todas as tasks desse grupo antes de falhar
        default_args = {
            "retries": 2
        }
    )
    def my_group(val: int):
        @task
        def b(my_val: int):
            '''
                se eu tentar usar 'val' direto vai dar erro (por
                mais que possa parecer o certo)
                
                o identificador único da task b será "my_group.b"
            '''
            print(my_val + 10)

        @task_group(
            default_args = {
                "retries": 3
            }
        )
        def my_second_group():
            @task
            def c():
                print("c")
            
            c()
        
        #b() >> my_second_group()
        b(val) >> my_second_group()
    
    #a() >> my_group()
    val = a()
    my_group(val)
    
group()