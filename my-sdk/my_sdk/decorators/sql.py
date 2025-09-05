from typing import Any, ClassVar, Collection, Mapping, Callable
from collections.abc import Sequence
from airflow.sdk.bases.decorator import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.sdk.definitions.context import Context
import warnings

class _SQLDecoratedOperator(DecoratedOperator, SQLExecuteQueryOperator):
    
    # mesclando os templates e os renderers das 2 classes herdadas
    # templates são valores que podem ser executados em tempo de execução através do fornecimento de strings com placeholders Jinja
    # renderers provêem uma forma de passar valores de forma dinâmica para os templates
    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SQLExecuteQueryOperator.template_fields)
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **SQLExecuteQueryOperator.template_fields_renderers,
    }
    
    # nome do novo decorador
    custom_operator_name: str = "@task.sql"
    overwrite_rtif_after_execution: bool = True
    
    def __init__(
        self,
        
        # isso diz que todos os parâmetros após esse devem ser nomeados, e não posicionais
        *,
        
        # recebe uma função como argumento (toda função é um objeto callable)
        # parâmetro obrigatório
        python_callable: Callable,
        
        # parâmetros posicionais opcionais para 'python_callable'
        # pode ser uma coleção de qualquer coisa ou None
        # None é o valor default
        op_args: Collection[Any] | None = None,
        
        # parâmetros nomeados opcionais para 'python_callable'
        # pode ser um dicionário ou None
        # None é o valor default
        op_kwargs: Mapping[str, Any] | None = None,
        
        # captura qualquer outro argumento nomeado que eu queira passar para as classes pai
        # tudo que não é python_callable, op_args ou op_kwargs vai parar em **kwargs
        **kwargs,
    ) -> None:
        
        # esse decorador não suportará o parâmetro 'multiple_outputs'
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )
        
        # instanciando as 2 classes pai
        super().__init__(
            # vem do DecoratedOperator
            python_callable=python_callable,
            
            # argumentos para a função
            op_args=op_args,
            op_kwargs=op_kwargs,
            
            # 'sql' é do SQLExecuteQueryOperator
            # a query será executada dinamicamente
            sql=SET_DURING_EXECUTION,
            multiple_outputs=False,
            
            # com esse kwargs eu garanto compatibilidade com todos os atributos opcionais de ambas as classes pai
            **kwargs,
        )
    
    # execute é o método chamado quando a task vai ser executada
    # contexto é um dicionário com informações de execução (ds - data de execução, ti - TaskInstance, etc.)
    # retorno é Any pois o método pode devolver qualquer coisa    
    def execute(self, context: Context) -> Any:
        # adicionando os argumentos nomeados ao context
        # permite que a função acesse argumentos direto do context
        context_merge(context, self.op_kwargs)
        
        # preparando os argumentos para a função, tanto os posicionais de op_args quanto todos do context (incluindo os op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        
        # executa a função decorada pelo usuário, que deve retornar a string já "montada" com a query e todos os seus valores dinâmicos
        # self.sql é atributo de SQLExecuteQueryOperator
        self.sql = self.python_callable(*self.op_args, **kwargs)
        
        # garantindo que a função não retornou None ou uma string vazia
        if not isinstance(self.sql, str) or self.sql.strip() == "":
            raise TypeError("The returned value from the TaskFlow callable must be a non-empty string.")
        
        # percorre todos os templates da task e substitui os placeholders Jinja pelos valores reais do context
        context["ti"].render_templates()
        
        # execute é do operador SQLExecuteQueryOperator, que de fato executa o comando no banco de dados
        return super().execute(context)
    
# função que corresponde ao decorador
def sql_task(
        python_callable: Callable | None = None,
        **kwargs
    ) -> TaskDecorator:
    
    # essa função cria e retorna um novo decorador    
    return task_decorator_factory(
            # função/task definida na dag
            python_callable = python_callable,
            
            # classe que faz a execução da task personalizada
            decorated_operator_class = _SQLDecoratedOperator,
            **kwargs
    )