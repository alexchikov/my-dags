from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from typing import Any


class MySimpleOperator(BaseOperator):
    def __init__(self, name: str, message: str, **kwargs):
        super().__init__(**kwargs)
        self._name = name
        self._message = message

    def execute(self, context: Context) -> Any:
        print(self._name + ' said: ' + self._message)

    @property
    def message(self):
        return self._message

    @property
    def name(self):
        return self._name
