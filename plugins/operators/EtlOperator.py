from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable

class HelloOperator(BaseOperator):

    def __init__(
            self,
            name: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name

    def execute(self, context):
        message = "Hello {}".format(self.name)
        print(message)
        print("File: ", Variable.get("fileName"))
        return message