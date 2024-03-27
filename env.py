from airflow.utils.db import create_session
from airflow.models import Variable

v = Variable.get('test', "")
v2 = Variable.get('test22', "")

class Env:
    def __init__(self) -> None:
        with create_session() as session:
            self.airflow_vars = {var.key: var.val for var in session.query(Variable)}
