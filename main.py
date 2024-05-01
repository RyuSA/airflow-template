import json
from airflow.models import Variable
from generator import Generator
import logging

logger = logging.getLogger(__name__)
logger.info("[main] start")

# load json file
with open('/opt/airflow/dags/repo/model.json') as f:
    data = json.load(f)
    logger.info(data)

for model in data:
    variablr = Variable.get('test', "")
    name = model['name']
    Generator(name).generate()

logger.info("[main] end")
