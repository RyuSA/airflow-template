import json
from airflow.models import Variable
from env import Env
from generator import Generator
import logging

logger = logging.getLogger(__name__)
logger.info("[main] start")

# load json file
with open('~/dags/model.json') as f:
    data = json.load(f)
    logger.info(data)

for model in data:
    env = Env()
    variablr = Variable.get('test', "")
    logger.info(env.airflow_vars)
    name = model['name']
    Generator(name + "_2_").generate()

logger.info("[main] end")
