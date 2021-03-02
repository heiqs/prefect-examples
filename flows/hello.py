import prefect
from prefect import task, Flow
from prefect.storage import GitHub
import os

@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info(f"Hello, {os.getenv('USER')}!")

with Flow("hello-flow-git-with-env-var") as flow:
    say_hello()
