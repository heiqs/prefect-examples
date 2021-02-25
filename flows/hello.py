import prefect
from prefect import task, Flow
from prefect.storage import GitHub

@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")

with Flow("hello-flow-git") as flow:
    say_hello()
