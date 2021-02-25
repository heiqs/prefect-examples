import prefect
from prefect import task, Flow
from prefect.storage import GitHub

@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")

with Flow("hello-flow-git") as flow:
    say_hello()

flow.storage = GitHub(
    repo='heiqs/prefect-examples',
    path="flows/hello.py",
    ref='main'
)
