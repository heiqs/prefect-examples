import prefect
from prefect import task, Flow
import os
import time

@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info(f"Hello, {os.getenv('username')}!")
#     print('This is an updated version!')
    print(os.getenv('username'))

with Flow("hello-flow-git-with-env-var") as flow:
    say_hello()
