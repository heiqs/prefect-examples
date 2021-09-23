import prefect
from prefect import task, Flow
from prefect.utilities.notifications import slack_notifier
import os

@task(name="Say Hello", state_handlers=[slack_notifier])
def say_hello():
    logger = prefect.context.get("logger")
    logger.info(f"Hello, {os.getenv('username')}!")
    print(f"Hello, {os.getenv('username')}!")

with Flow("hello-flow-git-with-env-var") as flow:
    say_hello()
