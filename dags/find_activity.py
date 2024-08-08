from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
import requests

API = "https://api.chucknorris.io/jokes/random"

@dag(
    start_date = datetime(2024, 1, 1),
    schedule = "@daily",
    tags = ["activity"],
    catchup = False
)

def find_activity():
    @task
    def get_activity():
        r = requests.get(API, timeout=10)
        return r.json()['value']
    
    @task
    def write_activity_to_file(response):
        filepath = Variable.get("activity_file")
        with open(filepath, "a") as f:
            f.write(f"Funny joke: {response}\r\n")
        return filepath
    
    @task
    def read_activity_from_file(filepath):
        with open (filepath, "r") as f:
            print(f.read())
    
    r = get_activity()
    f = write_activity_to_file(r)
    read_activity_from_file(f)

find_activity()