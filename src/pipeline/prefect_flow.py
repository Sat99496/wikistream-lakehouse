from prefect import flow, task
import subprocess
import sys


@task
def bronze_to_silver():
    subprocess.check_call([sys.executable, "-m", "src.transform.bronze_to_silver_latest"])


@task
def build_gold():
    subprocess.check_call([sys.executable, "-m", "src.gold.run_gold"])


@flow
def wikistream_pipeline():
    bronze_to_silver()
    build_gold()


if __name__ == "__main__":
    wikistream_pipeline()
