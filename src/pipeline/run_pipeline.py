import subprocess
import sys


def run(cmd):
    print("+", " ".join(cmd))
    subprocess.check_call(cmd)


def main():
    run([sys.executable, "-m", "src.transform.bronze_to_silver_latest"])
    run([sys.executable, "-m", "src.gold.run_gold"])


if __name__ == "__main__":
    main()
