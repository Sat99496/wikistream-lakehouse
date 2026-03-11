import subprocess
import sys

SCRIPTS = [
    "src/gold/pageviews_daily_top.py",
    "src/gold/edits_per_hour.py",
    "src/gold/bot_vs_human.py",
]

def main():
    for script in SCRIPTS:
        print(f"Running {script}")
        subprocess.check_call([sys.executable, script])

if __name__ == "__main__":
    main()
