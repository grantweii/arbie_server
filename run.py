import sys
from src.backtests import runner as backtestRunner
# from src.backtests import runner as backtestRunner

# This file is for running scripts. The problem we have is that it is difficult to run python scripts within a project that are in different folders

# ARG0 is run.py
# ARG1 - script we want to run
# ARG[@] - parameters specific to script
if __name__ == '__main__':
    script = sys.argv[1]
    # store is a boolean to store the results of backtest in db
    store = None
    if len(sys.argv) > 2:
        store = sys.argv[2]
    if script == 'backtest':
        backtestRunner.run(store=store)
    