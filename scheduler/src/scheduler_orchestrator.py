# vivarium/scheduler/src/scheduler_orchestrator.py

"""
A central orchestrator for managing the lifecycle of background scheduler tasks.

This script reads a configuration file to determine which individual scheduler
services should be enabled and launched as subprocesses. It is designed to run
continuously, providing a single point of control for the Vivarium automation system.
"""

import sys
import subprocess
import time
import logging
from typing import Dict, List
from pathlib import Path


vivarium_root_path = Path(__file__).resolve().parents[2]
if str(vivarium_root_path) not in sys.path:
    sys.path.insert(0, str(vivarium_root_path))

from utilities.src.config import SchedulerConfig

#: A dictionary mapping logical scheduler names to their corresponding script filenames.
SCHEDULER_SCRIPTS = {
    "weather_fetcher": "scheduler/src/weather_scheduler.py",
    "vivarium_controller": "scheduler/src/vivarium_scheduler.py",
}

#: A list to track active subprocess objects.
active_processes: List[subprocess.Popen] = []


def start_scheduler_process(script_name: str) -> None:
    """
    Starts a given scheduler script as a subprocess.

    :param script_name: The filename of the scheduler script to be launched.
    :type script_name: str
    """
    try:
        process = subprocess.Popen([sys.executable, script_name])
        active_processes.append(process)
        print(f"-> Started {script_name} with PID {process.pid}")
    except FileNotFoundError:
        logging.error(f"Scheduler script '{script_name}' not found.")
    except Exception as e:
        logging.error(f"Error starting {script_name}: {e}")

def main() -> None:
    """
    Main function to orchestrate the scheduler services.
    """
    print("--- Starting Scheduler Orchestrator ---")
    config = SchedulerConfig()

    print("Checking enabled schedulers...")

    if config.enable_weather_fetch:
        start_scheduler_process(SCHEDULER_SCRIPTS["weather_fetcher"])
    
    if config.enable_climate_control:
        start_scheduler_process(SCHEDULER_SCRIPTS["vivarium_controller"])
    
    print("\nOrchestrator running. Press Ctrl+C to stop all schedulers.")
    try:
        while True:
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Shutting down all schedulers...")
        for process in active_processes:
            process.terminate()
            print(f"Terminating process {process.pid}")
        for process in active_processes:
            process.wait()
        print("All schedulers have been shut down.")
        sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()