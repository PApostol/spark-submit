"""Some basic default spark-submit arguments and driver end states"""
import os
from typing import Any, Dict, Set


__defaults__: Dict[str, Any ] = {
    'spark_home': os.environ.get('SPARK_HOME', os.path.expanduser('~/spark_home')),
    'master': 'local[*]',
    'name': 'spark-submit-task',
    'deploy_mode': 'client',
    'driver_memory': '1g',
    'executor_memory': '1g',
    'executor_cores': '1',
    'total_executor_cores': '2',
    'py_files': None,
    'files': None,
    'class': None,
    'jars': None,
    'packages': None,
    'exclude_packages': None,
    'repositories': None,
    'verbose': False,
    'supervise': False,
    'properties_file': None,
    'conf': [],
    'main_file_args': ''
}

# Possible Spark driver states:
# SUBMITTED: Submitted but not yet scheduled on a worker
# RUNNING: Has been allocated to a worker to run
# FINISHED: Previously ran and exited cleanly
# RELAUNCHING: Exited non-zero or due to worker failure, but has not yet started running again
# UNKNOWN: The state of the driver is temporarily not known due to master failure recovery
# KILLED: A user manually killed this driver
# FAILED: The driver exited non-zero and was not supervised
# ERROR: Unable to run or restart due to an unrecoverable error (e.g. missing jar file)

# states that conclude a job
__end_states__: Set[str] = {'FINISHED', 'UNKNOWN', 'KILLED', 'FAILED', 'ERROR'}
