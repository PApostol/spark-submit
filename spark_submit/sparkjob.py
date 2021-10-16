from .system import _get_env_vars
from .exceptions import SparkSubmitError, SparkJobKillError
from typing import Any
import logging
import os
import platform
import re
import requests
import subprocess
import threading
import time

# some basic default spark-submit arguments
__defaults__ = {
    'spark_home': os.environ.get('SPARK_HOME', os.path.expanduser('~/spark_home')),
    'master': 'local',
    'name': 'spark-submit-task',
    'class': None,
    'py_files': None,
    'files': None,
    'deploy_mode': 'client',
    'driver_memory': '1g',
    'executor_memory': '1g',
    'executor_cores': '1',
    'total_executor_cores': '2',
    'jars': None,
    'verbose': False,
    'supervise': False,
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

end_sates = {'FINISHED', 'UNKNOWN', 'KILLED', 'FAILED', 'ERROR'} # states that conclude a job


class SparkJob:
    """SparkJob class encapsulates the basics needed to submit jobs to Spark master based on user input and monitor the outcome

    Parameters
        main_file (str): location of entry .jar or .py file (either local or on cloud)
        **spark_args (Any): keyword arguments used to create a spark-submit command
            Examples:
                For a typical spark-submit CLI argument `--foo-bar baz`, use `foo_bar='baz'`
                For booleans, using `foo_bar=True` will result in `--foo-bar`
                For generic `--conf` arguments, use `conf=["foo.bar.baz='true'", "abc.xyz='ijk'"]`
                For arguments to the main_file, use `main_file_args='arg1 arg2'`

    Returns:
        SparkJob: a SparkJob object ready to be submitted against Spark master
    """

    def __init__(self, main_file: str, **spark_args: Any) -> None:
        self.spark_args = {**__defaults__, **spark_args}

        if main_file.startswith('s3') or main_file.startswith('local:') or os.path.isfile(os.path.expanduser(main_file)):
            self.spark_args['main_file'] = main_file
        else:
            raise FileNotFoundError(f'File {main_file} does not exist.')

        spark_home = self.spark_args['spark_home']
        self.spark_bin = spark_home + '/bin/spark-submit'
        if not os.path.isfile(self.spark_bin):
            raise FileNotFoundError(f'bin/spark-submit was not found in "{spark_home}". Please add SPARK_HOME to path or provide it as an argument: spark_home')

        self.env_vars = _get_env_vars()
        if not self.env_vars['JAVA_HOME']:
            logging.warning('"JAVA_HOME" is not defined in environment variables.')

        self.submit_cmd = self._get_submit_cmd()
        self.submit_response = {'output': '', 'code': '', 'submission_id': '', 'driver_state': ''}


    def _get_submit_cmd(self) -> str:
        booleans = {arg for arg, val in self.spark_args.items() if isinstance(val, bool)}
        exclude = {'spark_home', 'main_file', 'conf', 'main_file_args'} ^ booleans
        args = ['--{0} {1}'.format(arg.replace('_', '-'), val) for arg, val in self.spark_args.items() if arg not in exclude and val is not None]
        confs = [f'--conf {c}' for c in self.spark_args['conf']]

        booleans_str = ''
        for arg in booleans:
            if self.spark_args[arg]:
                booleans_str += '--' + arg.replace('_', '-')

        cmd = self.spark_bin + ' '.join(args) + booleans_str + ' '.join(confs) + ' ' + self.spark_args['main_file'] + ' ' + self.spark_args['main_file_args']
        return cmd.replace('--', ' --').replace('  ', ' ').strip()


    def _await_result(self, seconds: int) -> None:
        while self.submit_response['driver_state'] not in end_sates:
            time.sleep(seconds)
            self._check_submit()


    def _check_submit(self) -> None:
        if not self.submit_response['driver_state'] or self.submit_response['driver_state'] in end_sates:
            return
        else:
            check_url = '{0}/v1/submissions/status/{1}'.format(self.spark_args['master'].replace('spark://', 'http://'), self.submit_response['submission_id'])
            response = requests.get(check_url).text
            driver_state = re.findall('\"driverState\" : \"(.+)\"', response)

            if len(driver_state) < 1:
                logging.warning('driverState not found for in output "{0}" for Spark job "{1}"'.format(response, self.spark_args['name']))
                self.submit_response['driver_state'] = 'UNKNOWN'
            else:
                self.submit_response['driver_state'] = driver_state[0]


    def submit(self, use_env_vars: bool=False, await_result: int=0) -> None:
        """Submits the current Spark job to Spark master

        Parameters
            use_env_vars (bool): whether the environment variables obtained should be used (default: False)
            await_result (int): how often to poll for the Spark driver state in a background thread (default: 0, don't monitor in a background thread)

        Returns:
            None
       """
        env_vars = ''
        if use_env_vars:
            for env_var, val in self.env_vars.items():
                env_vars += f'export {env_var}="{val}"; '

            if platform.system() == 'Windows':
                env_vars = env_vars.replace('export', 'set')

        p = subprocess.Popen(env_vars + self.submit_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        self.submit_response['driver_state'] = 'SUBMITTED'
        o, _ = p.communicate()

        output, code = o.decode(), p.returncode
        self.submit_response['output'] = output
        self.submit_response['code'] = code

        if code != 0:
            self.submit_response['driver_state'] = 'ERROR'
            raise SparkSubmitError(f'{output}\nReturn code: {code}')

        elif self.spark_args['deploy_mode']=='client':
            self.submit_response['driver_state'] = 'FINISHED'

        else:
            submission_id = re.findall('\"submissionId\" : \"(.+)\"', output)
            if len(submission_id) < 1:
                logging.warning('submissionId not found in output "{0}" for Spark job "{1}"'.format(output, self.spark_args['name']))
                self.submit_response['driver_state'] = 'UNKNOWN'
            else:
                self.submit_response['submission_id'] = submission_id[0]
                if await_result > 0:
                    threading.Thread(target=self._await_result, args=(await_result, )).start()


    def get_state(self) -> str:
        """Gets the current state of the Spark job

        Returns:
            str: Spark job driver state
       """
        self._check_submit()
        return self.submit_response['driver_state']


    def get_output(self) -> str:
        """Gets the spark-submit stdout

        Returns:
            str: spark-submit stdout
       """
        return self.submit_response['output']


    def get_code(self) -> int:
        """Gets the spark-submit return code

        Returns:
            int: spark-submit return code (returns -1 if no code)
       """
        return self.submit_response['code'] if self.submit_response['code'] else -1


    def kill(self) -> None:
        """Kills the running Spark job (cluster mode only)

        Returns:
            None
       """
        if self.submit_response['driver_state'] in end_sates:
            logging.warning('Spark job "{0}" has concluded with state {1} and cannot be killed.'.format(self.spark_args['name'], self.submit_response['driver_state']))
        elif self.submit_response['submission_id']:
            kill_url = '{0}/v1/submissions/kill/{1}'.format(self.spark_args['master'].replace('spark://', 'http://'), self.submit_response['submission_id'])
            response = requests.post(kill_url)
            if response.status_code != 200:
                raise SparkJobKillError('Problem with killing Spark job "{0}" with submission ID {1}: {2}'.format(self.spark_args['name'], self.submit_response['submission_id'], response.text))
            else:
                self.submit_response['driver_state'] = 'KILLED'
        else:
            raise SparkJobKillError('Spark job "{0}" has no submission ID to kill.'.format(self.spark_args['name']))
