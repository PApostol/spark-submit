"""SparkJob class functionality"""
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from ._defaults import __defaults__, __end_states__
from .exceptions import SparkJobKillError, SparkSubmitError
from .system import _execute_cmd


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

        if (
            main_file.startswith('s3')
            or main_file.startswith('local:')
            or os.path.isfile(os.path.expanduser(main_file))
        ):
            self.spark_args['main_file'] = main_file.replace(os.path.sep, '/')
        else:
            raise FileNotFoundError(f'File {main_file} does not exist.')

        spark_home = self.spark_args['spark_home'].replace(os.path.sep, '/')
        self.spark_bin = f'{spark_home}/bin/spark-submit'
        if not os.path.isfile(self.spark_bin):
            raise FileNotFoundError(
                f'bin/spark-submit was not found in "{spark_home}". '
                f'Please add SPARK_HOME to path or provide it as a keyword argument: spark_home'
            )

        self.env_vars = os.environ.copy()
        if 'JAVA_HOME' not in self.env_vars:
            logging.warning('"JAVA_HOME" is not defined in environment variables.')

        self.is_yarn = 'yarn' in self.spark_args['master']
        self.is_k8s = 'k8s' in self.spark_args['master']

        self.submit_cmd = self._get_submit_cmd()
        self.submit_response: Dict[str, Any] = {
            'output': '',
            'code': -1,
            'submission_id': '',
            'driver_state': '',
        }
        self.concluded = False

    def _update_concluded(self) -> None:
        self.concluded = self.submit_response['driver_state'] in __end_states__

    def _get_submit_cmd(self) -> str:
        booleans = {arg for arg, val in self.spark_args.items() if isinstance(val, bool)}
        exclude = {'spark_home', 'main_file', 'conf', 'main_file_args'} ^ booleans

        args = [
            f"--{arg.replace('_', '-')} {val}"
            for arg, val in self.spark_args.items()
            if arg not in exclude and val is not None
        ]
        confs = [f'--conf {c}' for c in self.spark_args['conf']]

        booleans_str = ''
        for arg in booleans:
            if self.spark_args[arg]:
                booleans_str += '--' + arg.replace('_', '-')

        cmd = (
            self.spark_bin
            + ' '.join(args)
            + booleans_str
            + ' '.join(confs)
            + ' '
            + self.spark_args['main_file']
            + ' '
            + str(self.spark_args['main_file_args'])
        )

        return cmd.replace('--', ' --').replace('  ', ' ').strip()

    def _get_api_url(self, endpoint: str) -> str:
        return f"{self.spark_args['master'].replace('spark://', 'http://')}/v1/submissions/{endpoint}/{self.get_id()}"

    def _get_status_response(self) -> str:
        if self.is_yarn:
            status_cmd = f'yarn application -status {self.get_id()}'
            response, _ = _execute_cmd(status_cmd)
        elif self.is_k8s:
            status_cmd = f"{self.spark_bin} --master status --{self.spark_args['master']} {self.get_id()}"
            response, _ = _execute_cmd(status_cmd)
        else:
            status_url = self._get_api_url('status')
            response = requests.get(status_url).text
        return response

    def _check_submit(self) -> None:
        if self.get_id() and not self.concluded:
            response = self._get_status_response()
            driver_state = re.findall(r'"driverState" : "(.+)"', response)

            if len(driver_state) < 1:
                logging.warning(f"driverState not found in output \"{response}\" for job \"{self.spark_args['name']}\"")
                self.submit_response['driver_state'] = 'UNKNOWN'
            else:
                self.submit_response['driver_state'] = driver_state[0]
            self._update_concluded()

    def _poll_state(self, seconds: int) -> None:
        while not self.concluded:
            time.sleep(seconds)
            self._check_submit()

    def _get_submission_id(self, output: str) -> List[str]:
        if self.is_yarn:
            re_exp = r'(application[0-9_]+)'
        elif self.is_k8s:
            re_exp = r'\s*pod name: ((.+?)-([a-z0-9]+)-driver)'
        else:
            re_exp = r'"submissionId" : "(.+)"'
        return re.findall(re_exp, output)

    def _get_kill_response(self) -> Tuple[str, int]:
        if self.is_yarn:
            kill_cmd = f'yarn application -kill {self.get_id()}'
            return _execute_cmd(kill_cmd)

        if self.is_k8s:
            kill_cmd = f"{self.spark_bin} --master {self.spark_args['master']} --kill {self.get_id()}"
            return _execute_cmd(kill_cmd)

        kill_url = self._get_api_url('kill')
        resp = requests.get(kill_url)
        return resp.text, resp.status_code

    def submit(self, *,
               use_env_vars: bool = False,
               extra_env_vars: Optional[Dict[str, str]] = None,
               poll_time: int = 0,
               timeout: Optional[int] = None
               ) -> None:
        """Submits the current Spark job to Spark master.
           If `use_env_vars=True`, the host environment variables in `os.environ` will be used
           with the spark-submit shell.
           Additional environment variables can be supplied with the dictionary `extra_env_vars`
           as key-value pairs (if `use_env_vars=True`, any common keys between `os.environ` and
           `extra_env_vars` will be overwritten by the latter).
           The state of the Spark job can be  polled and updated in the background if a positive
           `poll_time` is given (in seconds) and deploy mode is cluster.
           If a positive `timeout` is given (in seconds), a `subprocess.TimeoutExpired` exception
           will be raised if the Spark job does not terminate within that time.

        Parameters
            use_env_vars (bool): whether the environment variables from `os.environ` should be used with spark-submit
                                 (default: False)
            extra_env_vars (Dict): a dictionary of additional environment variables to use with spark-submit
                                   (default: None)
            poll_time (int): how often to poll the Spark driver state in a background thread
                             (default: 0, don't poll in a background thread)
            timeout (int): exception is raised if spark-submit does not terminate after `timeout` seconds
                           (default: None)

        Returns:
            None
        """
        if extra_env_vars and not isinstance(extra_env_vars, dict):
            raise TypeError(f'extra_env_vars must be dict, not {type(extra_env_vars).__name__}')

        if use_env_vars and extra_env_vars:
            env = {**self.env_vars, **extra_env_vars}
        elif use_env_vars:
            env = self.env_vars
        elif extra_env_vars:
            env = extra_env_vars
        else:
            env = None

        self.submit_response['driver_state'] = 'SUBMITTED'
        self._update_concluded()

        output, code = _execute_cmd(self.submit_cmd, timeout=timeout, env=env, bufsize=-1, universal_newlines=True)
        self.submit_response['output'] = output
        self.submit_response['code'] = code

        if code != 0:
            self.submit_response['driver_state'] = 'ERROR'
            self._update_concluded()
            raise SparkSubmitError(f'{output}\nReturn code: {code}')

        if self.spark_args['deploy_mode'] == 'client':
            self.submit_response['driver_state'] = 'FINISHED'
            self._update_concluded()

        else:
            submission_id = self._get_submission_id(output)
            if len(submission_id) < 1:
                logging.warning(f"submissionId not found in output \"{output}\" for job \"{self.spark_args['name']}\"")
                self.submit_response['driver_state'] = 'UNKNOWN'
                self._update_concluded()
            else:
                self.submit_response['submission_id'] = submission_id[0]
                if poll_time > 0 and not (self.is_yarn or self.is_k8s):
                    threading.Thread(
                        name=self.spark_args['name'],
                        target=self._poll_state,
                        args=(poll_time,),
                    ).start()

    def get_submit_cmd(self, multiline: bool = False) -> str:
        """Gets the associated spark-submit command

        Parameters
            multiline (bool): whether to break the spark-submit command in multiple lines for readability
                              (default: False)

        Returns:
            str: spark-submit command
        """
        if multiline:
            return self.submit_cmd.replace(' --', ' \ \n--').replace(
                ' ' + self.spark_args['main_file'], ' \ \n' + self.spark_args['main_file']
            )
        return self.submit_cmd

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
        return self.submit_response['code']

    def get_id(self) -> str:
        """Gets the spark-submit submission ID

        Returns:
            str: spark-submit submission ID (returns -1 if no code)
        """
        return self.submit_response['submission_id']

    def kill(self) -> None:
        """Kills the running Spark job (cluster mode only)

        Returns:
            None
        """
        if self.concluded:
            logging.warning(
                f"Spark job \"{self.spark_args['name']}\" "
                f"has concluded with state \"{self.submit_response['driver_state']}\""
            )

        elif self.get_id():
            response, code = self._get_kill_response()

            if code not in {0, 200}:
                raise SparkJobKillError(
                    f"Error killing Spark job \"{self.spark_args['name']}\""
                    f'with submission ID "{self.get_id()}": {response}'
                    f'\nReturn code: {code}'
                )

            self.submit_response['driver_state'] = 'KILLED'
            self._update_concluded()
        else:
            raise SparkJobKillError(f"Spark job \"{self.spark_args['name']}\" has no submission ID to kill.")
