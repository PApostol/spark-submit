"""System related functionalities"""
import logging
import os
import platform
import re
import subprocess
import sys
from typing import Dict, Optional, Tuple


def _quote_spaces(val: str) -> str:
    return f'"{val}"' if ' ' in val else val


def _get_env_vars() -> Dict[str, str]:
    env_vars = {'JAVA_HOME': os.environ.get('JAVA_HOME', ''),
                'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', sys.executable),
                'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', sys.executable)
                }
    return {env_var: _quote_spaces(val).replace(os.path.sep, '/') for env_var, val in env_vars.items()}


def _execute_cmd(cmd: str, silent: bool = True, timeout: Optional[int] = None) -> Tuple[str, int]:
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True) as process:
        output, _ = process.communicate(timeout=timeout)
        res = output.decode()
        code = process.returncode

    if code != 0 and not silent:
        logging.warning(res)
    return res, code


def system_info() -> str:
    """Collects Spark related system information, such as versions of spark-submit, Scala, Java, PySpark, Python and OS

    Returns:
        str: system information
    """
    spark_home = os.environ.get('SPARK_HOME', os.path.expanduser('~/spark_home')).replace(os.path.sep, '/')
    spark_bin = spark_home + '/bin/spark-submit'
    info_cmd = _quote_spaces(spark_bin) + ' --version'

    java_home = os.environ.get('JAVA_HOME', '').replace(os.path.sep, '/')
    if java_home:
        java_bin = java_home + '/bin/java'
        info_cmd += f' ; {_quote_spaces(java_bin)} -version'

    info_cmd += f' ; {_quote_spaces(sys.executable)} -m pip show pyspark'
    if platform.system() == 'Windows':
        info_cmd = info_cmd.replace(' ; ', ' & ')

    info_stdout, _ = _execute_cmd(info_cmd, silent=False)
    info_re = {'Spark version': 'version (.+)',
               'Scala version': 'scala version (.+?),',
               'Java version': 'java version \"(.+)\"',
               'PySpark version': 'Version: (.+)'
              }

    sys_info: Dict[str, str] = {}
    for key, val in info_re.items():
        i = re.findall(val, info_stdout, re.IGNORECASE)
        if i:
            sys_info[key] = i[0].strip()

    sys_info['Python version'] = sys.version.split(' ', maxsplit=1)[0]
    sys_info['OS'] = platform.platform()
    return '\n'.join([f'{key}: {val}' for key, val in sys_info.items()])
