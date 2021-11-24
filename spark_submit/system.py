import logging
import os
import platform
import re
import subprocess
import sys
from typing import Tuple


def _quote_spaces(val: str) -> str:
    return f'"{val}"' if ' ' in val else val


def _get_env_vars() -> dict:
    env_vars = {'JAVA_HOME': os.environ.get('JAVA_HOME', ''),
                'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', sys.executable),
                'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', sys.executable)
                }
    return {env_var: _quote_spaces(val).replace(os.path.sep, '/') for env_var, val in env_vars.items()}


def _execute_cmd(cmd: str, silent: bool=True) -> Tuple[str, int]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    o, _ = p.communicate()
    o = o.decode()
    code = p.returncode

    if code != 0 and not silent:
        logging.warning(o)
    else:
        return o, code


def system_info() -> str:
    """Collects Spark related system information, such as versions of spark-submit, Scala, Java, PySpark, Python and OS

    Returns:
        str: system information
    """
    spark_bin = os.environ.get('SPARK_HOME', os.path.expanduser('~/spark_home')).replace(os.path.sep, '/') + '/bin/spark-submit'
    info_cmd = _quote_spaces(spark_bin) + ' --version'

    JAVA_HOME = os.environ.get('JAVA_HOME', '').replace(os.path.sep, '/')
    if JAVA_HOME:
        java_bin = JAVA_HOME + '/bin/java'
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

    sys_info = {}
    for k, v in info_re.items():
        i = re.findall(v, info_stdout, re.IGNORECASE)
        if i:
            sys_info[k] = i[0].strip()

    sys_info['Python version'] = sys.version.split(' ')[0]
    sys_info['OS'] = platform.platform()
    return '\n'.join([f'{k}: {v}' for k, v in sys_info.items()])
